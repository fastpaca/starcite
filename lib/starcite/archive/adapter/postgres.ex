defmodule Starcite.Archive.Adapter.Postgres do
  @moduledoc """
  Postgres archive adapter using Ecto.

  Inserts events in batches with ON CONFLICT DO NOTHING for idempotency.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  import Ecto.Query

  alias Starcite.Archive.{Event, SessionRecord}
  alias Starcite.Repo

  @impl true
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state), do: {:ok, state}

  # Postgres parameter limit is 65535. With 12 fields per event,
  # we can safely insert ~6000 rows per batch. Use 5000 to be safe.
  @chunk_size 5_000

  @impl true
  def write_events(rows) when is_list(rows) do
    if rows == [] do
      {:ok, 0}
    else
      total_count =
        rows
        |> Enum.chunk_every(@chunk_size)
        |> Enum.reduce(0, fn chunk, acc ->
          {count, _} = insert_all_with_conflict(chunk)
          acc + count
        end)

      {:ok, total_count}
    end
  end

  @impl true
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    query =
      from(e in Event,
        where: e.session_id == ^session_id and e.seq >= ^from_seq and e.seq <= ^to_seq,
        order_by: [asc: e.seq],
        select: %{
          seq: e.seq,
          type: e.type,
          payload: e.payload,
          actor: e.actor,
          producer_id: e.producer_id,
          producer_seq: e.producer_seq,
          source: e.source,
          metadata: e.metadata,
          refs: e.refs,
          idempotency_key: e.idempotency_key,
          inserted_at: e.inserted_at
        }
      )

    {:ok, Repo.all(query)}
  end

  @impl true
  def upsert_session(%{
        id: id,
        title: title,
        metadata: metadata,
        created_at: %DateTime{} = created_at,
        updated_at: %DateTime{} = updated_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_map(metadata) do
    entry = %{
      id: id,
      title: title,
      metadata: metadata,
      created_at: created_at,
      updated_at: updated_at
    }

    {_inserted, _} =
      Repo.insert_all(
        "sessions",
        [entry],
        on_conflict: :nothing,
        conflict_target: [:id]
      )

    :ok
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  def upsert_session(_row), do: {:error, :invalid_session}

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata})
      when is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    :ok = validate_metadata_filters!(metadata)

    query =
      SessionRecord
      |> apply_cursor(cursor)
      |> apply_metadata_filters(metadata)
      |> order_by([s], asc: s.id)
      |> limit(^limit)

    rows = Repo.all(query)

    {:ok,
     %{
       sessions: Enum.map(rows, &to_session_map/1),
       next_cursor: next_cursor(rows, limit)
     }}
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  def list_sessions(_query_opts), do: {:error, :invalid_list_query}

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata})
      when is_list(ids) and is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    :ok = validate_metadata_filters!(metadata)
    session_ids = normalize_ids!(ids)

    if session_ids == [] do
      {:ok, %{sessions: [], next_cursor: nil}}
    else
      query =
        SessionRecord
        |> where([s], s.id in ^session_ids)
        |> apply_cursor(cursor)
        |> apply_metadata_filters(metadata)
        |> order_by([s], asc: s.id)
        |> limit(^limit)

      rows = Repo.all(query)

      {:ok,
       %{
         sessions: Enum.map(rows, &to_session_map/1),
         next_cursor: next_cursor(rows, limit)
       }}
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  def list_sessions_by_ids(_ids, _query_opts), do: {:error, :invalid_list_query}

  defp insert_all_with_conflict(rows) do
    entries =
      Enum.map(rows, fn %{
                          session_id: session_id,
                          seq: seq,
                          type: type,
                          actor: actor,
                          producer_id: producer_id,
                          producer_seq: producer_seq,
                          source: source,
                          payload: payload,
                          metadata: metadata,
                          refs: refs,
                          idempotency_key: idempotency_key,
                          inserted_at: inserted_at
                        } ->
        %{
          session_id: session_id,
          seq: seq,
          type: type,
          actor: actor,
          producer_id: producer_id,
          producer_seq: producer_seq,
          source: source,
          payload: Jason.encode!(payload),
          metadata: Jason.encode!(metadata),
          refs: Jason.encode!(refs),
          idempotency_key: idempotency_key,
          inserted_at: DateTime.from_naive!(inserted_at, "Etc/UTC")
        }
      end)

    Repo.insert_all(
      "events",
      entries,
      placeholders: %{payload: :string, metadata: :string, refs: :string},
      on_conflict: :nothing,
      conflict_target: [:session_id, :seq]
    )
  end

  defp apply_cursor(query, nil), do: query

  defp apply_cursor(query, cursor) when is_binary(cursor) and cursor != "" do
    where(query, [s], s.id > ^cursor)
  end

  defp apply_metadata_filters(query, metadata_filters) when map_size(metadata_filters) == 0,
    do: query

  defp apply_metadata_filters(query, metadata_filters) do
    Enum.reduce(metadata_filters, query, fn {key, value}, acc ->
      where(acc, [s], fragment("? @> ?", s.metadata, type(^%{key => value}, :map)))
    end)
  end

  defp next_cursor(rows, limit) when is_list(rows) and length(rows) == limit do
    %SessionRecord{id: id} = List.last(rows)
    id
  end

  defp next_cursor(_rows, _limit), do: nil

  defp to_session_map(%SessionRecord{} = session) do
    %{
      id: session.id,
      title: session.title,
      metadata: session.metadata || %{},
      created_at: DateTime.to_iso8601(session.created_at),
      updated_at: DateTime.to_iso8601(session.updated_at)
    }
  end

  defp validate_metadata_filters!(metadata_filters) when is_map(metadata_filters) do
    Enum.each(metadata_filters, fn
      {key, value}
      when is_binary(key) and key != "" and not is_map(value) and not is_list(value) ->
        :ok

      _ ->
        raise ArgumentError, "invalid metadata filter"
    end)

    :ok
  end

  defp normalize_ids!(ids) when is_list(ids) do
    ids
    |> Enum.reduce([], fn
      id, acc when is_binary(id) and id != "" -> [id | acc]
      _, _ -> raise ArgumentError, "invalid session id"
    end)
    |> Enum.reverse()
    |> Enum.uniq()
  end
end
