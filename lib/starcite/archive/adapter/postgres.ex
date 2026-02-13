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

  @default_limit 100

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
  def upsert_session(row) when is_map(row) do
    with {:ok, id} <- required_non_empty_string(fetch_field(row, :id)),
         {:ok, title} <- optional_string(fetch_field(row, :title)),
         {:ok, metadata} <- optional_object(fetch_field(row, :metadata)),
         {:ok, created_at} <- required_utc_datetime(fetch_field(row, :created_at)),
         {:ok, updated_at} <- required_utc_datetime(fetch_field(row, :updated_at)) do
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
    else
      {:error, _reason} = error ->
        error
    end
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @impl true
  def list_sessions(query_opts) when is_map(query_opts) do
    with {:ok, opts} <- normalize_query_opts(query_opts) do
      query =
        SessionRecord
        |> apply_cursor(opts.cursor)
        |> apply_metadata_filters(opts.metadata)
        |> order_by([s], asc: s.id)
        |> limit(^opts.limit)

      rows = Repo.all(query)

      {:ok,
       %{
         sessions: Enum.map(rows, &to_session_map/1),
         next_cursor: next_cursor(rows, opts.limit)
       }}
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  @impl true
  def list_sessions_by_ids(ids, query_opts) when is_list(ids) and is_map(query_opts) do
    valid_ids =
      ids
      |> Enum.filter(&(is_binary(&1) and &1 != ""))
      |> Enum.uniq()

    if valid_ids == [] do
      {:ok, %{sessions: [], next_cursor: nil}}
    else
      with {:ok, opts} <- normalize_query_opts(query_opts) do
        query =
          SessionRecord
          |> where([s], s.id in ^valid_ids)
          |> apply_cursor(opts.cursor)
          |> apply_metadata_filters(opts.metadata)
          |> order_by([s], asc: s.id)
          |> limit(^opts.limit)

        rows = Repo.all(query)

        {:ok,
         %{
           sessions: Enum.map(rows, &to_session_map/1),
           next_cursor: next_cursor(rows, opts.limit)
         }}
      end
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp insert_all_with_conflict(rows) do
    entries =
      Enum.map(rows, fn row ->
        %{
          session_id: row.session_id,
          seq: row.seq,
          type: row.type,
          actor: row.actor,
          producer_id: row.producer_id,
          producer_seq: row.producer_seq,
          source: row.source,
          payload: Jason.encode!(row.payload),
          metadata: Jason.encode!(row.metadata),
          refs: Jason.encode!(row.refs),
          idempotency_key: row.idempotency_key,
          inserted_at: DateTime.from_naive!(row.inserted_at, "Etc/UTC")
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

  defp next_cursor(rows, limit) when is_list(rows) and is_integer(limit) do
    if length(rows) == limit do
      rows
      |> List.last()
      |> Map.get(:id)
    else
      nil
    end
  end

  defp to_session_map(%SessionRecord{} = session) do
    %{
      id: session.id,
      title: session.title,
      metadata: session.metadata || %{},
      created_at: DateTime.to_iso8601(session.created_at),
      updated_at: DateTime.to_iso8601(session.updated_at)
    }
  end

  defp normalize_query_opts(opts) when is_map(opts) do
    with {:ok, limit} <- optional_limit(Map.get(opts, :limit)),
         {:ok, cursor} <- optional_cursor(Map.get(opts, :cursor)),
         {:ok, metadata} <- optional_metadata(Map.get(opts, :metadata)) do
      {:ok, %{limit: limit, cursor: cursor, metadata: metadata}}
    end
  end

  defp optional_limit(nil), do: {:ok, @default_limit}
  defp optional_limit(limit) when is_integer(limit) and limit > 0, do: {:ok, limit}
  defp optional_limit(_limit), do: {:error, :invalid_limit}

  defp optional_cursor(nil), do: {:ok, nil}
  defp optional_cursor(cursor) when is_binary(cursor) and cursor != "", do: {:ok, cursor}
  defp optional_cursor(_cursor), do: {:error, :invalid_cursor}

  defp optional_metadata(nil), do: {:ok, %{}}

  defp optional_metadata(metadata) when is_map(metadata) do
    Enum.reduce_while(metadata, {:ok, %{}}, fn
      {key, value}, {:ok, acc}
      when is_binary(key) and key != "" and not is_map(value) and not is_list(value) ->
        {:cont, {:ok, Map.put(acc, key, value)}}

      _, _ ->
        {:halt, {:error, :invalid_metadata}}
    end)
  end

  defp optional_metadata(_metadata), do: {:error, :invalid_metadata}

  defp required_non_empty_string(value) when is_binary(value) and value != "", do: {:ok, value}
  defp required_non_empty_string(_value), do: {:error, :invalid_session}

  defp optional_string(nil), do: {:ok, nil}
  defp optional_string(value) when is_binary(value), do: {:ok, value}
  defp optional_string(_value), do: {:error, :invalid_session}

  defp optional_object(nil), do: {:ok, %{}}
  defp optional_object(value) when is_map(value), do: {:ok, value}
  defp optional_object(_value), do: {:error, :invalid_metadata}

  defp required_utc_datetime(%DateTime{} = datetime), do: {:ok, datetime}

  defp required_utc_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, datetime}
      _ -> {:error, :invalid_session}
    end
  end

  defp required_utc_datetime(_value), do: {:error, :invalid_session}

  defp fetch_field(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end
end
