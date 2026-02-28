defmodule Starcite.Archive.Adapter.Postgres do
  @moduledoc """
  Postgres archive adapter using Ecto.

  Inserts events in batches with ON CONFLICT DO NOTHING for idempotency.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  import Ecto.Query

  alias Starcite.Archive.{Event, SessionRecord}
  alias Starcite.Auth.Principal
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
        tenant_id: tenant_id,
        creator_principal: creator_principal,
        metadata: metadata,
        created_at: %DateTime{} = created_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and
             is_struct(creator_principal, Principal) and
             is_map(metadata) do
    {_inserted, _} =
      Repo.insert_all(
        "sessions",
        [
          %{
            id: id,
            title: title,
            tenant_id: tenant_id,
            creator_principal: creator_principal,
            metadata: metadata,
            created_at: created_at
          }
        ],
        on_conflict: :nothing,
        conflict_target: [:id]
      )

    :ok
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata} = query_opts)
      when is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    query =
      SessionRecord
      |> apply_cursor(cursor)
      |> apply_tenant_filter(Map.get(query_opts, :tenant_id))
      |> apply_owner_principal_filters(Map.get(query_opts, :owner_principal_ids))
      |> apply_metadata_filters(metadata)
      |> order_by([s], asc: s.id)
      |> limit(^limit)

    rows = Repo.all(query)

    build_session_page(rows, limit)
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata} = query_opts)
      when is_list(ids) and is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    session_ids = ids |> Enum.uniq() |> Enum.reject(&(&1 == ""))

    if session_ids == [] do
      {:ok, %{sessions: [], next_cursor: nil}}
    else
      query =
        SessionRecord
        |> where([s], s.id in ^session_ids)
        |> apply_cursor(cursor)
        |> apply_tenant_filter(Map.get(query_opts, :tenant_id))
        |> apply_owner_principal_filters(Map.get(query_opts, :owner_principal_ids))
        |> apply_metadata_filters(metadata)
        |> order_by([s], asc: s.id)
        |> limit(^limit)

      rows = Repo.all(query)

      build_session_page(rows, limit)
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
          payload: row.payload,
          metadata: row.metadata,
          refs: row.refs,
          idempotency_key: row.idempotency_key,
          inserted_at: normalize_inserted_at(row.inserted_at)
        }
      end)

    Repo.insert_all(
      Event,
      entries,
      on_conflict: :nothing,
      conflict_target: [:session_id, :seq]
    )
  end

  defp normalize_inserted_at(%NaiveDateTime{} = naive_datetime) do
    naive_datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.truncate(:second)
  end

  defp normalize_inserted_at(%DateTime{} = datetime), do: DateTime.truncate(datetime, :second)

  defp apply_cursor(query, nil), do: query

  defp apply_cursor(query, cursor) when is_binary(cursor) and cursor != "" do
    where(query, [s], s.id > ^cursor)
  end

  defp apply_tenant_filter(query, nil), do: query

  defp apply_tenant_filter(query, tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    where(query, [s], s.tenant_id == ^tenant_id)
  end

  defp apply_tenant_filter(query, _invalid_filter), do: where(query, [s], false)

  defp apply_owner_principal_filters(query, nil), do: query

  defp apply_owner_principal_filters(query, owner_principal_ids)
       when is_list(owner_principal_ids) do
    owner_principal_ids =
      owner_principal_ids
      |> Enum.uniq()
      |> Enum.reject(&(&1 == ""))

    case owner_principal_ids do
      [] ->
        where(query, [s], false)

      _other ->
        where(
          query,
          [s],
          fragment(
            "?->>'id' = ANY(?)",
            s.creator_principal,
            type(^owner_principal_ids, {:array, :string})
          )
        )
    end
  end

  defp apply_owner_principal_filters(query, _invalid_filter), do: where(query, [s], false)

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

  defp build_session_page(rows, limit) when is_list(rows) and is_integer(limit) and limit > 0 do
    sessions = normalize_session_rows(rows)

    {:ok,
     %{
       sessions: sessions,
       next_cursor: next_cursor(rows, limit)
     }}
  end

  defp normalize_session_rows(rows) when is_list(rows) do
    rows
    |> Enum.map(&to_session_row/1)
  end

  defp to_session_row(%SessionRecord{} = session) do
    %{
      id: session.id,
      title: session.title,
      tenant_id: session.tenant_id,
      creator_principal: session.creator_principal,
      metadata: session.metadata || %{},
      created_at: DateTime.to_iso8601(session.created_at)
    }
  end
end
