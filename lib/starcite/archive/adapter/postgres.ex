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
          tenant_id: e.tenant_id,
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
  def upsert_session(
        %{
          id: id,
          title: title,
          tenant_id: tenant_id,
          creator_principal: creator_principal,
          metadata: metadata,
          created_at: %DateTime{} = created_at
        } = session
      )
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and
             (is_struct(creator_principal, Principal) or is_map(creator_principal)) and
             is_map(metadata) do
    runtime = normalize_runtime_snapshot(session)

    row =
      %{
        id: id,
        title: title,
        tenant_id: tenant_id,
        creator_principal: creator_principal,
        metadata: metadata,
        created_at: created_at
      }
      |> Map.merge(runtime)

    {inserted, _} =
      Repo.insert_all(
        "sessions",
        [row],
        on_conflict: :nothing,
        conflict_target: [:id]
      )

    if inserted == 0 do
      :ok = maybe_update_existing_row(row)
    end

    :ok
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  defp maybe_update_existing_row(%{
         id: id,
         title: title,
         tenant_id: tenant_id,
         creator_principal: creator_principal,
         metadata: metadata,
         created_at: created_at,
         last_seq: last_seq,
         archived_seq: archived_seq,
         retention: retention,
         producer_cursors: producer_cursors,
         last_progress_poll: last_progress_poll,
         snapshot_version: snapshot_version
       })
       when is_binary(id) and id != "" and is_integer(last_seq) and last_seq >= 0 and
              is_integer(archived_seq) and archived_seq >= 0 and
              is_integer(last_progress_poll) and last_progress_poll >= 0 do
    query =
      from(s in SessionRecord,
        where: s.id == ^id,
        where:
          coalesce(s.last_seq, 0) < ^last_seq or
            (coalesce(s.last_seq, 0) == ^last_seq and
               coalesce(s.last_progress_poll, 0) < ^last_progress_poll) or
            (coalesce(s.last_seq, 0) == ^last_seq and
               coalesce(s.last_progress_poll, 0) == ^last_progress_poll and
               coalesce(s.archived_seq, 0) < ^archived_seq)
      )

    _updated_count =
      Repo.update_all(query,
        set: [
          title: title,
          tenant_id: tenant_id,
          creator_principal: creator_principal,
          metadata: metadata,
          created_at: created_at,
          last_seq: last_seq,
          archived_seq: archived_seq,
          retention: retention,
          producer_cursors: producer_cursors,
          last_progress_poll: last_progress_poll,
          snapshot_version: snapshot_version
        ]
      )

    :ok
  end

  defp normalize_runtime_snapshot(session) when is_map(session) do
    last_seq = non_neg_integer_snapshot(session, :last_seq, 0)
    archived_seq = non_neg_integer_snapshot(session, :archived_seq, 0)
    last_progress_poll = non_neg_integer_snapshot(session, :last_progress_poll, 0)
    retention = map_snapshot(session, :retention, %{})
    producer_cursors = map_snapshot(session, :producer_cursors, %{}) |> encode_producer_cursors()
    snapshot_version = snapshot_version(session)

    if archived_seq <= last_seq do
      %{
        last_seq: last_seq,
        archived_seq: archived_seq,
        retention: retention,
        producer_cursors: producer_cursors,
        last_progress_poll: last_progress_poll,
        snapshot_version: snapshot_version
      }
    else
      raise ArgumentError,
            "invalid session runtime snapshot: archived_seq=#{archived_seq} > last_seq=#{last_seq}"
    end
  end

  defp non_neg_integer_snapshot(session, key, default)
       when is_map(session) and is_atom(key) and is_integer(default) and default >= 0 do
    case Map.get(session, key, default) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "invalid session #{key}: #{inspect(value)}"
    end
  end

  defp map_snapshot(session, key, default)
       when is_map(session) and is_atom(key) and is_map(default) do
    case Map.get(session, key, default) do
      value when is_map(value) -> value
      value -> raise ArgumentError, "invalid session #{key}: #{inspect(value)}"
    end
  end

  defp snapshot_version(session) when is_map(session) do
    case Map.get(session, :snapshot_version) do
      nil -> nil
      value when is_binary(value) and value != "" -> value
      value -> raise ArgumentError, "invalid session snapshot_version: #{inspect(value)}"
    end
  end

  defp encode_producer_cursors(cursors) when map_size(cursors) == 0, do: %{}

  defp encode_producer_cursors(cursors) when is_map(cursors) do
    Enum.reduce(cursors, %{}, fn
      {producer_id, cursor}, acc
      when is_binary(producer_id) and producer_id != "" and is_map(cursor) ->
        producer_seq = cursor_integer!(cursor, :producer_seq)
        session_seq = cursor_integer!(cursor, :session_seq)
        hash = cursor_hash!(cursor)

        Map.put(acc, producer_id, %{
          producer_seq: producer_seq,
          session_seq: session_seq,
          hash: Base.url_encode64(hash, padding: false)
        })

      entry, _acc ->
        raise ArgumentError, "invalid producer cursor entry: #{inspect(entry)}"
    end)
  end

  defp cursor_integer!(cursor, key) when is_map(cursor) and is_atom(key) do
    case Map.get(cursor, key) || Map.get(cursor, Atom.to_string(key)) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "invalid producer cursor #{key}: #{inspect(value)}"
    end
  end

  defp cursor_hash!(cursor) when is_map(cursor) do
    case Map.get(cursor, :hash) || Map.get(cursor, "hash") do
      value when is_binary(value) and value != "" -> value
      value -> raise ArgumentError, "invalid producer cursor hash: #{inspect(value)}"
    end
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
          tenant_id: row.tenant_id,
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
      created_at: DateTime.to_iso8601(session.created_at),
      last_seq: session.last_seq || 0,
      archived_seq: session.archived_seq || 0,
      retention: session.retention || %{},
      producer_cursors: session.producer_cursors || %{},
      last_progress_poll: session.last_progress_poll || 0,
      snapshot_version: session.snapshot_version
    }
  end
end
