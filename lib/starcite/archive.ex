defmodule Starcite.Archive do
  @moduledoc """
  Pull-based archiver.

  - periodically scans local `EventStore` session cursors
  - archives only sessions for groups led on this node
  - persists batches through `Starcite.Archive.Store`
  - acknowledges archived progress via local Raft command (`WritePath.ack_archived_local/2`)

  Archive persistence failures are treated as fatal for this worker: the process
  crashes and relies on supervisor restart rather than masking write failures.
  """

  use GenServer

  alias Starcite.Archive.Store
  alias Starcite.Observability.Telemetry
  alias Starcite.{WritePath}
  alias Starcite.DataPlane.{EventStore, RaftAccess, RaftManager}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.get(opts, :adapter, Store.adapter())
    adapter_opts = Keyword.get(opts, :adapter_opts, [])
    single_local_write_node = single_local_write_node?()

    {:ok, _} = adapter_mod.start_link(adapter_opts)

    schedule_flush(interval)

    {:ok,
     %{
       interval: interval,
       adapter: adapter_mod,
       single_local_write_node: single_local_write_node,
       archived_seq_cache: %{},
       tenant_cache: %{}
     }}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    start = System.monotonic_time(:millisecond)
    session_ids = EventStore.session_ids()
    ordered_session_ids = fair_session_order(session_ids, state.tenant_cache)

    {stats, archived_seq_cache, tenant_cache} =
      Enum.reduce(
        ordered_session_ids,
        {zero_stats(), state.archived_seq_cache, state.tenant_cache},
        fn session_id, {stats_acc, archived_seq_cache_acc, tenant_cache_acc} ->
          {session_stats, archived_seq_cache_next, tenant_cache_next} =
            flush_session(
              session_id,
              state.adapter,
              archived_seq_cache_acc,
              tenant_cache_acc,
              state.single_local_write_node
            )

          {merge_stats(stats_acc, session_stats), archived_seq_cache_next, tenant_cache_next}
        end
      )

    archived_seq_cache = Map.take(archived_seq_cache, session_ids)
    tenant_cache = Map.take(tenant_cache, session_ids)

    elapsed_ms = System.monotonic_time(:millisecond) - start

    # Queue age tracking is intentionally disabled for now; keep metric shape stable.
    Telemetry.archive_queue_age(0)

    Telemetry.archive_flush(
      elapsed_ms,
      stats.attempted,
      stats.inserted,
      stats.pending_after,
      stats.pending_sessions,
      stats.bytes_attempted,
      stats.bytes_inserted
    )

    schedule_flush(state.interval)
    {:noreply, %{state | archived_seq_cache: archived_seq_cache, tenant_cache: tenant_cache}}
  end

  defp flush_session(
         session_id,
         adapter,
         archived_seq_cache,
         tenant_cache,
         single_local_write_node
       )
       when is_binary(session_id) and session_id != "" and is_map(archived_seq_cache) and
              is_map(tenant_cache) and is_boolean(single_local_write_node) do
    with {:ok, max_seq} <- EventStore.max_seq(session_id),
         {:ok, archived_seq, archived_seq_cache} <-
           load_archived_seq(session_id, archived_seq_cache) do
      pending_before = max(max_seq - archived_seq, 0)

      cond do
        pending_before == 0 ->
          {zero_stats(), archived_seq_cache, tenant_cache}

        ensure_local_group_leader(session_id, single_local_write_node) != :ok ->
          {zero_stats(), archived_seq_cache, tenant_cache}

        true ->
          rows =
            EventStore.from_cursor(session_id, archived_seq, archive_batch_size())
            |> Enum.map(&Map.put(&1, :session_id, session_id))

          {session_stats, next_archived_seq, tenant_id} =
            persist_rows(
              rows,
              session_id,
              max_seq,
              pending_before,
              adapter
            )

          {
            session_stats,
            put_archived_seq(archived_seq_cache, session_id, next_archived_seq),
            put_session_tenant(tenant_cache, session_id, tenant_id)
          }
      end
    else
      _ ->
        {zero_stats(), Map.delete(archived_seq_cache, session_id),
         Map.delete(tenant_cache, session_id)}
    end
  end

  defp persist_rows(
         [],
         _session_id,
         _max_seq,
         pending_before,
         _adapter
       )
       when is_integer(pending_before) and pending_before > 0 do
    {%{zero_stats() | pending_after: pending_before, pending_sessions: 1}, nil, nil}
  end

  defp persist_rows(
         rows,
         session_id,
         max_seq,
         pending_before,
         adapter
       ) do
    tenant_id = archive_batch_tenant_id!(session_id, rows)
    attempted = length(rows)
    bytes_attempted = Enum.reduce(rows, 0, fn row, acc -> acc + approx_bytes(row) end)

    case Store.write_events(adapter, rows) do
      {:ok, inserted} when is_integer(inserted) and inserted >= 0 ->
        :ok =
          EventStore.cache_archived_events(
            session_id,
            Enum.map(rows, &Map.delete(&1, :session_id))
          )

        upto_seq = contiguous_upto(rows)

        case WritePath.ack_archived_local(session_id, upto_seq) do
          {:ok, %{archived_seq: acked_seq}} ->
            pending_after = max(max_seq - acked_seq, 0)
            avg_event_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0

            Telemetry.archive_batch(
              session_id,
              tenant_id,
              attempted,
              bytes_attempted,
              avg_event_bytes,
              pending_after
            )

            bytes_inserted =
              if attempted > 0 do
                div(bytes_attempted * inserted, attempted)
              else
                0
              end

            {
              %{
                attempted: attempted,
                inserted: inserted,
                bytes_attempted: bytes_attempted,
                bytes_inserted: bytes_inserted,
                pending_after: pending_after,
                pending_sessions: if(pending_after > 0, do: 1, else: 0)
              },
              acked_seq,
              tenant_id
            }

          _ack_error ->
            {
              %{
                attempted: attempted,
                inserted: 0,
                bytes_attempted: bytes_attempted,
                bytes_inserted: 0,
                pending_after: pending_before,
                pending_sessions: 1
              },
              nil,
              tenant_id
            }
        end

      {:ok, other} ->
        raise "archive adapter returned invalid insert count for #{session_id}: #{inspect(other)}"

      {:error, reason} ->
        raise "archive write failed for #{session_id}: #{inspect(reason)}"
    end
  end

  defp archive_batch_tenant_id!(session_id, [%{tenant_id: tenant_id} | rest])
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    if Enum.all?(rest, fn %{tenant_id: row_tenant_id} -> row_tenant_id == tenant_id end) do
      tenant_id
    else
      raise "archive batch tenant mismatch for #{session_id}"
    end
  end

  defp archive_batch_tenant_id!(session_id, rows)
       when is_binary(session_id) and session_id != "" and is_list(rows) do
    raise "archive batch missing tenant metadata for #{session_id}"
  end

  defp approx_bytes(%{payload: payload, metadata: metadata, refs: refs}) do
    :erlang.external_size(payload) +
      :erlang.external_size(metadata) +
      :erlang.external_size(refs)
  end

  defp archive_batch_size do
    Application.get_env(:starcite, :archive_batch_size, 5_000)
  end

  defp contiguous_upto(rows) when is_list(rows) do
    rows
    |> Enum.reduce({nil, 0}, fn %{seq: seq}, {prev, upto} ->
      cond do
        prev == nil -> {seq, seq}
        seq == prev + 1 -> {seq, seq}
        true -> {prev, upto}
      end
    end)
    |> elem(1)
  end

  defp ensure_local_group_leader(_session_id, true), do: :ok

  defp ensure_local_group_leader(session_id, false)
       when is_binary(session_id) and session_id != "" do
    group_id = RaftManager.group_for_session(session_id)
    server_id = RaftManager.server_id(group_id)

    case :ra.members({server_id, Node.self()}) do
      {:ok, _members, {^server_id, leader_node}} ->
        if leader_node == Node.self(), do: :ok, else: :error

      _ ->
        :error
    end
  end

  defp single_local_write_node? do
    if RaftManager.replication_factor() == 1 do
      case RaftManager.write_nodes() do
        [write_node] when write_node == node() -> true
        _ -> false
      end
    else
      false
    end
  end

  defp load_archived_seq(session_id, archived_seq_cache)
       when is_binary(session_id) and is_map(archived_seq_cache) do
    case Map.fetch(archived_seq_cache, session_id) do
      {:ok, archived_seq} when is_integer(archived_seq) and archived_seq >= 0 ->
        {:ok, archived_seq, archived_seq_cache}

      _ ->
        with {:ok, archived_seq} <- RaftAccess.fetch_archived_seq(session_id) do
          {:ok, archived_seq, Map.put(archived_seq_cache, session_id, archived_seq)}
        else
          _ -> {:error, :session_lookup_failed}
        end
    end
  end

  defp put_archived_seq(cache, _session_id, nil), do: cache

  defp put_archived_seq(cache, session_id, archived_seq)
       when is_map(cache) and is_binary(session_id) and is_integer(archived_seq) and
              archived_seq >= 0 do
    Map.put(cache, session_id, archived_seq)
  end

  defp put_session_tenant(cache, _session_id, nil), do: cache

  defp put_session_tenant(cache, session_id, tenant_id)
       when is_map(cache) and is_binary(session_id) and is_binary(tenant_id) and tenant_id != "" do
    Map.put(cache, session_id, tenant_id)
  end

  defp put_session_tenant(cache, _session_id, _tenant_id), do: cache

  defp fair_session_order(session_ids, tenant_cache)
       when is_list(session_ids) and is_map(tenant_cache) do
    session_ids
    |> Enum.group_by(&session_tenant_for_order(&1, tenant_cache))
    |> Enum.sort_by(fn {tenant_id, _ids} -> tenant_sort_key(tenant_id) end)
    |> Enum.map(fn {_tenant_id, ids} -> ids end)
    |> interleave_tenant_groups()
  end

  defp session_tenant_for_order(session_id, tenant_cache)
       when is_binary(session_id) and session_id != "" and is_map(tenant_cache) do
    case Map.fetch(tenant_cache, session_id) do
      {:ok, tenant_id} when is_binary(tenant_id) and tenant_id != "" ->
        tenant_id

      _ ->
        case EventStore.from_cursor(session_id, 0, 1) do
          [%{tenant_id: tenant_id} | _rest] when is_binary(tenant_id) and tenant_id != "" ->
            tenant_id

          _ ->
            :unknown
        end
    end
  end

  defp interleave_tenant_groups([]), do: []

  defp interleave_tenant_groups(groups) when is_list(groups) do
    max_len =
      Enum.reduce(groups, 0, fn ids, acc ->
        max(acc, length(ids))
      end)

    0..(max_len - 1)
    |> Enum.flat_map(fn idx ->
      groups
      |> Enum.map(&Enum.at(&1, idx))
      |> Enum.reject(&is_nil/1)
    end)
  end

  defp tenant_sort_key(tenant_id) when is_binary(tenant_id) and tenant_id != "",
    do: {0, tenant_id}

  defp tenant_sort_key(:unknown), do: {1, ""}
  defp tenant_sort_key(_other), do: {2, ""}

  defp zero_stats do
    %{
      attempted: 0,
      inserted: 0,
      bytes_attempted: 0,
      bytes_inserted: 0,
      pending_after: 0,
      pending_sessions: 0
    }
  end

  defp merge_stats(left, right) do
    %{
      attempted: left.attempted + right.attempted,
      inserted: left.inserted + right.inserted,
      bytes_attempted: left.bytes_attempted + right.bytes_attempted,
      bytes_inserted: left.bytes_inserted + right.bytes_inserted,
      pending_after: left.pending_after + right.pending_after,
      pending_sessions: left.pending_sessions + right.pending_sessions
    }
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_tick, interval)
  end
end
