defmodule Starcite.Archive do
  @moduledoc """
  Pull-based archiver.

  - periodically scans local `EventStore` session cursors
  - archives only sessions for groups led on this node
  - persists batches through `Starcite.Archive.Store`
  - acknowledges archived progress through the write path (`WritePath.ack_archived/1`)

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

    {stats, pending_acks, archived_seq_cache, tenant_cache} =
      Enum.reduce(
        ordered_session_ids,
        {zero_stats(), [], state.archived_seq_cache, state.tenant_cache},
        fn session_id, {stats_acc, pending_acks_acc, archived_seq_cache_acc, tenant_cache_acc} ->
          {session_stats, pending_ack, archived_seq_cache_next, tenant_cache_next} =
            flush_session(
              session_id,
              state.adapter,
              archived_seq_cache_acc,
              tenant_cache_acc,
              state.single_local_write_node
            )

          next_pending_acks =
            case pending_ack do
              nil -> pending_acks_acc
              ack -> [ack | pending_acks_acc]
            end

          {
            merge_stats(stats_acc, session_stats),
            next_pending_acks,
            archived_seq_cache_next,
            tenant_cache_next
          }
        end
      )

    {ack_stats, archived_seq_cache} =
      apply_archive_acks(Enum.reverse(pending_acks), archived_seq_cache)

    stats = merge_stats(stats, ack_stats)

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
          {zero_stats(), nil, archived_seq_cache, tenant_cache}

        ensure_local_group_leader(session_id, single_local_write_node) != :ok ->
          {zero_stats(), nil, archived_seq_cache, tenant_cache}

        true ->
          rows =
            EventStore.from_cursor(session_id, archived_seq, archive_batch_size())
            |> Enum.map(&Map.put(&1, :session_id, session_id))

          {session_stats, pending_ack, tenant_id} =
            persist_rows(
              rows,
              session_id,
              max_seq,
              pending_before,
              adapter
            )

          {
            session_stats,
            pending_ack,
            archived_seq_cache,
            put_session_tenant(tenant_cache, session_id, tenant_id)
          }
      end
    else
      _ ->
        {zero_stats(), nil, Map.delete(archived_seq_cache, session_id),
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
        :ok = persist_session_archived_seq(adapter, session_id, tenant_id, upto_seq)

        {
          zero_stats(),
          %{
            session_id: session_id,
            tenant_id: tenant_id,
            attempted: attempted,
            inserted: inserted,
            bytes_attempted: bytes_attempted,
            pending_before: pending_before,
            max_seq: max_seq,
            upto_seq: upto_seq
          },
          tenant_id
        }

      {:ok, other} ->
        raise "archive adapter returned invalid insert count for #{session_id}: #{inspect(other)}"

      {:error, reason} ->
        raise "archive write failed for #{session_id}: #{inspect(reason)}"
    end
  end

  defp persist_session_archived_seq(adapter, session_id, tenant_id, archived_seq)
       when is_atom(adapter) and is_binary(session_id) and session_id != "" and
              is_binary(tenant_id) and tenant_id != "" and is_integer(archived_seq) and
              archived_seq >= 0 do
    case Store.update_session_archived_seq(adapter, session_id, tenant_id, archived_seq) do
      :ok ->
        :ok

      {:error, reason} ->
        raise "archive session upsert failed for #{session_id}: #{inspect(reason)}"
    end
  end

  defp apply_archive_acks([], archived_seq_cache) when is_map(archived_seq_cache),
    do: {zero_stats(), archived_seq_cache}

  defp apply_archive_acks(pending_acks, archived_seq_cache)
       when is_list(pending_acks) and is_map(archived_seq_cache) do
    ack_entries = Enum.map(pending_acks, &{&1.session_id, &1.upto_seq})

    case WritePath.ack_archived(ack_entries) do
      {:ok, %{applied: applied, failed: failed}} when is_list(applied) and is_list(failed) ->
        applied_by_session = Map.new(applied, &{&1.session_id, &1})
        failed_by_session = Map.new(failed, &{&1.session_id, &1.reason})

        Enum.reduce(pending_acks, {zero_stats(), archived_seq_cache}, fn pending_ack,
                                                                         {stats_acc, cache_acc} ->
          apply_archive_ack_result(
            pending_ack,
            Map.get(applied_by_session, pending_ack.session_id),
            Map.get(failed_by_session, pending_ack.session_id),
            stats_acc,
            cache_acc
          )
        end)

      other ->
        apply_archive_ack_failure(pending_acks, archived_seq_cache, other)
    end
  end

  defp apply_archive_ack_result(
         pending_ack,
         %{archived_seq: archived_seq},
         _failure_reason,
         stats_acc,
         archived_seq_cache
       )
       when is_integer(archived_seq) and archived_seq >= 0 do
    pending_after = max(pending_ack.max_seq - archived_seq, 0)
    avg_event_bytes = average_bytes(pending_ack.bytes_attempted, pending_ack.attempted)

    Telemetry.archive_batch(
      pending_ack.session_id,
      pending_ack.tenant_id,
      pending_ack.attempted,
      pending_ack.bytes_attempted,
      avg_event_bytes,
      pending_after
    )

    stats =
      %{
        attempted: pending_ack.attempted,
        inserted: pending_ack.inserted,
        bytes_attempted: pending_ack.bytes_attempted,
        bytes_inserted:
          inserted_bytes(pending_ack.bytes_attempted, pending_ack.inserted, pending_ack.attempted),
        pending_after: pending_after,
        pending_sessions: if(pending_after > 0, do: 1, else: 0)
      }

    {
      merge_stats(stats_acc, stats),
      put_archived_seq(archived_seq_cache, pending_ack.session_id, archived_seq)
    }
  end

  defp apply_archive_ack_result(
         pending_ack,
         _applied_result,
         _failure_reason,
         stats_acc,
         archived_seq_cache
       ) do
    {
      merge_stats(stats_acc, archive_ack_failure_stats(pending_ack)),
      archived_seq_cache
    }
  end

  defp apply_archive_ack_failure(pending_acks, archived_seq_cache, _reason)
       when is_list(pending_acks) and is_map(archived_seq_cache) do
    stats =
      Enum.reduce(pending_acks, zero_stats(), fn pending_ack, stats_acc ->
        merge_stats(stats_acc, archive_ack_failure_stats(pending_ack))
      end)

    {stats, archived_seq_cache}
  end

  defp archive_ack_failure_stats(pending_ack) when is_map(pending_ack) do
    %{
      attempted: pending_ack.attempted,
      inserted: 0,
      bytes_attempted: pending_ack.bytes_attempted,
      bytes_inserted: 0,
      pending_after: pending_ack.pending_before,
      pending_sessions: 1
    }
  end

  defp average_bytes(_bytes_attempted, attempted) when attempted <= 0, do: 0
  defp average_bytes(bytes_attempted, attempted), do: div(bytes_attempted, attempted)

  defp inserted_bytes(_bytes_attempted, _inserted, attempted) when attempted <= 0, do: 0

  defp inserted_bytes(bytes_attempted, inserted, attempted),
    do: div(bytes_attempted * inserted, attempted)

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
