defmodule Starcite.Archive do
  @moduledoc """
  Pull-based archiver.

  - periodically scans local `EventStore` session cursors
  - archives only sessions for groups led on this node
  - persists batches through the configured adapter
  - acknowledges archived progress via local Raft command (`Runtime.ack_archived_local/2`)

  Archive persistence failures are treated as fatal for this worker: the process
  crashes and relies on supervisor restart rather than masking write failures.
  """

  use GenServer

  alias Starcite.Runtime
  alias Starcite.Runtime.{EventStore, RaftManager}

  @default_adapter Starcite.Archive.Adapter.Postgres

  @spec adapter() :: module()
  def adapter do
    Application.get_env(:starcite, :archive_adapter, @default_adapter)
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.fetch!(opts, :adapter)
    adapter_opts = Keyword.get(opts, :adapter_opts, [])

    {:ok, _} = adapter_mod.start_link(adapter_opts)

    schedule_flush(interval)

    {:ok,
     %{
       interval: interval,
       adapter: adapter_mod,
       archived_seq_cache: %{}
     }}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    start = System.monotonic_time(:millisecond)
    session_ids = EventStore.session_ids()

    {stats, archived_seq_cache} =
      Enum.reduce(session_ids, {zero_stats(), state.archived_seq_cache}, fn session_id,
                                                                            {stats_acc, cache_acc} ->
        {session_stats, cache_next} = flush_session(session_id, state.adapter, cache_acc)
        {merge_stats(stats_acc, session_stats), cache_next}
      end)

    archived_seq_cache = Map.take(archived_seq_cache, session_ids)

    elapsed_ms = System.monotonic_time(:millisecond) - start

    # Queue age tracking is intentionally disabled for now; keep metric shape stable.
    Starcite.Observability.Telemetry.archive_queue_age(0)

    Starcite.Observability.Telemetry.archive_flush(
      elapsed_ms,
      stats.attempted,
      stats.inserted,
      stats.pending_after,
      stats.pending_sessions,
      stats.bytes_attempted,
      stats.bytes_inserted
    )

    schedule_flush(state.interval)
    {:noreply, %{state | archived_seq_cache: archived_seq_cache}}
  end

  defp flush_session(session_id, adapter, archived_seq_cache)
       when is_binary(session_id) and session_id != "" and is_map(archived_seq_cache) do
    with {:ok, max_seq} <- EventStore.max_seq(session_id),
         {:ok, archived_seq, archived_seq_cache} <-
           load_archived_seq(session_id, archived_seq_cache) do
      pending_before = max(max_seq - archived_seq, 0)

      cond do
        pending_before == 0 ->
          {zero_stats(), archived_seq_cache}

        ensure_local_group_leader(session_id) != :ok ->
          {zero_stats(), archived_seq_cache}

        true ->
          rows =
            EventStore.from_cursor(session_id, archived_seq, archive_batch_size())
            |> Enum.map(&Map.put(&1, :session_id, session_id))

          {session_stats, next_archived_seq} =
            persist_rows(
              rows,
              session_id,
              max_seq,
              pending_before,
              adapter
            )

          {session_stats, put_archived_seq(archived_seq_cache, session_id, next_archived_seq)}
      end
    else
      _ -> {zero_stats(), Map.delete(archived_seq_cache, session_id)}
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
    {%{zero_stats() | pending_after: pending_before, pending_sessions: 1}, nil}
  end

  defp persist_rows(
         rows,
         session_id,
         max_seq,
         pending_before,
         adapter
       ) do
    attempted = length(rows)
    bytes_attempted = Enum.reduce(rows, 0, fn row, acc -> acc + approx_bytes(row) end)

    case adapter.write_events(rows) do
      {:ok, inserted} when is_integer(inserted) and inserted >= 0 ->
        :ok =
          EventStore.cache_archived_events(
            session_id,
            Enum.map(rows, &Map.delete(&1, :session_id))
          )

        upto_seq = contiguous_upto(rows)

        case Runtime.ack_archived_local(session_id, upto_seq) do
          {:ok, %{archived_seq: acked_seq}} ->
            pending_after = max(max_seq - acked_seq, 0)
            avg_event_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0

            Starcite.Observability.Telemetry.archive_batch(
              session_id,
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
              acked_seq
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
              nil
            }
        end

      {:ok, other} ->
        raise "archive adapter returned invalid insert count for #{session_id}: #{inspect(other)}"

      {:error, reason} ->
        raise "archive write failed for #{session_id}: #{inspect(reason)}"
    end
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

  defp ensure_local_group_leader(session_id) when is_binary(session_id) and session_id != "" do
    group_id = RaftManager.group_for_session(session_id)
    server_id = RaftManager.server_id(group_id)

    case :ra.members({server_id, Node.self()}) do
      {:ok, _members, {^server_id, leader_node}} ->
        if leader_node == Node.self(), do: :ok, else: :error

      _ ->
        :error
    end
  end

  defp load_archived_seq(session_id, archived_seq_cache)
       when is_binary(session_id) and is_map(archived_seq_cache) do
    case Map.fetch(archived_seq_cache, session_id) do
      {:ok, archived_seq} when is_integer(archived_seq) and archived_seq >= 0 ->
        {:ok, archived_seq, archived_seq_cache}

      _ ->
        case Runtime.get_session_local(session_id) do
          {:ok, %{archived_seq: archived_seq}}
          when is_integer(archived_seq) and archived_seq >= 0 ->
            {:ok, archived_seq, Map.put(archived_seq_cache, session_id, archived_seq)}

          _ ->
            {:error, :session_lookup_failed}
        end
    end
  end

  defp put_archived_seq(cache, _session_id, nil), do: cache

  defp put_archived_seq(cache, session_id, archived_seq)
       when is_map(cache) and is_binary(session_id) and is_integer(archived_seq) and
              archived_seq >= 0 do
    Map.put(cache, session_id, archived_seq)
  end

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
