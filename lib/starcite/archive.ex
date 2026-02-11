defmodule Starcite.Archive do
  @moduledoc """
  Pull-based archiver.

  - periodically scans local `EventStore` session cursors
  - archives only sessions for groups led on this node
  - persists batches through the configured adapter
  - acknowledges archived progress via local Raft command (`Runtime.ack_archived_local/2`)
  """

  use GenServer

  alias Starcite.Archive.Store
  alias Starcite.Runtime
  alias Starcite.Runtime.{EventStore, RaftManager}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

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
       adapter: adapter_mod
     }}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    start = System.monotonic_time(:millisecond)

    stats =
      EventStore.session_ids()
      |> Enum.reduce(zero_stats(), fn session_id, stats_acc ->
        session_stats = flush_session(session_id, state.adapter)
        merge_stats(stats_acc, session_stats)
      end)

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
    {:noreply, state}
  end

  defp flush_session(session_id, adapter) when is_binary(session_id) and session_id != "" do
    with :ok <- ensure_local_group_leader(session_id),
         {:ok, max_seq} <- EventStore.max_seq(session_id),
         {:ok, session} <- Runtime.get_session_local(session_id) do
      archived_seq = session.archived_seq
      pending_before = max(max_seq - archived_seq, 0)

      cond do
        pending_before == 0 ->
          zero_stats()

        true ->
          rows =
            EventStore.from_cursor(session_id, archived_seq, archive_batch_size())
            |> Enum.map(&Map.put(&1, :session_id, session_id))

          persist_rows(
            rows,
            session_id,
            max_seq,
            pending_before,
            adapter
          )
      end
    else
      _ -> zero_stats()
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
    %{zero_stats() | pending_after: pending_before, pending_sessions: 1}
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

    case Store.write_events(adapter, rows) do
      {:ok, inserted} ->
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

            %{
              attempted: attempted,
              inserted: inserted,
              bytes_attempted: bytes_attempted,
              bytes_inserted: bytes_inserted,
              pending_after: pending_after,
              pending_sessions: if(pending_after > 0, do: 1, else: 0)
            }

          _ack_error ->
            %{
              attempted: attempted,
              inserted: 0,
              bytes_attempted: bytes_attempted,
              bytes_inserted: 0,
              pending_after: pending_before,
              pending_sessions: 1
            }
        end

      {:error, _reason} ->
        %{
          attempted: attempted,
          inserted: 0,
          bytes_attempted: bytes_attempted,
          bytes_inserted: 0,
          pending_after: pending_before,
          pending_sessions: 1
        }
    end
  end

  defp approx_bytes(%{payload: payload, metadata: metadata, refs: refs}) do
    byte_size(Jason.encode!(payload)) +
      byte_size(Jason.encode!(metadata)) +
      byte_size(Jason.encode!(refs))
  end

  defp archive_batch_size do
    Application.get_env(:starcite, :archive_batch_size, 5_000)
  end

  defp contiguous_upto(rows) when is_list(rows) do
    rows
    |> Enum.sort_by(& &1.seq)
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
