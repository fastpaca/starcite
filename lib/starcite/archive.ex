defmodule Starcite.Archive do
  @moduledoc """
  Cursor-driven archiver.

  - subscribes to global cursor updates
  - reads committed events from local ETS event store
  - persists batches through the configured adapter
  - acknowledges archived progress via Raft (`Runtime.ack_archived/2`)
  """

  use GenServer

  alias Starcite.Runtime
  alias Starcite.Runtime.{CursorUpdate, EventStore}

  @type pending_entry :: %{
          required(:last_seq) => non_neg_integer(),
          required(:oldest_inserted_at) => NaiveDateTime.t() | DateTime.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.fetch!(opts, :adapter)
    adapter_opts = Keyword.get(opts, :adapter_opts, [])

    {:ok, _} = adapter_mod.start_link(adapter_opts)
    :ok = Phoenix.PubSub.subscribe(Starcite.PubSub, CursorUpdate.global_topic())

    schedule_flush(interval)

    {:ok,
     %{
       interval: interval,
       adapter: adapter_mod,
       pending: %{}
     }}
  end

  @impl true
  def handle_info({:cursor_update, %{session_id: session_id, seq: seq} = update}, state)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    inserted_at = Map.get(update, :inserted_at, NaiveDateTime.utc_now())

    pending =
      Map.update(state.pending, session_id, %{last_seq: seq, oldest_inserted_at: inserted_at}, fn
        %{last_seq: pending_seq, oldest_inserted_at: oldest} = pending_entry ->
          %{
            pending_entry
            | last_seq: max(pending_seq, seq),
              oldest_inserted_at: older_timestamp(oldest, inserted_at)
          }
      end)

    {:noreply, %{state | pending: pending}}
  end

  def handle_info({:cursor_update, _update}, state), do: {:noreply, state}

  @impl true
  def handle_info(:flush_tick, state) do
    start = System.monotonic_time(:millisecond)

    {pending, stats} =
      Enum.reduce(state.pending, {%{}, zero_stats()}, fn {session_id, pending_entry},
                                                         {pending_acc, stats_acc} ->
        {next_entry, session_stats} = flush_session(session_id, pending_entry, state.adapter)

        pending_acc =
          case next_entry do
            nil -> pending_acc
            entry -> Map.put(pending_acc, session_id, entry)
          end

        {pending_acc, merge_stats(stats_acc, session_stats)}
      end)

    elapsed_ms = System.monotonic_time(:millisecond) - start
    pending_sessions = map_size(pending)
    pending_events = stats.pending_after

    Starcite.Observability.Telemetry.archive_queue_age(oldest_age_seconds(pending))

    Starcite.Observability.Telemetry.archive_flush(
      elapsed_ms,
      stats.attempted,
      stats.inserted,
      pending_events,
      pending_sessions,
      stats.bytes_attempted,
      stats.bytes_inserted
    )

    schedule_flush(state.interval)
    {:noreply, %{state | pending: pending}}
  end

  defp flush_session(session_id, %{last_seq: target_seq} = pending_entry, adapter)
       when is_binary(session_id) and session_id != "" and is_integer(target_seq) and
              target_seq >= 0 do
    with {:ok, session} <- Runtime.get_session(session_id) do
      target_seq = max(target_seq, session.last_seq)
      archived_seq = session.archived_seq

      cond do
        archived_seq >= target_seq ->
          {nil, zero_stats()}

        true ->
          rows =
            EventStore.from_cursor(session_id, archived_seq, archive_batch_size())
            |> Enum.map(&Map.put(&1, :session_id, session_id))

          persist_rows(rows, session_id, target_seq, archived_seq, pending_entry, adapter)
      end
    else
      {:error, :session_not_found} ->
        {nil, zero_stats()}

      _other ->
        {pending_entry, zero_stats()}
    end
  end

  defp persist_rows([], _session_id, target_seq, archived_seq, pending_entry, _adapter) do
    pending_after = max(target_seq - archived_seq, 0)
    {%{pending_entry | last_seq: target_seq}, %{zero_stats() | pending_after: pending_after}}
  end

  defp persist_rows(rows, session_id, target_seq, archived_seq, pending_entry, adapter) do
    attempted = length(rows)
    bytes_attempted = Enum.reduce(rows, 0, fn row, acc -> acc + approx_bytes(row) end)

    case adapter.write_events(rows) do
      {:ok, inserted} ->
        upto_seq = contiguous_upto(rows)

        case Runtime.ack_archived(session_id, upto_seq) do
          {:ok, %{archived_seq: acked_seq}} ->
            pending_after = max(target_seq - acked_seq, 0)
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

            next_entry =
              if pending_after == 0 do
                nil
              else
                %{pending_entry | last_seq: target_seq}
              end

            {next_entry,
             %{
               attempted: attempted,
               inserted: inserted,
               bytes_attempted: bytes_attempted,
               bytes_inserted: bytes_inserted,
               pending_after: pending_after
             }}

          _ack_error ->
            pending_after = max(target_seq - archived_seq, 0)

            {%{pending_entry | last_seq: target_seq},
             %{
               attempted: attempted,
               inserted: 0,
               bytes_attempted: bytes_attempted,
               bytes_inserted: 0,
               pending_after: pending_after
             }}
        end

      {:error, _reason} ->
        pending_after = max(target_seq - archived_seq, 0)

        {%{pending_entry | last_seq: target_seq},
         %{
           attempted: attempted,
           inserted: 0,
           bytes_attempted: bytes_attempted,
           bytes_inserted: 0,
           pending_after: pending_after
         }}
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

  defp older_timestamp(%DateTime{} = left, %DateTime{} = right) do
    if DateTime.compare(left, right) == :gt, do: right, else: left
  end

  defp older_timestamp(%NaiveDateTime{} = left, %NaiveDateTime{} = right) do
    if NaiveDateTime.compare(left, right) == :gt, do: right, else: left
  end

  defp older_timestamp(%DateTime{} = left, %NaiveDateTime{} = right) do
    older_timestamp(left, DateTime.from_naive!(right, "Etc/UTC"))
  end

  defp older_timestamp(%NaiveDateTime{} = left, %DateTime{} = right) do
    older_timestamp(DateTime.from_naive!(left, "Etc/UTC"), right)
  end

  defp older_timestamp(_left, right), do: right

  defp oldest_age_seconds(pending) when is_map(pending) do
    now = DateTime.utc_now()

    pending
    |> Map.values()
    |> Enum.map(&Map.get(&1, :oldest_inserted_at))
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&to_datetime/1)
    |> case do
      [] ->
        0

      timestamps ->
        timestamps
        |> Enum.map(fn ts -> max(DateTime.diff(now, ts, :second), 0) end)
        |> Enum.max()
    end
  end

  defp to_datetime(%DateTime{} = datetime), do: datetime
  defp to_datetime(%NaiveDateTime{} = datetime), do: DateTime.from_naive!(datetime, "Etc/UTC")
  defp to_datetime(_other), do: DateTime.utc_now()

  defp zero_stats do
    %{attempted: 0, inserted: 0, bytes_attempted: 0, bytes_inserted: 0, pending_after: 0}
  end

  defp merge_stats(left, right) do
    %{
      attempted: left.attempted + right.attempted,
      inserted: left.inserted + right.inserted,
      bytes_attempted: left.bytes_attempted + right.bytes_attempted,
      bytes_inserted: left.bytes_inserted + right.bytes_inserted,
      pending_after: left.pending_after + right.pending_after
    }
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_tick, interval)
  end
end
