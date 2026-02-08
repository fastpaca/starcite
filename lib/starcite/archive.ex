defmodule Starcite.Archive do
  @moduledoc """
  Leader-local archive queue and flush loop.

  - append_events/2 is invoked by the Raft FSM effects after commits
  - holds pending events in ETS until flushed to cold storage
  - after flush, advances archive watermark via Runtime.ack_archived/2

  Designed so adapters for Postgres or S3 can be plugged in without touching
  Raft state or HTTP paths.
  """

  use GenServer

  alias Starcite.Runtime

  @events_tab :starcite_archive_events
  @sessions_tab :starcite_archive_sessions

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Append committed events to the pending archive queue (ETS).

  Events are normalized into the archive row shape used by adapters.
  """
  @spec append_events(String.t(), [map()]) :: :ok
  def append_events(session_id, events)
      when is_binary(session_id) and is_list(events) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      _pid -> GenServer.cast(__MODULE__, {:append, session_id, events})
    end

    :ok
  end

  @impl true
  def init(opts) do
    # ETS tables (uncapped by design)
    :ets.new(@events_tab, [:ordered_set, :public, :named_table, {:read_concurrency, true}])
    :ets.new(@sessions_tab, [:set, :public, :named_table, {:read_concurrency, true}])

    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.fetch!(opts, :adapter)
    adapter_opts = Keyword.get(opts, :adapter_opts, [])

    {:ok, _} = adapter_mod.start_link(adapter_opts)

    state = %{
      interval: interval,
      adapter: adapter_mod,
      adapter_opts: adapter_opts
    }

    schedule_flush(interval)
    {:ok, state}
  end

  @impl true
  def handle_cast({:append, session_id, events}, state) do
    Enum.each(events, fn event ->
      key = {session_id, event.seq}
      :ets.insert(@events_tab, {key, normalize_event(event)})
    end)

    :ets.insert(@sessions_tab, {session_id, :pending})
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    flush_all(state)
    schedule_flush(state.interval)
    {:noreply, state}
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_tick, interval)
  end

  defp flush_all(%{adapter: adapter} = state) do
    start = System.monotonic_time(:millisecond)
    session_ids = sessions_list()

    {attempted_total, inserted_total, bytes_attempted_total, bytes_inserted_total} =
      Enum.reduce(session_ids, {0, 0, 0, 0}, fn session_id,
                                                {attempted_acc, inserted_acc, bytes_att_acc,
                                                 bytes_ins_acc} ->
        events = take_pending_events(session_id, state)
        attempted = length(events)
        bytes_attempted = Enum.reduce(events, 0, fn event, acc -> acc + approx_bytes(event) end)

        if events != [] do
          case adapter.write_events(events) do
            {:ok, _count} ->
              upto_seq = contiguous_upto(events)

              case Runtime.ack_archived(session_id, upto_seq) do
                {:ok, %{archived_seq: _archived, trimmed: _}} ->
                  trim_local(session_id, upto_seq)
                  pending_after = pending_count(session_id)
                  avg_event_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0

                  Starcite.Observability.Telemetry.archive_batch(
                    session_id,
                    attempted,
                    bytes_attempted,
                    avg_event_bytes,
                    pending_after
                  )

                  maybe_clear_session(session_id)

                  {attempted_acc + attempted, inserted_acc + attempted,
                   bytes_att_acc + bytes_attempted, bytes_ins_acc + bytes_attempted}

                _ ->
                  {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
                   bytes_ins_acc}
              end

            {:error, _reason} ->
              {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
               bytes_ins_acc}
          end
        else
          {attempted_acc, inserted_acc, bytes_att_acc, bytes_ins_acc}
        end
      end)

    elapsed = System.monotonic_time(:millisecond) - start
    pending_events = :ets.info(@events_tab, :size) || 0
    pending_sessions = :ets.info(@sessions_tab, :size) || 0

    # Emit queue age gauge
    age_seconds = oldest_age_seconds()
    Starcite.Observability.Telemetry.archive_queue_age(age_seconds)

    Starcite.Observability.Telemetry.archive_flush(
      elapsed,
      attempted_total,
      inserted_total,
      pending_events,
      pending_sessions,
      bytes_attempted_total,
      bytes_inserted_total
    )

    :ok
  end

  defp approx_bytes(%{payload: payload, metadata: metadata, refs: refs}) do
    # Approximate payload size by JSON-encoding primary dynamic fields.
    byte_size(Jason.encode!(payload)) +
      byte_size(Jason.encode!(metadata)) +
      byte_size(Jason.encode!(refs))
  end

  defp sessions_list do
    :ets.tab2list(@sessions_tab) |> Enum.map(fn {id, _} -> id end)
  end

  defp take_pending_events(session_id, _state) do
    # Select a chunk of rows in seq-ascending order for this session
    limit = Application.get_env(:starcite, :archive_batch_size, 5_000)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [{{:"$1", :"$2"}}]
      }
    ]

    select_take(@events_tab, ms, limit)
    |> Enum.map(fn {seq, event} ->
      Map.put(event, :session_id, session_id) |> Map.put(:seq, seq)
    end)
    |> Enum.sort_by(& &1.seq)
  end

  defp select_take(tab, ms, limit) do
    do_select_take(tab, ms, limit, [])
  end

  defp do_select_take(_tab, _ms, 0, acc), do: Enum.reverse(acc)

  defp do_select_take(tab, ms, remaining, acc) do
    case :ets.select(tab, ms, 1) do
      {[row], cont} ->
        # delete as we go only after ack; for now just accumulate
        # We'll delete after ack succeeds via trim_local/2
        next_acc = [row | acc]
        _ = cont
        # Use continuation to fetch next; but continuation returns tuples of rows
        do_select_take_cont(cont, remaining - 1, next_acc)

      {[], _} ->
        Enum.reverse(acc)

      :"$end_of_table" ->
        Enum.reverse(acc)
    end
  end

  defp do_select_take_cont(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp do_select_take_cont(_cont, remaining, acc) when remaining <= 0, do: Enum.reverse(acc)

  defp do_select_take_cont(cont, remaining, acc) when remaining > 0 do
    case :ets.select(cont) do
      {[row], cont2} -> do_select_take_cont(cont2, remaining - 1, [row | acc])
      {[], cont2} -> do_select_take_cont(cont2, 0, acc)
      :"$end_of_table" -> Enum.reverse(acc)
    end
  end

  defp contiguous_upto(rows) do
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

  defp trim_local(session_id, upto_seq) do
    # Delete all rows for this session with seq <= upto_seq
    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:"=<", :"$1", upto_seq}],
        [true]
      }
    ]

    :ets.select_delete(@events_tab, ms)
    :ok
  end

  defp maybe_clear_session(session_id) do
    # If no rows remain for this session, clear it from sessions_tab
    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    case :ets.select(@events_tab, ms, 1) do
      {[], _} -> :ets.delete(@sessions_tab, session_id)
      _ -> :ok
    end
  end

  defp pending_count(session_id) do
    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(@events_tab, ms)
  end

  defp oldest_age_seconds do
    now = NaiveDateTime.utc_now()
    session_ids = sessions_list()

    session_ids
    |> Enum.map(&first_row_ts/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn ts -> max(NaiveDateTime.diff(now, ts, :second), 0) end)
    |> case do
      [] -> 0
      ages -> Enum.max(ages)
    end
  end

  defp first_row_ts(session_id) do
    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [:"$2"]
      }
    ]

    case :ets.select(@events_tab, ms, 1) do
      {[event], _cont} -> Map.get(event, :inserted_at)
      _ -> nil
    end
  end

  defp normalize_event(event) do
    %{
      type: Map.get(event, :type),
      payload: Map.get(event, :payload, %{}),
      actor: Map.get(event, :actor),
      source: Map.get(event, :source),
      metadata: Map.get(event, :metadata, %{}),
      refs: Map.get(event, :refs, %{}),
      idempotency_key: Map.get(event, :idempotency_key),
      inserted_at: Map.get(event, :inserted_at, NaiveDateTime.utc_now())
    }
  end
end
