defmodule Starcite.Archive do
  @moduledoc """
  Leader-local archive queue and flush loop.

  - append_events/2 is invoked by the Raft FSM effects after commits
  - holds pending events in ETS until flushed to cold storage
  - after flush, advances archive watermark via Runtime.ack_archived_if_current/3

  Designed so adapters for Postgres or S3 can be plugged in without touching
  Raft state or HTTP paths.
  """

  use GenServer

  alias Starcite.Observability.Telemetry
  alias Starcite.Runtime
  alias Starcite.Session

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
    if events != [] do
      append_to_queue(session_id, events)
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
      adapter_opts: adapter_opts,
      cursors: %{}
    }

    schedule_flush(interval)
    {:ok, state}
  end

  @impl true
  def handle_cast({:append, session_id, events}, state) do
    append_to_queue(session_id, events)
    {:noreply, state}
  end

  @impl true
  def handle_info(:flush_tick, state) do
    state = flush_all(state)
    schedule_flush(state.interval)
    {:noreply, state}
  end

  defp schedule_flush(interval) do
    Process.send_after(self(), :flush_tick, interval)
  end

  defp flush_all(%{adapter: adapter, cursors: cursors} = state) do
    start = System.monotonic_time(:millisecond)
    session_ids = sessions_list()

    {attempted_total, inserted_total, bytes_attempted_total, bytes_inserted_total, cursors} =
      Enum.reduce(session_ids, {0, 0, 0, 0, cursors}, fn session_id,
                                                         {attempted_acc, inserted_acc,
                                                          bytes_att_acc, bytes_ins_acc,
                                                          cursors_acc} ->
        events = take_pending_events(session_id, state)

        {{attempted, inserted, bytes_attempted, bytes_inserted}, cursors_acc} =
          flush_session(session_id, events, adapter, cursors_acc)

        {attempted_acc + attempted, inserted_acc + inserted, bytes_att_acc + bytes_attempted,
         bytes_ins_acc + bytes_inserted, cursors_acc}
      end)

    elapsed = System.monotonic_time(:millisecond) - start
    pending_events = :ets.info(@events_tab, :size) || 0
    pending_sessions = :ets.info(@sessions_tab, :size) || 0

    # Emit queue age gauge
    age_seconds = oldest_age_seconds()
    Telemetry.archive_queue_age(age_seconds)

    Telemetry.archive_flush(
      elapsed,
      attempted_total,
      inserted_total,
      pending_events,
      pending_sessions,
      bytes_attempted_total,
      bytes_inserted_total
    )

    %{state | cursors: cursors}
  end

  defp flush_session(_session_id, [], _adapter, cursors), do: {{0, 0, 0, 0}, cursors}

  defp flush_session(session_id, events, adapter, cursors) do
    case load_cursor(session_id, cursors) do
      {:ok, cursor, cursors} ->
        case build_flush_batch(events, cursor) do
          :stale ->
            {pending_after, cursors} = trim_and_retain_cursor(session_id, cursor, cursors)

            cursors =
              maybe_drop_cursor(cursors, session_id, pending_after)

            {{0, 0, 0, 0}, cursors}

          {:gap, first_pending_seq, pending_rows} ->
            {pending_after, cursors} = trim_and_retain_cursor(session_id, cursor, cursors)

            Telemetry.archive_ack_gap(
              session_id,
              cursor,
              cursor + 1,
              first_pending_seq,
              pending_rows
            )

            cursors =
              maybe_drop_cursor(cursors, session_id, pending_after)

            {{0, 0, 0, 0}, cursors}

          {:ok, rows, first_pending_seq, upto_seq} ->
            attempted = length(rows)
            bytes_attempted = Enum.reduce(rows, 0, fn event, acc -> acc + approx_bytes(event) end)

            case adapter.write_events(rows) do
              {:ok, _count} ->
                handle_post_write_ack(
                  session_id,
                  cursor,
                  first_pending_seq,
                  upto_seq,
                  attempted,
                  bytes_attempted,
                  cursors
                )

              {:error, _reason} ->
                {{attempted, 0, bytes_attempted, 0}, cursors}
            end
        end

      {:error, cursors} ->
        {{0, 0, 0, 0}, cursors}
    end
  end

  defp load_cursor(session_id, cursors) when is_binary(session_id) and session_id != "" do
    case Map.fetch(cursors, session_id) do
      {:ok, cursor} ->
        {:ok, cursor, cursors}

      :error ->
        case Runtime.get_session(session_id) do
          {:ok, %Session{archived_seq: archived_seq}}
          when is_integer(archived_seq) and archived_seq >= 0 ->
            {:ok, archived_seq, Map.put(cursors, session_id, archived_seq)}

          _ ->
            {:error, cursors}
        end
    end
  end

  defp handle_post_write_ack(
         session_id,
         cursor,
         first_pending_seq,
         upto_seq,
         attempted,
         bytes_attempted,
         cursors
       ) do
    case Runtime.ack_archived_if_current(session_id, cursor, upto_seq) do
      {:ok, %{archived_seq: archived_seq, trimmed: _trimmed}}
      when is_integer(archived_seq) and archived_seq >= cursor ->
        trim_local(session_id, archived_seq)
        pending_after = pending_count(session_id)
        avg_event_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0

        Telemetry.archive_batch(
          session_id,
          attempted,
          bytes_attempted,
          avg_event_bytes,
          pending_after
        )

        maybe_clear_session(session_id)

        cursors =
          cursors
          |> Map.put(session_id, archived_seq)
          |> maybe_drop_cursor(session_id, pending_after)

        {{attempted, attempted, bytes_attempted, bytes_attempted}, cursors}

      {:error, {:archived_seq_mismatch, _expected_archived_seq, current_archived_seq}}
      when is_integer(current_archived_seq) and current_archived_seq >= 0 ->
        {pending_after, cursors} =
          trim_and_retain_cursor(session_id, current_archived_seq, cursors)

        if current_archived_seq + 1 < first_pending_seq do
          Telemetry.archive_ack_gap(
            session_id,
            current_archived_seq,
            current_archived_seq + 1,
            first_pending_seq,
            attempted
          )
        end

        cursors =
          cursors
          |> maybe_drop_cursor(session_id, pending_after)

        {{attempted, 0, bytes_attempted, 0}, cursors}

      {:error, _reason} ->
        {{attempted, 0, bytes_attempted, 0}, cursors}

      {:timeout, _leader} ->
        {{attempted, 0, bytes_attempted, 0}, cursors}
    end
  end

  defp trim_and_retain_cursor(session_id, cursor, cursors)
       when is_integer(cursor) and cursor >= 0 do
    trim_local(session_id, cursor)
    pending_after = pending_count(session_id)
    maybe_clear_session(session_id)
    {pending_after, Map.put(cursors, session_id, cursor)}
  end

  defp maybe_drop_cursor(cursors, session_id, pending_after)
       when is_integer(pending_after) and pending_after <= 0,
       do: Map.delete(cursors, session_id)

  defp maybe_drop_cursor(cursors, _session_id, _pending_after), do: cursors

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

  defp build_flush_batch(rows, cursor)
       when is_list(rows) and is_integer(cursor) and cursor >= 0 do
    fresh_rows = Enum.drop_while(rows, &(&1.seq <= cursor))

    case fresh_rows do
      [] ->
        :stale

      [%{seq: first_pending_seq} | _]
      when is_integer(first_pending_seq) and first_pending_seq > cursor + 1 ->
        {:gap, first_pending_seq, length(fresh_rows)}

      _ ->
        {contiguous_rows, _next_expected} =
          Enum.reduce_while(fresh_rows, {[], cursor + 1}, fn %{seq: seq} = row,
                                                             {acc, expected_seq} ->
            cond do
              seq < expected_seq ->
                {:cont, {acc, expected_seq}}

              seq == expected_seq ->
                {:cont, {[row | acc], expected_seq + 1}}

              seq > expected_seq ->
                {:halt, {acc, expected_seq}}
            end
          end)

        contiguous_rows = Enum.reverse(contiguous_rows)

        case contiguous_rows do
          [] ->
            :stale

          [%{seq: first_pending_seq} | _] = contiguous_rows ->
            upto_seq = contiguous_rows |> List.last() |> Map.fetch!(:seq)
            {:ok, contiguous_rows, first_pending_seq, upto_seq}
        end
    end
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

  defp append_to_queue(session_id, events) do
    if queue_tables_ready?() do
      rows =
        Enum.map(events, fn event ->
          {{session_id, event.seq}, normalize_event(event)}
        end)

      :ets.insert(@events_tab, rows)
      :ets.insert(@sessions_tab, {session_id, :pending})
    end

    :ok
  catch
    :error, :badarg ->
      # Tables may be recreated during process restarts; skip enqueue for this call.
      :ok
  end

  defp queue_tables_ready? do
    :ets.whereis(@events_tab) != :undefined and :ets.whereis(@sessions_tab) != :undefined
  end
end
