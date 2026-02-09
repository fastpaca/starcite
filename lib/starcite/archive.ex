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
  @retry_tab :starcite_archive_retry_sessions

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Append committed events to the pending archive queue (ETS).

  Events are normalized into the archive row shape used by adapters.
  """
  @spec append_events(String.t(), [map()]) :: :ok | {:error, term()}
  def append_events(session_id, events)
      when is_binary(session_id) and is_list(events) do
    if events == [] do
      :ok
    else
      case append_to_queue(session_id, events) do
        :ok -> :ok
        {:error, reason} -> handle_enqueue_failure(session_id, events, reason)
      end
    end
  end

  @impl true
  def init(opts) do
    :ok = ensure_queue_tables()
    :ok = ensure_retry_table()

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
    _ = append_to_queue(session_id, events)
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
    :ok = ensure_queue_tables()
    :ok = ensure_retry_table()

    start = System.monotonic_time(:millisecond)
    session_ids = Enum.uniq(sessions_list() ++ retry_sessions_list())

    {attempted_total, inserted_total, bytes_attempted_total, bytes_inserted_total, cursors} =
      Enum.reduce(session_ids, {0, 0, 0, 0, cursors}, fn session_id,
                                                         {attempted_acc, inserted_acc,
                                                          bytes_att_acc, bytes_ins_acc,
                                                          cursors_acc} ->
        {{attempted, inserted, bytes_attempted, bytes_inserted}, cursors_acc} =
          flush_session(session_id, adapter, cursors_acc, state)

        {attempted_acc + attempted, inserted_acc + inserted, bytes_att_acc + bytes_attempted,
         bytes_ins_acc + bytes_inserted, cursors_acc}
      end)

    elapsed = System.monotonic_time(:millisecond) - start
    pending_events = :ets.info(@events_tab, :size) || 0
    pending_sessions = :ets.info(@sessions_tab, :size) || 0
    pending_retry_markers = :ets.info(@retry_tab, :size) || 0

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
      bytes_inserted_total,
      pending_retry_markers
    )

    %{state | cursors: cursors}
  end

  defp flush_session(session_id, adapter, cursors, state) do
    case load_cursor(session_id, cursors) do
      {:ok, cursor, cursors} ->
        events = events_for_flush(session_id, cursor, state)

        case build_flush_batch(events, cursor) do
          :stale ->
            maybe_clear_retry_marker(session_id, cursor)
            {pending_after, cursors} = trim_and_retain_cursor(session_id, cursor, cursors)

            cursors =
              maybe_drop_cursor(cursors, session_id, pending_after)

            {{0, 0, 0, 0}, cursors}

          {:ok, rows, upto_seq} ->
            attempted = length(rows)
            bytes_attempted = Enum.reduce(rows, 0, fn event, acc -> acc + approx_bytes(event) end)

            case adapter.write_events(rows) do
              {:ok, _count} ->
                handle_post_write_ack(
                  session_id,
                  cursor,
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
         upto_seq,
         attempted,
         bytes_attempted,
         cursors
       ) do
    case Runtime.ack_archived_if_current(session_id, cursor, upto_seq) do
      {:ok, %{archived_seq: archived_seq, trimmed: _trimmed}}
      when is_integer(archived_seq) and archived_seq >= cursor ->
        trim_local(session_id, archived_seq)
        maybe_clear_retry_marker(session_id, archived_seq)
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
        maybe_clear_retry_marker(session_id, current_archived_seq)

        {pending_after, cursors} =
          trim_and_retain_cursor(session_id, current_archived_seq, cursors)

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
    if :ets.whereis(@sessions_tab) == :undefined do
      []
    else
      :ets.tab2list(@sessions_tab) |> Enum.map(fn {id, _} -> id end)
    end
  catch
    :error, :badarg -> []
  end

  defp retry_sessions_list do
    if :ets.whereis(@retry_tab) == :undefined do
      []
    else
      :ets.tab2list(@retry_tab) |> Enum.map(fn {id, _} -> id end)
    end
  catch
    :error, :badarg -> []
  end

  defp take_pending_events(session_id, _state) do
    # Select a chunk of rows in seq-ascending order for this session
    limit = archive_batch_limit()

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

  defp events_for_flush(session_id, cursor, state) do
    queued = take_pending_events(session_id, state)
    limit = archive_batch_limit()

    case retry_upto(session_id) do
      retry_upto when is_integer(retry_upto) and retry_upto > cursor ->
        recovered = recover_events_from_runtime(session_id, cursor, limit)
        merge_events(queued, recovered, limit)

      _ ->
        queued
    end
  end

  defp recover_events_from_runtime(session_id, cursor, limit)
       when is_binary(session_id) and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
              limit > 0 do
    case Runtime.get_events_from_cursor(session_id, cursor, limit) do
      {:ok, events} when is_list(events) ->
        if events == [] do
          []
        else
          Telemetry.archive_enqueue_retry(:recovered)
          Enum.map(events, &runtime_event_to_archive_row(session_id, &1))
        end

      {:error, _reason} ->
        Telemetry.archive_enqueue_retry(:recover_failed)
        []
    end
  end

  defp runtime_event_to_archive_row(session_id, event) do
    base =
      event
      |> normalize_event()
      |> Map.put(:session_id, session_id)
      |> Map.put(:seq, Map.get(event, :seq))

    base
  end

  defp merge_events(queued, recovered, limit)
       when is_list(queued) and is_list(recovered) and is_integer(limit) and limit > 0 do
    (queued ++ recovered)
    |> Enum.reduce(%{}, fn event, acc ->
      case Map.get(event, :seq) do
        seq when is_integer(seq) and seq >= 0 -> Map.put(acc, seq, event)
        _ -> acc
      end
    end)
    |> Map.values()
    |> Enum.sort_by(& &1.seq)
    |> Enum.take(limit)
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
  catch
    :error, :badarg -> Enum.reverse(acc)
  end

  defp do_select_take_cont(:"$end_of_table", _remaining, acc), do: Enum.reverse(acc)
  defp do_select_take_cont(_cont, remaining, acc) when remaining <= 0, do: Enum.reverse(acc)

  defp do_select_take_cont(cont, remaining, acc) when remaining > 0 do
    case :ets.select(cont) do
      {[row], cont2} -> do_select_take_cont(cont2, remaining - 1, [row | acc])
      {[], cont2} -> do_select_take_cont(cont2, 0, acc)
      :"$end_of_table" -> Enum.reverse(acc)
    end
  catch
    :error, :badarg -> Enum.reverse(acc)
  end

  defp build_flush_batch(rows, cursor)
       when is_list(rows) and is_integer(cursor) and cursor >= 0 do
    fresh_rows = Enum.drop_while(rows, &(&1.seq <= cursor))

    case fresh_rows do
      [] ->
        :stale

      [%{seq: _first_pending_seq} | _] = fresh_rows ->
        upto_seq = fresh_rows |> List.last() |> Map.fetch!(:seq)
        {:ok, fresh_rows, upto_seq}
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
  catch
    :error, :badarg -> :ok
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
  catch
    :error, :badarg -> :ok
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
  catch
    :error, :badarg -> 0
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
  catch
    :error, :badarg -> nil
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
      :ok
    else
      {:error, :tables_unavailable}
    end
  catch
    :error, :badarg ->
      {:error, :badarg}
  end

  defp handle_enqueue_failure(session_id, events, reason) do
    strict_mode = enqueue_strict_mode()
    event_count = length(events)
    Telemetry.archive_enqueue_failure(reason, event_count, strict_mode)

    max_seq = max_event_seq(events)

    case mark_retry(session_id, max_seq) do
      :ok ->
        Telemetry.archive_enqueue_retry(:marked)

      {:error, _retry_reason} ->
        Telemetry.archive_enqueue_retry(:mark_failed)
    end

    case strict_mode do
      :strict -> {:error, {:archive_enqueue_failed, reason}}
      :relaxed -> :ok
    end
  end

  defp mark_retry(session_id, max_seq)
       when is_binary(session_id) and is_integer(max_seq) and max_seq >= 0 do
    :ok = ensure_retry_table()

    case :ets.lookup(@retry_tab, session_id) do
      [{^session_id, existing}] when is_integer(existing) and existing >= max_seq ->
        :ok

      _ ->
        :ets.insert(@retry_tab, {session_id, max_seq})
        :ok
    end
  catch
    :error, :badarg -> {:error, :retry_table_unavailable}
  end

  defp retry_upto(session_id) when is_binary(session_id) do
    case :ets.lookup(@retry_tab, session_id) do
      [{^session_id, upto}] when is_integer(upto) and upto >= 0 -> upto
      _ -> nil
    end
  catch
    :error, :badarg -> nil
  end

  defp maybe_clear_retry_marker(session_id, archived_seq)
       when is_binary(session_id) and is_integer(archived_seq) and archived_seq >= 0 do
    case retry_upto(session_id) do
      upto when is_integer(upto) and upto <= archived_seq ->
        :ets.delete(@retry_tab, session_id)
        Telemetry.archive_enqueue_retry(:cleared)
        :ok

      _ ->
        :ok
    end
  catch
    :error, :badarg -> :ok
  end

  defp max_event_seq(events) when is_list(events) do
    Enum.reduce(events, 0, fn
      %{seq: seq}, acc when is_integer(seq) and seq >= 0 -> max(seq, acc)
      _, acc -> acc
    end)
  end

  defp archive_batch_limit do
    Application.get_env(:starcite, :archive_batch_size, 5_000)
  end

  defp enqueue_strict_mode do
    case Application.get_env(:starcite, :archive_enqueue_strict, :unset) do
      :unset -> parse_strict_mode(System.get_env("STARCITE_ARCHIVE_ENQUEUE_STRICT"))
      value -> parse_strict_mode(value)
    end
  end

  defp parse_strict_mode(value) do
    case value do
      nil -> :strict
      true -> :strict
      false -> :relaxed
      :strict -> :strict
      :relaxed -> :relaxed
      "true" -> :strict
      "1" -> :strict
      "yes" -> :strict
      "on" -> :strict
      "false" -> :relaxed
      "0" -> :relaxed
      "no" -> :relaxed
      "off" -> :relaxed
      _ -> :strict
    end
  end

  defp ensure_queue_tables do
    with :ok <-
           ensure_table(
             @events_tab,
             [:ordered_set, :public, :named_table, {:read_concurrency, true}]
           ),
         :ok <-
           ensure_table(
             @sessions_tab,
             [:set, :public, :named_table, {:read_concurrency, true}]
           ) do
      :ok
    end
  end

  defp ensure_retry_table do
    ensure_table(
      @retry_tab,
      [:set, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, true}]
    )
  end

  defp ensure_table(table, opts) do
    if :ets.whereis(table) == :undefined do
      :ets.new(table, opts)
      :ok
    else
      :ok
    end
  catch
    :error, :badarg ->
      if :ets.whereis(table) == :undefined, do: {:error, :table_unavailable}, else: :ok
  end

  defp queue_tables_ready? do
    :ets.whereis(@events_tab) != :undefined and :ets.whereis(@sessions_tab) != :undefined
  end
end
