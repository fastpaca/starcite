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
      adapter_opts: adapter_opts
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
              with {:ok, archived_seq} <- fetch_archived_seq(session_id) do
                case anchored_contiguous_upto(events, archived_seq) do
                  {:ok, upto_seq} ->
                    case Runtime.ack_archived(session_id, upto_seq) do
                      {:ok, %{archived_seq: _archived, trimmed: _}} ->
                        trim_local(session_id, upto_seq)
                        pending_after = pending_count(session_id)

                        avg_event_bytes =
                          if attempted > 0, do: div(bytes_attempted, attempted), else: 0

                        Telemetry.archive_batch(
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

                  {:gap, expected_seq, first_pending_seq} ->
                    Telemetry.archive_ack_gap(
                      session_id,
                      archived_seq,
                      expected_seq,
                      first_pending_seq,
                      attempted
                    )

                    {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
                     bytes_ins_acc}

                  :stale ->
                    trim_local(session_id, archived_seq)
                    maybe_clear_session(session_id)

                    {attempted_acc + attempted, inserted_acc, bytes_att_acc + bytes_attempted,
                     bytes_ins_acc}
                end
              else
                {:error, _reason} ->
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

  defp fetch_archived_seq(session_id) when is_binary(session_id) and session_id != "" do
    case Runtime.get_session(session_id) do
      {:ok, %Session{archived_seq: archived_seq}}
      when is_integer(archived_seq) and archived_seq >= 0 ->
        {:ok, archived_seq}

      {:error, reason} ->
        {:error, reason}

      _other ->
        {:error, :unexpected_session_lookup_result}
    end
  end

  defp anchored_contiguous_upto(rows, archived_seq)
       when is_list(rows) and is_integer(archived_seq) and archived_seq >= 0 do
    expected_start = archived_seq + 1
    first_pending_seq = first_pending_seq(rows, archived_seq)

    cond do
      is_nil(first_pending_seq) ->
        :stale

      first_pending_seq > expected_start ->
        {:gap, expected_start, first_pending_seq}

      true ->
        upto_seq =
          rows
          |> Enum.sort_by(& &1.seq)
          |> Enum.reduce_while({expected_start, archived_seq}, fn %{seq: seq},
                                                                  {expected_seq, upto_seq} ->
            cond do
              seq < expected_seq ->
                {:cont, {expected_seq, upto_seq}}

              seq == expected_seq ->
                {:cont, {expected_seq + 1, seq}}

              seq > expected_seq ->
                {:halt, {expected_seq, upto_seq}}
            end
          end)
          |> elem(1)

        if upto_seq > archived_seq do
          {:ok, upto_seq}
        else
          {:gap, expected_start, first_pending_seq}
        end
    end
  end

  defp first_pending_seq(rows, archived_seq)
       when is_list(rows) and is_integer(archived_seq) and archived_seq >= 0 do
    rows
    |> Enum.map(&Map.get(&1, :seq))
    |> Enum.filter(&(is_integer(&1) and &1 > archived_seq))
    |> case do
      [] -> nil
      seqs -> Enum.min(seqs)
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
