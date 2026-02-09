defmodule Starcite.Archive do
  @moduledoc """
  Leader-local archive coordinator and flush loop.

  - mark_dirty/2 is invoked by Raft effects after commits
  - holds dirty session references in ETS (no payload duplication)
  - fetches unarchived ranges from Raft by cursor in bounded batches
  - advances archive watermark via Runtime.ack_archived_if_current/3
  - runs bounded reconciliation scans to recover missed dirty marks
  """

  use GenServer

  alias Starcite.Observability.Telemetry
  alias Starcite.Runtime
  alias Starcite.Runtime.RaftManager
  alias Starcite.Session

  @sessions_tab :starcite_archive_sessions

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Mark a session as dirty for archival.
  """
  @spec mark_dirty(String.t(), non_neg_integer()) :: :ok
  def mark_dirty(session_id, seq \\ 0)
      when is_binary(session_id) and session_id != "" do
    normalized_seq =
      case seq do
        value when is_integer(value) and value >= 0 -> value
        _ -> 0
      end

    mark_session_dirty(session_id, normalized_seq)
    :ok
  end

  @doc """
  Backwards-compatibility shim for older effect payloads.
  """
  @spec append_events(String.t(), [map()]) :: :ok
  def append_events(session_id, events)
      when is_binary(session_id) and is_list(events) do
    if events != [] do
      max_seq = events |> Enum.map(&Map.get(&1, :seq, 0)) |> Enum.max(fn -> 0 end)
      mark_dirty(session_id, max_seq)
    end

    :ok
  end

  @impl true
  def init(opts) do
    :ets.new(
      @sessions_tab,
      [:set, :public, :named_table, {:read_concurrency, true}, {:write_concurrency, true}]
    )

    interval = Keyword.get(opts, :flush_interval_ms, 5_000)
    adapter_mod = Keyword.fetch!(opts, :adapter)
    adapter_opts = Keyword.get(opts, :adapter_opts, [])
    reconcile_enabled = Keyword.get(opts, :reconcile_enabled, true)

    reconcile_batch_size =
      normalize_reconcile_batch_size(Keyword.get(opts, :reconcile_batch_size, 64))

    reconcile_group = normalize_group_start(Keyword.get(opts, :reconcile_start_group, 0))

    {:ok, _} = adapter_mod.start_link(adapter_opts)

    state = %{
      interval: interval,
      adapter: adapter_mod,
      adapter_opts: adapter_opts,
      cursors: %{},
      reconcile_enabled: reconcile_enabled,
      reconcile_batch_size: reconcile_batch_size,
      reconcile_group: reconcile_group,
      reconcile_after: nil
    }

    schedule_flush(interval)
    {:ok, state}
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
    batch_limit = archive_batch_limit()
    session_ids = sessions_list()

    {attempted_total, inserted_total, bytes_attempted_total, bytes_inserted_total, cursors} =
      Enum.reduce(session_ids, {0, 0, 0, 0, cursors}, fn session_id,
                                                         {attempted_acc, inserted_acc,
                                                          bytes_att_acc, bytes_ins_acc,
                                                          cursors_acc} ->
        {{attempted, inserted, bytes_attempted, bytes_inserted}, cursors_acc} =
          flush_session(session_id, adapter, cursors_acc, batch_limit)

        {attempted_acc + attempted, inserted_acc + inserted, bytes_att_acc + bytes_attempted,
         bytes_ins_acc + bytes_inserted, cursors_acc}
      end)

    state = maybe_reconcile(state)

    elapsed = System.monotonic_time(:millisecond) - start
    pending_sessions = :ets.info(@sessions_tab, :size) || 0
    pending_refs = pending_sessions

    age_seconds = oldest_age_seconds()
    Telemetry.archive_queue_age(age_seconds)

    Telemetry.archive_flush(
      elapsed,
      attempted_total,
      inserted_total,
      pending_refs,
      pending_sessions,
      bytes_attempted_total,
      bytes_inserted_total
    )

    %{state | cursors: cursors}
  end

  defp flush_session(session_id, adapter, cursors, batch_limit) do
    case load_cursor(session_id, cursors) do
      {:ok, cursor, cursors} ->
        case fetch_unarchived(session_id, cursor, batch_limit) do
          {:ok, events} ->
            case build_flush_batch(events, cursor) do
              :stale ->
                clear_session_marker_if_caught_up(session_id, cursor)
                {{0, 0, 0, 0}, Map.delete(cursors, session_id)}

              {:ok, rows, upto_seq} ->
                attempted = length(rows)

                bytes_attempted =
                  Enum.reduce(rows, 0, fn event, acc -> acc + approx_bytes(event) end)

                case adapter.write_events(rows) do
                  {:ok, _count} ->
                    handle_post_write_ack(
                      session_id,
                      cursor,
                      upto_seq,
                      attempted,
                      bytes_attempted,
                      batch_limit,
                      cursors
                    )

                  {:error, _reason} ->
                    {{attempted, 0, bytes_attempted, 0}, cursors}
                end
            end

          {:error, _reason} ->
            {{0, 0, 0, 0}, cursors}
        end

      {:error, cursors} ->
        clear_session_marker_if_caught_up(session_id, 0)
        {{0, 0, 0, 0}, Map.delete(cursors, session_id)}
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
         batch_limit,
         cursors
       ) do
    case Runtime.ack_archived_if_current(session_id, cursor, upto_seq) do
      {:ok, %{archived_seq: archived_seq, trimmed: _trimmed}}
      when is_integer(archived_seq) and archived_seq >= cursor ->
        keep_dirty = attempted >= batch_limit
        avg_event_bytes = if attempted > 0, do: div(bytes_attempted, attempted), else: 0
        pending_after = if keep_dirty, do: 1, else: 0

        Telemetry.archive_batch(
          session_id,
          attempted,
          bytes_attempted,
          avg_event_bytes,
          pending_after
        )

        if keep_dirty, do: :ok, else: clear_session_marker_if_caught_up(session_id, archived_seq)

        cursors =
          cursors
          |> Map.put(session_id, archived_seq)
          |> maybe_drop_cursor(session_id, keep_dirty)

        {{attempted, attempted, bytes_attempted, bytes_attempted}, cursors}

      {:error, {:archived_seq_mismatch, _expected_archived_seq, current_archived_seq}}
      when is_integer(current_archived_seq) and current_archived_seq >= 0 ->
        clear_session_marker_if_caught_up(session_id, current_archived_seq)
        {{attempted, 0, bytes_attempted, 0}, Map.put(cursors, session_id, current_archived_seq)}

      {:error, _reason} ->
        {{attempted, 0, bytes_attempted, 0}, cursors}

      {:timeout, _leader} ->
        {{attempted, 0, bytes_attempted, 0}, cursors}
    end
  end

  defp maybe_drop_cursor(cursors, _session_id, true), do: cursors
  defp maybe_drop_cursor(cursors, session_id, false), do: Map.delete(cursors, session_id)

  defp approx_bytes(%{payload: payload, metadata: metadata, refs: refs}) do
    byte_size(Jason.encode!(payload)) +
      byte_size(Jason.encode!(metadata)) +
      byte_size(Jason.encode!(refs))
  end

  defp fetch_unarchived(session_id, cursor, limit)
       when is_binary(session_id) and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
              limit > 0 do
    case Runtime.get_events_from_cursor(session_id, cursor, limit) do
      {:ok, events} when is_list(events) ->
        rows =
          events
          |> Enum.map(&normalize_runtime_event(session_id, &1))
          |> Enum.sort_by(& &1.seq)

        {:ok, rows}

      _ ->
        {:error, :unavailable}
    end
  end

  defp normalize_runtime_event(session_id, event) do
    %{
      session_id: session_id,
      seq: Map.get(event, :seq),
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

  defp build_flush_batch(rows, cursor)
       when is_list(rows) and is_integer(cursor) and cursor >= 0 do
    fresh_rows = Enum.drop_while(rows, &(&1.seq <= cursor))

    case fresh_rows do
      [] ->
        :stale

      [%{seq: _first_pending_seq} | _] = pending ->
        upto_seq = pending |> List.last() |> Map.fetch!(:seq)
        {:ok, pending, upto_seq}
    end
  end

  defp mark_session_dirty(session_id, seq)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq >= 0 do
    marked_at = System.system_time(:second)

    case :ets.lookup(@sessions_tab, session_id) do
      [] ->
        :ets.insert(@sessions_tab, {session_id, marked_at, seq})

      [{^session_id, existing_marked_at, existing_seq}]
      when is_integer(existing_marked_at) and is_integer(existing_seq) ->
        :ets.insert(@sessions_tab, {session_id, existing_marked_at, max(existing_seq, seq)})

      _ ->
        :ets.insert(@sessions_tab, {session_id, marked_at, seq})
    end

    :ok
  catch
    :error, :badarg -> :ok
  end

  defp clear_session_marker_if_caught_up(session_id, archived_seq)
       when is_binary(session_id) and is_integer(archived_seq) and archived_seq >= 0 do
    case :ets.lookup(@sessions_tab, session_id) do
      [{^session_id, _marked_at, max_dirty_seq}]
      when is_integer(max_dirty_seq) and max_dirty_seq <= archived_seq ->
        :ets.delete(@sessions_tab, session_id)

      _ ->
        :ok
    end

    :ok
  catch
    :error, :badarg -> :ok
  end

  defp sessions_list do
    :ets.tab2list(@sessions_tab) |> Enum.map(fn {id, _marked_at, _max_dirty_seq} -> id end)
  catch
    :error, :badarg -> []
  end

  defp oldest_age_seconds do
    now = System.system_time(:second)

    :ets.tab2list(@sessions_tab)
    |> Enum.map(fn
      {_session_id, marked_at, _max_dirty_seq} when is_integer(marked_at) ->
        max(now - marked_at, 0)

      _ ->
        0
    end)
    |> case do
      [] -> 0
      ages -> Enum.max(ages)
    end
  catch
    :error, :badarg -> 0
  end

  defp maybe_reconcile(%{reconcile_enabled: false} = state), do: state

  defp maybe_reconcile(state) do
    group_id = state.reconcile_group
    after_session_id = state.reconcile_after
    batch_size = state.reconcile_batch_size
    start = System.monotonic_time(:millisecond)

    case Runtime.list_lagging_sessions(group_id, after_session_id, batch_size) do
      {:ok, %{session_ids: session_ids, has_more: has_more, next_after: next_after}} ->
        Enum.each(session_ids, &mark_session_dirty(&1, 0))

        elapsed_ms = System.monotonic_time(:millisecond) - start

        Telemetry.archive_reconcile(
          group_id,
          elapsed_ms,
          length(session_ids),
          length(session_ids),
          has_more,
          :ok
        )

        advance_reconcile_cursor(state, has_more, next_after)

      {:error, _reason} ->
        elapsed_ms = System.monotonic_time(:millisecond) - start
        Telemetry.archive_reconcile(group_id, elapsed_ms, 0, 0, false, :error)
        advance_reconcile_cursor(state, false, nil)
    end
  end

  defp advance_reconcile_cursor(state, true, next_after) when is_binary(next_after) do
    %{state | reconcile_after: next_after}
  end

  defp advance_reconcile_cursor(state, _has_more, _next_after) do
    next_group = rem(state.reconcile_group + 1, RaftManager.num_groups())
    %{state | reconcile_group: next_group, reconcile_after: nil}
  end

  defp normalize_group_start(value) when is_integer(value) and value >= 0 do
    rem(value, RaftManager.num_groups())
  end

  defp normalize_group_start(_value), do: 0

  defp normalize_reconcile_batch_size(value) when is_integer(value) and value > 0, do: value
  defp normalize_reconcile_batch_size(_value), do: 64

  defp archive_batch_limit do
    Application.get_env(:starcite, :archive_batch_size, 5_000)
  end
end
