defmodule StarciteWeb.TailStream do
  @moduledoc false

  alias Phoenix.PubSub
  alias Starcite.Auth.Principal
  alias Starcite.Cursor
  alias Starcite.DataPlane.{CursorUpdate, EventStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.ReadPath
  alias StarciteWeb.Auth.Context

  @replay_batch_size 1_000
  @catchup_interval_ms 5_000

  @type state :: %{
          required(:session_id) => String.t(),
          required(:principal) => Principal.t() | nil,
          required(:cursor) => non_neg_integer(),
          required(:cursor_epoch) => non_neg_integer() | nil,
          required(:frame_batch_size) => pos_integer(),
          required(:replay_queue) => :queue.queue(),
          required(:replay_done) => boolean(),
          required(:live_buffer) => map(),
          required(:drain_scheduled) => boolean(),
          required(:catchup_timer_ref) => reference() | nil,
          required(:auth_expires_at) => non_neg_integer() | nil
        }

  @type response ::
          {:ok, state()}
          | {:emit, {:events, [map()]} | {:gap, map()}, state()}
          | {:close, pos_integer(), String.t(), state()}
          | {:stop, term(), state()}

  @spec init(map()) :: {:ok, state()}
  def init(%{
        session_id: session_id,
        cursor: cursor,
        frame_batch_size: frame_batch_size,
        principal: principal,
        auth_context: %Context{} = auth_context
      })
      when is_binary(session_id) and session_id != "" and is_map(cursor) and
             is_integer(frame_batch_size) and frame_batch_size > 0 and
             (is_struct(principal, Principal) or is_nil(principal)) do
    :ok = PubSub.subscribe(Starcite.PubSub, CursorUpdate.topic(session_id))
    auth_expires_at = auth_expires_at(auth_context)
    cursor_seq = Map.fetch!(cursor, :seq)
    cursor_epoch = Map.get(cursor, :epoch)

    state = %{
      session_id: session_id,
      principal: principal,
      cursor: cursor_seq,
      cursor_epoch: cursor_epoch,
      frame_batch_size: frame_batch_size,
      replay_queue: :queue.new(),
      replay_done: false,
      live_buffer: %{},
      drain_scheduled: false,
      catchup_timer_ref: nil,
      auth_expires_at: auth_expires_at
    }

    {:ok,
     state
     |> schedule_auth_expiry()
     |> schedule_catchup()
     |> schedule_drain()}
  end

  @spec handle_info(term(), state()) :: response()
  def handle_info(:drain_replay, state) do
    state = %{state | drain_scheduled: false}
    drain_replay(state)
  end

  def handle_info(:auth_expired, state), do: {:close, 4001, "token_expired", state}

  def handle_info({:cursor_update, update}, state) when is_map(update) do
    observe_cursor_update(update, state)
    handle_cursor_update(update, state)
  end

  def handle_info(:catchup_check, state) do
    state = %{state | catchup_timer_ref: nil}

    case EventStore.max_seq(state.session_id) do
      {:ok, max_seq} when max_seq > state.cursor ->
        next_state =
          state
          |> Map.put(:replay_done, false)
          |> schedule_catchup()
          |> schedule_drain()

        {:ok, next_state}

      _ ->
        {:ok, schedule_catchup(state)}
    end
  end

  def handle_info(_message, state), do: {:ok, state}

  defp handle_cursor_update(%{seq: seq} = update, state)
       when is_integer(seq) and seq > 0 do
    if seq <= state.cursor do
      {:ok, state}
    else
      if tail_idle?(state) do
        case resolve_live_cursor_update_event(state.session_id, state.principal, update) do
          {:ok, event} ->
            next_state = update_cursor_state(state, event)
            emit_events([event], next_state)

          :error ->
            {:ok, buffer_cursor_update(state, seq, update)}
        end
      else
        {:ok, buffer_cursor_update(state, seq, update)}
      end
    end
  end

  defp drain_replay(state) do
    case take_replay_events(state.replay_queue, state.frame_batch_size) do
      {events, replay_queue} when events != [] ->
        last_event = List.last(events)

        next_state =
          state
          |> Map.put(:replay_queue, replay_queue)
          |> update_cursor_state(last_event)
          |> maybe_schedule_drain()

        emit_events(events, next_state)

      {[], _queue} ->
        if state.replay_done do
          flush_buffered(state)
        else
          fetch_replay_batch(state)
        end
    end
  end

  defp fetch_replay_batch(state) do
    case measure_read(:tail_catchup, fn ->
           ReadPath.replay_from_cursor(
             state.session_id,
             Cursor.new(state.cursor_epoch, state.cursor),
             @replay_batch_size
           )
         end) do
      {:ok, []} ->
        state
        |> Map.put(:replay_done, true)
        |> flush_buffered()

      {:ok, events} ->
        next_state =
          state
          |> Map.put(:replay_queue, :queue.from_list(events))
          |> maybe_schedule_drain()

        {:ok, next_state}

      {:gap, gap} ->
        next_state =
          state
          |> Map.put(:cursor, gap.next_cursor.seq)
          |> Map.put(:cursor_epoch, gap.next_cursor.epoch)
          |> Map.put(:replay_done, false)
          |> maybe_schedule_drain()

        {:emit, {:gap, gap_payload(gap)}, next_state}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp flush_buffered(state) do
    {buffered_events, unresolved} =
      state.live_buffer
      |> Enum.sort_by(fn {seq, _value} -> seq end)
      |> Enum.reduce({[], %{}}, fn {seq, value}, {events, pending} ->
        cond do
          seq <= state.cursor ->
            {events, pending}

          true ->
            case resolve_buffered_value(state, value) do
              {:ok, event} -> {[event | events], pending}
              :error -> {events, Map.put(pending, seq, value)}
            end
        end
      end)

    buffered_events = Enum.reverse(buffered_events)

    if buffered_events == [] do
      {:ok, %{state | live_buffer: unresolved}}
    else
      next_state =
        state
        |> Map.put(:live_buffer, unresolved)
        |> Map.put(:replay_queue, :queue.from_list(buffered_events))
        |> maybe_schedule_drain()

      {:ok, next_state}
    end
  end

  defp resolve_buffered_value(
         %{session_id: session_id, principal: principal},
         {:cursor_update, %{seq: seq} = update}
       )
       when is_binary(session_id) and is_integer(seq) and seq > 0 and
              (is_struct(principal, Principal) or is_nil(principal)) do
    resolve_live_cursor_update_event(session_id, principal, update)
  end

  defp resolve_buffered_value(_state, _value), do: :error

  defp resolve_live_cursor_update_event(session_id, principal, update)
       when is_binary(session_id) and session_id != "" and is_map(update) and
              (is_struct(principal, Principal) or is_nil(principal)) do
    measure_read(:tail_live, fn ->
      resolve_cursor_update_event(session_id, principal, update)
    end)
  end

  defp resolve_cursor_update_event(session_id, principal, %{seq: seq} = update)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
              (is_struct(principal, Principal) or is_nil(principal)) do
    tenant_id = tail_tenant_id(update)

    case event_from_cursor_update(update, seq) do
      {:ok, event} ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :embedded, :hit)
        {:ok, event}

      :error ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :embedded, :miss)
        read_event_for_tail(session_id, principal, tenant_id, seq, Map.get(update, :epoch))
    end
  end

  defp event_from_cursor_update(%{event: %{seq: seq} = event} = update, seq)
       when is_integer(seq) and seq > 0 do
    {:ok, Map.put_new(event, :epoch, event_epoch(update, 0))}
  end

  defp event_from_cursor_update(_update, _seq), do: :error

  defp read_event_for_tail(session_id, principal, tenant_id, seq, epoch)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
              (is_struct(principal, Principal) or is_nil(principal)) do
    case EventStore.get_event(session_id, seq) do
      {:ok, event} ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :event_store, :hit)
        {:ok, put_event_epoch(event, epoch)}

      :error ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :event_store, :miss)
        read_event_from_storage(session_id, principal, tenant_id, seq, epoch)
    end
  end

  defp read_event_from_storage(session_id, principal, tenant_id, seq, epoch)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
              (is_struct(principal, Principal) or is_nil(principal)) do
    case ReadPath.get_events_from_cursor(session_id, seq - 1, 1) do
      {:ok, [%{seq: ^seq} = event]} ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :storage, :hit)
        {:ok, put_event_epoch(event, epoch)}

      _ ->
        Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :storage, :miss)
        :error
    end
  end

  defp observe_cursor_update(
         %{
           session_id: session_id,
           tenant_id: tenant_id,
           seq: seq,
           published_at_ms: published_at_ms
         },
         state
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(seq) and seq > 0 and is_integer(published_at_ms) and published_at_ms >= 0 do
    Telemetry.tail_visibility(
      session_id,
      tenant_id,
      seq,
      cursor_update_mode(seq, state),
      max(System.system_time(:millisecond) - published_at_ms, 0),
      :queue.len(state.replay_queue),
      map_size(state.live_buffer)
    )
  end

  defp observe_cursor_update(_update, _state), do: :ok

  defp cursor_update_mode(seq, state) when is_integer(seq) and seq > 0 do
    cond do
      seq <= state.cursor -> :stale
      tail_idle?(state) -> :live_fast_path
      true -> :buffered
    end
  end

  defp tail_tenant_id(%{tenant_id: tenant_id}) when is_binary(tenant_id) and tenant_id != "",
    do: tenant_id

  defp tail_tenant_id(_update), do: "unknown"

  defp maybe_schedule_drain(state) do
    queue_non_empty? = not :queue.is_empty(state.replay_queue)
    should_flush_buffer? = state.replay_done and map_size(state.live_buffer) > 0
    should_fetch_more_replay? = not state.replay_done and :queue.is_empty(state.replay_queue)

    cond do
      state.drain_scheduled ->
        state

      queue_non_empty? or should_flush_buffer? or should_fetch_more_replay? ->
        schedule_drain(state)

      true ->
        state
    end
  end

  defp schedule_drain(state) do
    send(self(), :drain_replay)
    %{state | drain_scheduled: true}
  end

  defp schedule_catchup(%{catchup_timer_ref: nil} = state) do
    ref = Process.send_after(self(), :catchup_check, @catchup_interval_ms)
    %{state | catchup_timer_ref: ref}
  end

  defp schedule_auth_expiry(%{auth_expires_at: expires_at} = state)
       when is_integer(expires_at) and expires_at > 0 do
    now_ms = System.system_time(:millisecond)
    expires_at_ms = expires_at * 1000

    if expires_at_ms <= now_ms do
      send(self(), :auth_expired)
      state
    else
      _ref = Process.send_after(self(), :auth_expired, expires_at_ms - now_ms)
      state
    end
  end

  defp schedule_auth_expiry(state), do: state

  defp auth_expires_at(%Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0,
       do: expires_at

  defp auth_expires_at(%Context{}), do: nil

  defp take_replay_events(queue, max_count)
       when is_integer(max_count) and max_count >= 1 do
    do_take_replay_events(queue, max_count, [])
  end

  defp do_take_replay_events(queue, 0, events), do: {Enum.reverse(events), queue}

  defp do_take_replay_events(queue, remaining, events) when remaining > 0 do
    case :queue.out(queue) do
      {{:value, event}, replay_queue} ->
        do_take_replay_events(replay_queue, remaining - 1, [event | events])

      {:empty, replay_queue} ->
        {Enum.reverse(events), replay_queue}
    end
  end

  defp emit_events(events, %{cursor_epoch: cursor_epoch} = state)
       when is_list(events) and events != [] do
    rendered_events = Enum.map(events, &render_event(&1, cursor_epoch))
    {:emit, {:events, rendered_events}, state}
  end

  defp gap_payload(gap) when is_map(gap) do
    %{
      type: "gap",
      reason: to_string(gap.reason),
      from_cursor: gap.from_cursor,
      next_cursor: gap.next_cursor,
      committed_cursor: gap.committed_cursor,
      earliest_available_cursor: gap.earliest_available_cursor
    }
  end

  defp render_event(event, fallback_epoch)
       when is_map(event) and (is_integer(fallback_epoch) or is_nil(fallback_epoch)) do
    epoch = event_epoch(event, fallback_epoch || 0)
    seq = Map.get(event, :seq)

    event
    |> Map.put_new(:epoch, epoch)
    |> Map.put_new(:cursor, Cursor.new(epoch, seq))
    |> Map.update(:inserted_at, nil, &iso8601_utc/1)
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp iso8601_utc(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp iso8601_utc(other), do: other

  defp measure_read(operation, fun)
       when operation in [:tail_catchup, :tail_live] and is_function(fun, 0) do
    started_at = System.monotonic_time()
    result = fun.()
    duration_ms = elapsed_ms_since(started_at)
    :ok = Telemetry.read(operation, :deliver, read_outcome(result), duration_ms)
    result
  end

  defp read_outcome({:ok, _result}), do: :ok
  defp read_outcome({:gap, _gap}), do: :ok
  defp read_outcome(_result), do: :error

  defp update_cursor_state(state, %{seq: seq} = event)
       when is_map(state) and is_integer(seq) and seq > 0 do
    %{
      state
      | cursor: max(state.cursor, seq),
        cursor_epoch: event_epoch(event, state.cursor_epoch)
    }
  end

  defp put_event_epoch(event, epoch)
       when is_map(event) and is_integer(epoch) and epoch >= 0 do
    Map.put_new(event, :epoch, epoch)
  end

  defp put_event_epoch(event, _epoch) when is_map(event), do: event

  defp tail_idle?(state) when is_map(state) do
    state.replay_done and :queue.is_empty(state.replay_queue) and map_size(state.live_buffer) == 0
  end

  defp buffer_cursor_update(state, seq, update)
       when is_map(state) and is_integer(seq) and seq > 0 and is_map(update) do
    state
    |> Map.put(:live_buffer, Map.put(state.live_buffer, seq, {:cursor_update, update}))
    |> maybe_schedule_drain()
  end

  defp event_epoch(value, fallback)
       when is_map(value) and (is_integer(fallback) or is_nil(fallback)) do
    case Map.get(value, :epoch) do
      epoch when is_integer(epoch) and epoch >= 0 -> epoch
      _ -> fallback
    end
  end

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
