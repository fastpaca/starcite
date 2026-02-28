defmodule StarciteWeb.TailSocket do
  @moduledoc """
  Raw WebSocket handler for session tails.

  Emits one JSON event per WebSocket text frame.
  """

  @behaviour WebSock

  alias Starcite.Observability.Telemetry
  alias Starcite.ReadPath
  alias Starcite.DataPlane.{CursorUpdate, EventStore}
  alias StarciteWeb.Auth.Context
  alias Phoenix.PubSub

  @replay_batch_size 1_000
  @catchup_interval_ms 5_000

  @impl true
  def init(%{
        session_id: session_id,
        cursor: cursor,
        tenant_id: tenant_id,
        auth_context: %Context{} = auth_context
      })
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_binary(tenant_id) and tenant_id != "" do
    topic = CursorUpdate.topic(session_id)
    :ok = PubSub.subscribe(Starcite.PubSub, topic)
    auth_expires_at = auth_expires_at(auth_context)

    state = %{
      session_id: session_id,
      topic: topic,
      tenant_id: tenant_id,
      auth_context: auth_context,
      cursor: cursor,
      replay_queue: :queue.new(),
      replay_done: false,
      live_buffer: %{},
      drain_scheduled: false,
      catchup_timer_ref: nil,
      auth_expires_at: auth_expires_at,
      auth_expiry_timer_ref: nil
    }

    {:ok,
     state
     |> schedule_auth_expiry()
     |> schedule_catchup()
     |> schedule_drain()}
  end

  @impl true
  def handle_in({_payload, opcode: _opcode}, state) do
    # Tail socket is server->client only; inbound frames are ignored.
    {:ok, state}
  end

  @impl true
  def handle_info(:drain_replay, state) do
    state = %{state | drain_scheduled: false}
    drain_replay(state)
  end

  def handle_info(:auth_expired, state), do: close_for_auth_error(:token_expired, state)

  def handle_info({:cursor_update, update}, state) when is_map(update) do
    handle_cursor_update(update, state)
  end

  def handle_info(:catchup_check, state) do
    state = %{state | catchup_timer_ref: nil}

    case EventStore.max_seq(state.session_id) do
      {:ok, max_seq} when max_seq > state.cursor ->
        # Events exist beyond our cursor that we missed (likely a PubSub gap
        # during Raft leadership transition). Re-enter replay to catch up.
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

  @impl true
  def terminate(_reason, _state), do: :ok

  defp handle_cursor_update(%{seq: seq} = update, state)
       when is_integer(seq) and seq > 0 do
    if seq <= state.cursor do
      {:ok, state}
    else
      queue_empty? = :queue.is_empty(state.replay_queue)
      buffer_empty? = map_size(state.live_buffer) == 0

      if state.replay_done and queue_empty? and buffer_empty? do
        case resolve_cursor_update_event(state.session_id, state.tenant_id, update) do
          {:ok, event} ->
            next_state = %{state | cursor: event.seq}
            {:push, {:text, Jason.encode!(render_event(event))}, next_state}

          :error ->
            buffered = Map.put(state.live_buffer, seq, {:cursor_update, update})
            next_state = %{state | live_buffer: buffered}
            {:ok, maybe_schedule_drain(next_state)}
        end
      else
        buffered = Map.put(state.live_buffer, seq, {:cursor_update, update})
        next_state = %{state | live_buffer: buffered}
        {:ok, maybe_schedule_drain(next_state)}
      end
    end
  end

  defp drain_replay(state) do
    case :queue.out(state.replay_queue) do
      {{:value, event}, replay_queue} ->
        next_state =
          state
          |> Map.put(:replay_queue, replay_queue)
          |> Map.put(:cursor, max(state.cursor, event.seq))
          |> maybe_schedule_drain()

        {:push, {:text, Jason.encode!(render_event(event))}, next_state}

      {:empty, _queue} ->
        if state.replay_done do
          flush_buffered(state)
        else
          fetch_replay_batch(state)
        end
    end
  end

  defp fetch_replay_batch(state) do
    case ReadPath.get_events_from_cursor(
           state.session_id,
           state.cursor,
           @replay_batch_size,
           tenant_id: state.tenant_id
         ) do
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

      {:error, _reason} ->
        {:stop, :normal, state}
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
         %{session_id: session_id, tenant_id: tenant_id},
         {:cursor_update, %{seq: seq} = update}
       )
       when is_binary(session_id) and is_binary(tenant_id) and is_integer(seq) and seq > 0 do
    resolve_cursor_update_event(session_id, tenant_id, update)
  end

  defp resolve_buffered_value(_state, _value), do: :error

  defp resolve_cursor_update_event(session_id, tenant_id, %{seq: seq} = update)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(seq) and seq > 0 do
    case event_from_cursor_update(update, seq) do
      {:ok, event} ->
        {:ok, event}

      :error ->
        read_event_for_tail(session_id, tenant_id, seq)
    end
  end

  defp event_from_cursor_update(%{event: %{seq: seq} = event}, seq)
       when is_integer(seq) and seq > 0 do
    {:ok, event}
  end

  defp event_from_cursor_update(_update, _seq), do: :error

  defp read_event_for_tail(session_id, tenant_id, seq)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(seq) and seq > 0 do
    case EventStore.get_event(session_id, seq) do
      {:ok, event} ->
        :ok = Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :ets, :hit)
        {:ok, event}

      :error ->
        :ok = Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :ets, :miss)
        read_event_from_storage(session_id, tenant_id, seq)
    end
  end

  defp read_event_from_storage(session_id, tenant_id, seq)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(seq) and seq > 0 do
    case ReadPath.get_events_from_cursor(session_id, seq - 1, 1, tenant_id: tenant_id) do
      {:ok, [%{seq: ^seq} = event]} ->
        :ok = Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :storage, :hit)
        {:ok, event}

      _ ->
        :ok = Telemetry.tail_cursor_lookup(session_id, tenant_id, seq, :storage, :miss)
        :error
    end
  end

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
      ref = Process.send_after(self(), :auth_expired, expires_at_ms - now_ms)
      %{state | auth_expiry_timer_ref: ref}
    end
  end

  defp close_for_auth_error(:token_expired, state) do
    {:stop, :token_expired, {4001, "token_expired"}, state}
  end

  defp auth_expires_at(%Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0,
       do: expires_at

  defp auth_expires_at(%Context{}), do: nil

  defp render_event(event) when is_map(event) do
    Map.update(event, :inserted_at, nil, &iso8601_utc/1)
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp iso8601_utc(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp iso8601_utc(other), do: other
end
