defmodule StarciteWeb.TailSocket do
  @moduledoc """
  Raw WebSocket handler for session tails.

  Emits one JSON event per WebSocket text frame.
  """

  @behaviour WebSock

  alias Starcite.Runtime
  alias Starcite.Runtime.{CursorUpdate, PayloadStore}
  alias Phoenix.PubSub

  @replay_batch_size 1_000

  @impl true
  def init(%{session_id: session_id, cursor: cursor}) do
    tail_source = tail_source_mode()
    topic = topic_for(session_id, tail_source)
    :ok = PubSub.subscribe(Starcite.PubSub, topic)

    state = %{
      session_id: session_id,
      topic: topic,
      tail_source: tail_source,
      cursor: cursor,
      replay_queue: :queue.new(),
      replay_done: false,
      live_buffer: %{},
      drain_scheduled: false
    }

    {:ok, schedule_drain(state)}
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

  def handle_info({:event, event}, state) do
    handle_live_event(event, state)
  end

  def handle_info({:cursor_update, update}, state) when is_map(update) do
    handle_cursor_update(update, state)
  end

  def handle_info(_message, state), do: {:ok, state}

  defp handle_live_event(event, state) do
    if event.seq <= state.cursor do
      {:ok, state}
    else
      queue_empty? = :queue.is_empty(state.replay_queue)
      buffer_empty? = map_size(state.live_buffer) == 0

      if state.replay_done and queue_empty? and buffer_empty? do
        next_state = %{state | cursor: event.seq}
        {:push, {:text, Jason.encode!(render_event(event))}, next_state}
      else
        buffered = Map.put(state.live_buffer, event.seq, event)
        next_state = %{state | live_buffer: buffered}
        {:ok, maybe_schedule_drain(next_state)}
      end
    end
  end

  defp handle_cursor_update(%{seq: seq} = update, %{tail_source: :ets} = state)
       when is_integer(seq) and seq > 0 do
    if seq <= state.cursor do
      {:ok, state}
    else
      queue_empty? = :queue.is_empty(state.replay_queue)
      buffer_empty? = map_size(state.live_buffer) == 0

      if state.replay_done and queue_empty? and buffer_empty? do
        case PayloadStore.get_event(state.session_id, seq) do
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

  defp handle_cursor_update(_update, state), do: {:ok, state}

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
    case replay_batch(state) do
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

  defp replay_batch(%{tail_source: :legacy, session_id: session_id, cursor: cursor}) do
    Runtime.get_events_from_cursor(session_id, cursor, @replay_batch_size)
  end

  defp replay_batch(%{tail_source: :ets, session_id: session_id, cursor: cursor}) do
    {:ok, PayloadStore.from_cursor(session_id, cursor, @replay_batch_size)}
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

  defp resolve_buffered_value(_state, %{seq: seq} = event) when is_integer(seq) and seq > 0 do
    {:ok, event}
  end

  defp resolve_buffered_value(
         %{tail_source: :ets, session_id: session_id},
         {:cursor_update, %{seq: seq}}
       )
       when is_integer(seq) and seq > 0 do
    PayloadStore.get_event(session_id, seq)
  end

  defp resolve_buffered_value(_state, _value), do: :error

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

  defp topic_for(session_id, :legacy), do: "session:#{session_id}"
  defp topic_for(session_id, :ets), do: CursorUpdate.topic(session_id)

  defp tail_source_mode do
    case Application.get_env(:starcite, :tail_source, :legacy) do
      :legacy -> :legacy
      :ets -> :ets
      "legacy" -> :legacy
      "ets" -> :ets
      other -> raise ArgumentError, "invalid :starcite, :tail_source value: #{inspect(other)}"
    end
  end

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
