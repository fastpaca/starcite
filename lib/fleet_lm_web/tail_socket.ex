defmodule FleetLMWeb.TailSocket do
  @moduledoc """
  Raw WebSocket handler for session tails.

  Emits one JSON event per WebSocket text frame.
  """

  @behaviour WebSock

  alias FleetLM.Runtime
  alias Phoenix.PubSub

  @replay_batch_size 1_000

  @impl true
  def init(%{session_id: session_id, cursor: cursor}) do
    topic = "session:#{session_id}"
    :ok = PubSub.subscribe(FleetLM.PubSub, topic)

    state = %{
      session_id: session_id,
      topic: topic,
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

  def handle_info(_message, state), do: {:ok, state}

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
    case Runtime.get_events_from_cursor(state.session_id, state.cursor, @replay_batch_size) do
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
    buffered_events =
      state.live_buffer
      |> Map.values()
      |> Enum.filter(&(&1.seq > state.cursor))
      |> Enum.sort_by(& &1.seq)

    if buffered_events == [] do
      {:ok, %{state | live_buffer: %{}}}
    else
      next_state =
        state
        |> Map.put(:live_buffer, %{})
        |> Map.put(:replay_queue, :queue.from_list(buffered_events))
        |> maybe_schedule_drain()

      {:ok, next_state}
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
