defmodule StarciteWeb.TailChannel do
  @moduledoc false

  use StarciteWeb, :channel

  alias Phoenix.Socket
  alias Starcite.DataPlane.{EventStore, TailBroadcast}
  alias Starcite.Observability.Telemetry
  alias Starcite.{ReadPath, Session}
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy
  alias StarciteWeb.{ErrorInfo, SessionAppend}

  @default_tail_frame_batch_size 1
  @max_tail_frame_batch_size 1_000
  @replay_batch_size 1_000
  @catchup_interval_ms 5_000
  @tail_state_assign :tail_channel_state
  @tail_event TailBroadcast.event_name()

  intercept [@tail_event]

  @impl true
  def join(
        "tail:" <> session_id,
        params,
        %Socket{assigns: %{auth_context: %Context{} = auth_context}} = socket
      )
      when is_binary(session_id) and session_id != "" and is_map(params) do
    with {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}} <- parse_tail_params(params),
         {:ok, _session} <- authorize_read(auth_context, session_id) do
      state =
        %{
          session_id: session_id,
          cursor: cursor,
          frame_batch_size: frame_batch_size,
          replay_queue: :queue.new(),
          replay_done: false,
          live_buffer: %{},
          drain_scheduled: false
        }
        |> schedule_auth_expiry(auth_context)
        |> schedule_catchup()
        |> schedule_drain()

      {:ok, assign(socket, @tail_state_assign, state)}
    else
      {:error, reason} ->
        {:error, %{reason: to_string(reason)}}
    end
  end

  def join("tail:" <> _session_id, _params, _socket),
    do: {:error, %{reason: "invalid_session_id"}}

  def join(_topic, _params, _socket), do: {:error, %{reason: "invalid_session_id"}}

  @impl true
  def handle_in("append", payload, %Socket{} = socket) when is_map(payload) do
    state = tail_state(socket)
    auth_context = socket.assigns.auth_context

    case SessionAppend.append(auth_context, state.session_id, payload) do
      {:ok, reply} ->
        {:reply, {:ok, reply}, socket}

      error ->
        {:reply, {:error, ErrorInfo.payload(error)}, socket}
    end
  end

  def handle_in(_event, _payload, socket) do
    {:reply, {:error, ErrorInfo.payload({:error, :invalid_event})}, socket}
  end

  @impl true
  def handle_info(:drain_replay, %Socket{} = socket) do
    state = %{tail_state(socket) | drain_scheduled: false}
    drain_replay(socket, state)
  end

  def handle_info(:auth_expired, %Socket{} = socket) do
    socket.endpoint.broadcast(socket.assigns.socket_id, "disconnect", %{})
    {:stop, :token_expired, socket}
  end

  def handle_info(:catchup_check, %Socket{} = socket) do
    state = tail_state(socket)

    case EventStore.max_seq(state.session_id) do
      {:ok, max_seq} when max_seq > state.cursor ->
        next_state =
          state
          |> Map.put(:replay_done, false)
          |> schedule_catchup()
          |> schedule_drain()

        {:noreply, put_tail_state(socket, next_state)}

      _ ->
        {:noreply, put_tail_state(socket, schedule_catchup(state))}
    end
  end

  def handle_info(_message, socket), do: {:noreply, socket}

  @impl true
  def handle_out(@tail_event, %{seq: seq} = payload, %Socket{} = socket)
      when is_integer(seq) and seq > 0 do
    state = tail_state(socket)

    if seq <= state.cursor do
      {:noreply, socket}
    else
      queue_empty? = :queue.is_empty(state.replay_queue)
      buffer_empty? = map_size(state.live_buffer) == 0

      if state.replay_done and queue_empty? and buffer_empty? do
        case resolve_live_event(state.session_id, payload) do
          {:ok, event} ->
            next_socket = put_tail_state(socket, %{state | cursor: event.seq})
            push_events(next_socket, [event])

          :error ->
            {:noreply, put_tail_state(socket, buffer_live_payload(state, payload))}
        end
      else
        {:noreply, put_tail_state(socket, buffer_live_payload(state, payload))}
      end
    end
  end

  def handle_out(@tail_event, payload, _socket) do
    raise ArgumentError, "invalid tail_event payload: #{inspect(payload)}"
  end

  def handle_out(_event, _payload, socket), do: {:noreply, socket}

  defp drain_replay(socket, state) do
    case take_replay_events(state.replay_queue, state.frame_batch_size) do
      {events, replay_queue} when events != [] ->
        last_event = List.last(events)

        next_state =
          state
          |> Map.put(:replay_queue, replay_queue)
          |> Map.put(:cursor, max(state.cursor, last_event.seq))
          |> maybe_schedule_drain()

        socket
        |> put_tail_state(next_state)
        |> push_events(events)

      {[], _queue} ->
        if state.replay_done do
          flush_buffered(socket, state)
        else
          fetch_replay_batch(socket, state)
        end
    end
  end

  defp fetch_replay_batch(socket, state) do
    case measure_read(:tail_catchup, fn ->
           ReadPath.get_events_from_cursor(state.session_id, state.cursor, @replay_batch_size)
         end) do
      {:ok, []} ->
        flush_buffered(socket, %{state | replay_done: true})

      {:ok, events} ->
        next_state =
          state
          |> Map.put(:replay_queue, :queue.from_list(events))
          |> maybe_schedule_drain()

        {:noreply, put_tail_state(socket, next_state)}

      {:error, reason} ->
        {:stop, reason, socket}
    end
  end

  defp flush_buffered(socket, state) do
    {buffered_events, unresolved} =
      state.live_buffer
      |> Enum.sort_by(fn {item_seq, _payload} -> item_seq end)
      |> Enum.reduce({[], %{}}, fn {item_seq, payload}, {events, pending} ->
        cond do
          item_seq <= state.cursor ->
            {events, pending}

          true ->
            case resolve_live_event(state.session_id, payload) do
              {:ok, event} -> {[event | events], pending}
              :error -> {events, Map.put(pending, item_seq, payload)}
            end
        end
      end)

    buffered_events = Enum.reverse(buffered_events)

    if buffered_events == [] do
      {:noreply, put_tail_state(socket, %{state | live_buffer: unresolved})}
    else
      next_state =
        state
        |> Map.put(:live_buffer, unresolved)
        |> Map.put(:replay_queue, :queue.from_list(buffered_events))
        |> maybe_schedule_drain()

      {:noreply, put_tail_state(socket, next_state)}
    end
  end

  defp resolve_live_event(_session_id, %{seq: seq, event: %{seq: seq} = event})
       when is_integer(seq) and seq > 0 do
    measure_read(:tail_live, fn -> {:ok, event} end)
  end

  defp resolve_live_event(session_id, %{seq: seq}) when is_integer(seq) and seq > 0 do
    measure_read(:tail_live, fn -> read_event_for_tail(session_id, seq) end)
  end

  defp read_event_for_tail(session_id, seq) when is_integer(seq) and seq > 0 do
    case EventStore.get_event(session_id, seq) do
      {:ok, event} ->
        {:ok, event}

      :error ->
        case ReadPath.get_events_from_cursor(session_id, seq - 1, 1) do
          {:ok, [%{seq: ^seq} = event]} -> {:ok, event}
          _ -> :error
        end
    end
  end

  defp buffer_live_payload(state, %{seq: seq} = payload)
       when is_integer(seq) and seq > 0 do
    buffered = Map.put(state.live_buffer, seq, payload)
    maybe_schedule_drain(%{state | live_buffer: buffered})
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

  defp schedule_catchup(state) do
    Process.send_after(self(), :catchup_check, @catchup_interval_ms)
    state
  end

  defp schedule_auth_expiry(state, %Context{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0 do
    now_ms = System.system_time(:millisecond)
    expires_at_ms = expires_at * 1000

    if expires_at_ms <= now_ms do
      send(self(), :auth_expired)
      state
    else
      Process.send_after(self(), :auth_expired, expires_at_ms - now_ms)
      state
    end
  end

  defp schedule_auth_expiry(state, %Context{}), do: state

  defp take_replay_events(queue, max_count) when is_integer(max_count) and max_count > 0 do
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

  defp push_events(%Socket{} = socket, events) when is_list(events) and events != [] do
    push(socket, "events", %{events: Enum.map(events, &render_event/1)})
    {:noreply, socket}
  end

  defp render_event(%{inserted_at: inserted_at} = event) do
    %{event | inserted_at: iso8601_utc(inserted_at)}
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp iso8601_utc(datetime) when is_binary(datetime), do: datetime
  defp iso8601_utc(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp measure_read(operation, fun)
       when operation in [:tail_catchup, :tail_live] and is_function(fun, 0) do
    started_at = System.monotonic_time()
    result = fun.()
    duration_ms = elapsed_ms_since(started_at)
    :ok = Telemetry.read(operation, :deliver, read_outcome(result), duration_ms)
    result
  end

  defp read_outcome({:ok, _result}), do: :ok
  defp read_outcome(_result), do: :error

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end

  defp tail_state(%Socket{assigns: assigns}) when is_map(assigns) do
    Map.fetch!(assigns, @tail_state_assign)
  end

  defp put_tail_state(%Socket{} = socket, state), do: assign(socket, @tail_state_assign, state)

  defp authorize_read(%Context{} = auth, session_id)
       when is_binary(session_id) and session_id != "" do
    with :ok <- Policy.allowed_to_access_session(auth, session_id),
         {:ok, %Session{} = session} <- ReadPath.get_session(session_id),
         :ok <- Policy.allowed_to_read_session(auth, session) do
      {:ok, session}
    end
  end

  defp authorize_read(_auth, _session_id), do: {:error, :invalid_session_id}

  defp parse_tail_params(params) when is_map(params) do
    with {:ok, cursor} <- parse_cursor(Map.get(params, "cursor", 0)),
         {:ok, frame_batch_size} <-
           parse_frame_batch_size(Map.get(params, "batch_size", @default_tail_frame_batch_size)) do
      {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}}
    end
  end

  defp parse_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: {:ok, cursor}

  defp parse_cursor(cursor) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_cursor}
    end
  end

  defp parse_cursor(_cursor), do: {:error, :invalid_cursor}

  defp parse_frame_batch_size(batch_size)
       when is_integer(batch_size) and batch_size >= @default_tail_frame_batch_size and
              batch_size <= @max_tail_frame_batch_size,
       do: {:ok, batch_size}

  defp parse_frame_batch_size(batch_size) when is_binary(batch_size) do
    case Integer.parse(batch_size) do
      {parsed, ""}
      when parsed >= @default_tail_frame_batch_size and parsed <= @max_tail_frame_batch_size ->
        {:ok, parsed}

      _ ->
        {:error, :invalid_tail_batch_size}
    end
  end

  defp parse_frame_batch_size(_batch_size), do: {:error, :invalid_tail_batch_size}
end
