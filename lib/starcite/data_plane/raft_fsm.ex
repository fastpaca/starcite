defmodule Starcite.DataPlane.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores session state (including auth-critical fields) and applies ordered commands through Raft consensus.
  """

  @behaviour :ra_machine

  alias Starcite.DataPlane.{CursorUpdate, EventStore}
  alias Starcite.Session
  alias Starcite.Session.ProducerIndex

  @emit_event_append_telemetry Application.compile_env(
                                 :starcite,
                                 :emit_event_append_telemetry,
                                 false
                               )

  defstruct [:group_id, :sessions]

  @type session_state :: %Session{producer_cursors: ProducerIndex.t()}

  @type t :: %__MODULE__{
          group_id: term(),
          sessions: %{optional(String.t()) => session_state()}
        }

  @impl true
  def init(%{group_id: group_id}) do
    %__MODULE__{group_id: group_id, sessions: %{}}
  end

  @impl true
  def apply(
        _meta,
        {:create_session, session_id, title, creator_principal, metadata},
        state
      ) do
    case Map.get(state.sessions, session_id) do
      nil ->
        session =
          Session.new(session_id,
            title: title,
            creator_principal: creator_principal,
            metadata: metadata
          )

        new_state = %{state | sessions: Map.put(state.sessions, session_id, session)}
        {new_state, {:reply, {:ok, Session.to_map(session)}}}

      %Session{} ->
        {state, {:reply, {:error, :session_exists}}}
    end
  end

  @impl true
  def apply(_meta, {:append_event, session_id, input, expected_seq}, state) do
    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <- guard_expected_seq(session, expected_seq),
         {:ok, updated_session, reply, event_to_store} <- append_one_to_session(session, input),
         :ok <- put_appended_event(session_id, event_to_store) do
      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}
      emit_appended_event_telemetry(session_id, event_to_store)
      effect = build_effect_for_event(session_id, event_to_store)
      reply = {:reply, {:ok, reply}}

      case effect do
        nil -> {new_state, reply}
        value -> {new_state, reply, [value]}
      end
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  @impl true
  def apply(_meta, {:append_events, _session_id, [], _opts}, state) do
    {state, {:reply, {:error, :invalid_event}}}
  end

  @impl true
  def apply(_meta, {:append_events, session_id, inputs, opts}, state)
      when is_list(inputs) and (is_list(opts) or is_nil(opts)) do
    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <- guard_expected_seq(session, expected_seq_from_opts(opts)),
         {:ok, updated_session, replies, events_to_store} <- append_to_session(session, inputs),
         :ok <- put_appended_events(session_id, events_to_store) do
      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}
      emit_appended_telemetry(session_id, events_to_store)
      effects = build_effects_for_events(session_id, events_to_store)
      reply = {:reply, {:ok, %{results: replies, last_seq: updated_session.last_seq}}}

      case effects do
        [] -> {new_state, reply}
        _ -> {new_state, reply, effects}
      end
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  @impl true
  def apply(meta, {:ack_archived, session_id, upto_seq}, state) do
    with {:ok, session} <- fetch_session(state.sessions, session_id) do
      previous_archived_seq = session.archived_seq
      {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)
      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}

      evicted =
        evict_archived_events(session_id, previous_archived_seq, updated_session.archived_seq)

      tail_size = Session.tail_size(updated_session)

      Starcite.Observability.Telemetry.archive_ack_applied(
        session_id,
        updated_session.last_seq,
        updated_session.archived_seq,
        evicted,
        updated_session.retention.tail_keep,
        tail_size
      )

      reply = {:reply, {:ok, %{archived_seq: updated_session.archived_seq, trimmed: evicted}}}

      case release_cursor_effect(
             meta,
             previous_archived_seq,
             updated_session.archived_seq,
             new_state
           ) do
        nil -> {new_state, reply}
        effect -> {new_state, reply, [effect]}
      end
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  # Queries

  @doc """
  Query one session by ID.
  """
  @spec query_session(t(), String.t()) :: {:ok, session_state()} | {:error, :session_not_found}
  def query_session(state, session_id) do
    with {:ok, session} <- fetch_session(state.sessions, session_id) do
      {:ok, session}
    else
      _ -> {:error, :session_not_found}
    end
  end

  # Helpers

  @spec fetch_session(%{optional(String.t()) => session_state()}, String.t()) ::
          {:ok, session_state()} | {:error, :session_not_found}
  defp fetch_session(sessions, session_id) when is_map(sessions) do
    case Map.get(sessions, session_id) do
      nil -> {:error, :session_not_found}
      session -> {:ok, session}
    end
  end

  defp guard_expected_seq(_session, nil), do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0 and last_seq == expected_seq,
       do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0,
       do: {:error, {:expected_seq_conflict, expected_seq, last_seq}}

  defp expected_seq_from_opts(nil), do: nil
  defp expected_seq_from_opts(opts) when is_list(opts), do: opts[:expected_seq]

  defp evict_archived_events(_session_id, previous_archived_seq, updated_archived_seq)
       when updated_archived_seq <= previous_archived_seq do
    0
  end

  defp evict_archived_events(session_id, _previous_archived_seq, updated_archived_seq) do
    EventStore.delete_below(session_id, updated_archived_seq + 1)
  end

  defp release_cursor_effect(
         %{index: raft_index},
         previous_archived_seq,
         updated_archived_seq,
         %__MODULE__{} = state
       )
       when is_integer(raft_index) and raft_index > 0 and
              updated_archived_seq > previous_archived_seq do
    {:release_cursor, raft_index, state}
  end

  defp release_cursor_effect(_meta, _previous_archived_seq, _updated_archived_seq, _state),
    do: nil

  defp append_to_session(%Session{} = session, inputs)
       when is_list(inputs) and inputs != [] do
    do_append_to_session(session, inputs, [], [])
  end

  defp append_to_session(%Session{}, []), do: {:error, :invalid_event}

  defp append_one_to_session(%Session{} = session, input) when is_map(input) do
    case Session.append_event(session, input) do
      {:appended, updated_session, event} ->
        reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
        {:ok, updated_session, reply, event}

      {:deduped, updated_session, seq} ->
        reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
        {:ok, updated_session, reply, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp append_one_to_session(%Session{}, _input), do: {:error, :invalid_event}

  defp do_append_to_session(%Session{} = session, [input | rest], replies, events) do
    case Session.append_event(session, input) do
      {:appended, updated_session, event} ->
        reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
        do_append_to_session(updated_session, rest, [reply | replies], [event | events])

      {:deduped, updated_session, seq} ->
        reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
        do_append_to_session(updated_session, rest, [reply | replies], events)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_append_to_session(%Session{} = session, [], replies, events) do
    {:ok, session, Enum.reverse(replies), Enum.reverse(events)}
  end

  defp put_appended_events(_session_id, []), do: :ok

  defp put_appended_events(session_id, events) when is_binary(session_id) and is_list(events) do
    EventStore.put_events(session_id, events)
  end

  defp put_appended_event(_session_id, nil), do: :ok

  defp put_appended_event(session_id, %{seq: seq} = event)
       when is_binary(session_id) and is_integer(seq) and seq > 0 do
    EventStore.put_event(session_id, event)
  end

  if @emit_event_append_telemetry do
    defp emit_appended_telemetry(_session_id, []), do: :ok

    defp emit_appended_telemetry(session_id, events)
         when is_binary(session_id) and is_list(events) do
      Enum.each(events, fn %{type: type, actor: actor, payload: payload} = event ->
        Starcite.Observability.Telemetry.event_appended(
          session_id,
          type,
          actor,
          Map.get(event, :source),
          payload_bytes(payload)
        )
      end)

      :ok
    end

    defp emit_appended_event_telemetry(_session_id, nil), do: :ok

    defp emit_appended_event_telemetry(
           session_id,
           %{type: type, actor: actor, payload: payload} = event
         )
         when is_binary(session_id) and is_binary(type) and is_binary(actor) do
      Starcite.Observability.Telemetry.event_appended(
        session_id,
        type,
        actor,
        Map.get(event, :source),
        payload_bytes(payload)
      )

      :ok
    end
  else
    defp emit_appended_telemetry(_session_id, _events), do: :ok
    defp emit_appended_event_telemetry(_session_id, _event), do: :ok
  end

  defp build_effects_for_events(_session_id, []), do: []

  defp build_effects_for_events(session_id, events)
       when is_binary(session_id) and is_list(events) do
    Enum.map(events, fn %{seq: seq} = event ->
      {
        :mod_call,
        Phoenix.PubSub,
        :broadcast,
        [
          Starcite.PubSub,
          CursorUpdate.topic(session_id),
          CursorUpdate.message(session_id, event, seq)
        ]
      }
    end)
  end

  defp build_effect_for_event(_session_id, nil), do: nil

  defp build_effect_for_event(session_id, %{seq: seq} = event)
       when is_binary(session_id) and is_integer(seq) and seq > 0 do
    {
      :mod_call,
      Phoenix.PubSub,
      :broadcast,
      [
        Starcite.PubSub,
        CursorUpdate.topic(session_id),
        CursorUpdate.message(session_id, event, seq)
      ]
    }
  end

  if @emit_event_append_telemetry do
    defp payload_bytes(payload), do: :erlang.external_size(payload)
  end
end
