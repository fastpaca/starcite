defmodule Starcite.Runtime.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores session metadata and applies ordered commands through Raft consensus.
  """

  @behaviour :ra_machine

  alias Starcite.Runtime.{CursorUpdate, EventStore}
  alias Starcite.Session

  defstruct [:group_id, :sessions]

  @impl true
  def init(%{group_id: group_id}) do
    %__MODULE__{group_id: group_id, sessions: %{}}
  end

  @impl true
  def apply(_meta, {:create_session, session_id, title, metadata}, state) do
    case Map.get(state.sessions, session_id) do
      nil ->
        session = Session.new(session_id, title: title, metadata: metadata)
        new_state = %{state | sessions: Map.put(state.sessions, session_id, session)}
        {new_state, {:reply, {:ok, Session.to_map(session)}}}

      %Session{} ->
        {state, {:reply, {:error, :session_exists}}}
    end
  end

  @impl true
  def apply(_meta, {:append_event, session_id, input, opts}, state) do
    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <- guard_expected_seq(session, opts[:expected_seq]) do
      case Session.append_event(session, input) do
        {:appended, updated_session, event} ->
          case EventStore.put_event(session_id, event) do
            :ok ->
              new_state = %{
                state
                | sessions: Map.put(state.sessions, session_id, updated_session)
              }

              Starcite.Observability.Telemetry.event_appended(
                session_id,
                event.type,
                event.actor,
                event.source,
                payload_bytes(event.payload)
              )

              reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
              effects = build_effects(session_id, event, updated_session.last_seq)
              {new_state, {:reply, {:ok, reply}}, effects}

            {:error, :event_store_backpressure} ->
              {state, {:reply, {:error, :event_store_backpressure}}}
          end

        {:deduped, _session, seq} ->
          reply = %{seq: seq, last_seq: session.last_seq, deduped: true}
          {state, {:reply, {:ok, reply}}}

        {:error, :idempotency_conflict} ->
          {state, {:reply, {:error, :idempotency_conflict}}}
      end
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  @impl true
  def apply(_meta, :force_snapshot, state) do
    {state, {:reply, :ok}}
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
  def query_session(state, session_id) do
    with {:ok, session} <- fetch_session(state.sessions, session_id) do
      {:ok, session}
    else
      _ -> {:error, :session_not_found}
    end
  end

  # Helpers

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

  defp build_effects(session_id, event, last_seq) do
    cursor_update =
      {
        :mod_call,
        Phoenix.PubSub,
        :broadcast,
        [
          Starcite.PubSub,
          CursorUpdate.topic(session_id),
          CursorUpdate.message(session_id, event, last_seq)
        ]
      }

    [cursor_update]
  end

  defp payload_bytes(payload), do: :erlang.external_size(payload)
end
