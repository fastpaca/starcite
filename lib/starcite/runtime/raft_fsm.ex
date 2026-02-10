defmodule Starcite.Runtime.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores session metadata and applies ordered commands through Raft consensus.
  """

  @behaviour :ra_machine

  alias Starcite.Runtime.{CursorUpdate, EventStore}
  alias Starcite.Session

  @num_lanes 16

  defmodule Lane do
    @moduledoc false
    defstruct sessions: %{}
  end

  defstruct [:group_id, :lanes]

  @impl true
  def init(%{group_id: group_id}) do
    lanes = for lane <- 0..(@num_lanes - 1), into: %{}, do: {lane, %Lane{}}
    %__MODULE__{group_id: group_id, lanes: lanes}
  end

  @impl true
  def apply(_meta, {:create_session, lane_id, session_id, title, metadata}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    case Map.get(lane.sessions, session_id) do
      nil ->
        session = Session.new(session_id, title: title, metadata: metadata)
        new_lane = %{lane | sessions: Map.put(lane.sessions, session_id, session)}
        new_state = put_in(state.lanes[lane_id], new_lane)
        {new_state, {:reply, {:ok, Session.to_map(session)}}}

      %Session{} ->
        {state, {:reply, {:error, :session_exists}}}
    end
  end

  @impl true
  def apply(_meta, {:append_event, lane_id, session_id, input, opts}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, session} <- fetch_session(lane, session_id),
         :ok <- guard_expected_seq(session, opts[:expected_seq]) do
      case Session.append_event(session, input) do
        {:appended, updated_session, event} ->
          :ok = EventStore.put_event(session_id, event)

          new_lane = %{lane | sessions: Map.put(lane.sessions, session_id, updated_session)}
          new_state = put_in(state.lanes[lane_id], new_lane)

          Starcite.Observability.Telemetry.event_appended(
            session_id,
            event.type,
            event.actor,
            event.source,
            byte_size(Jason.encode!(event.payload))
          )

          Starcite.Observability.Telemetry.cursor_update_emitted(
            session_id,
            event.seq,
            updated_session.last_seq
          )

          reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
          effects = build_effects(session_id, event, updated_session.last_seq)
          {new_state, {:reply, {:ok, reply}}, effects}

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
  def apply(meta, {:ack_archived, lane_id, session_id, upto_seq}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, session} <- fetch_session(lane, session_id) do
      previous_archived_seq = session.archived_seq
      {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)
      new_lane = %{lane | sessions: Map.put(lane.sessions, session_id, updated_session)}
      new_state = put_in(state.lanes[lane_id], new_lane)

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

  # ---------------------------------------------------------------------------
  # Queries
  # ---------------------------------------------------------------------------

  @doc """
  Query one session by ID.
  """
  def query_session(state, lane_id, session_id) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, session} <- fetch_session(lane, session_id) do
      {:ok, session}
    else
      _ -> {:error, :session_not_found}
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp fetch_session(%Lane{sessions: sessions}, session_id) do
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
    stream_event =
      {
        :mod_call,
        Phoenix.PubSub,
        :broadcast,
        [
          Starcite.PubSub,
          "session:#{session_id}",
          {:event, event}
        ]
      }

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

    [stream_event, cursor_update]
  end
end
