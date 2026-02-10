defmodule Starcite.Runtime.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores append-only session event logs and supports cursor-based replay.
  """

  @behaviour :ra_machine

  alias Starcite.Session
  alias Starcite.Session.EventLog

  @num_lanes 16

  defmodule Lane do
    @moduledoc false
    defstruct sessions: %{}
  end

  defstruct [:group_id, :lanes, :max_unarchived_events]

  @impl true
  def init(%{group_id: group_id}) do
    lanes = for lane <- 0..(@num_lanes - 1), into: %{}, do: {lane, %Lane{}}

    %__MODULE__{
      group_id: group_id,
      lanes: lanes,
      max_unarchived_events: archive_backpressure_limit()
    }
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
         :ok <- guard_expected_seq(session, opts[:expected_seq]),
         :ok <- guard_archive_backpressure(session, state.max_unarchived_events) do
      case Session.append_event(session, input) do
        {:appended, updated_session, event} ->
          new_lane = %{lane | sessions: Map.put(lane.sessions, session_id, updated_session)}
          new_state = put_in(state.lanes[lane_id], new_lane)

          Starcite.Observability.Telemetry.event_appended(
            session_id,
            event.type,
            event.actor,
            event.source,
            byte_size(Jason.encode!(event.payload))
          )

          reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
          effects = build_effects(session_id, event)
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
  def apply(_meta, {:ack_archived, lane_id, session_id, upto_seq}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, session} <- fetch_session(lane, session_id) do
      apply_archive_ack(state, lane_id, lane, session_id, session, upto_seq)
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  @impl true
  def apply(
        _meta,
        {:ack_archived_if_current, lane_id, session_id, expected_archived_seq, upto_seq},
        state
      ) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, session} <- fetch_session(lane, session_id),
         :ok <- guard_archived_seq(session, expected_archived_seq) do
      apply_archive_ack(state, lane_id, lane, session_id, session, upto_seq)
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  # ---------------------------------------------------------------------------
  # Queries
  # ---------------------------------------------------------------------------

  @doc """
  Query events with `seq > cursor`, ordered ascending.
  """
  def query_events_from_cursor(state, lane_id, session_id, cursor, limit) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, session} <- fetch_session(lane, session_id) do
      {:ok, Session.events_from_cursor(session, cursor, limit)}
    else
      _ -> {:error, :session_not_found}
    end
  end

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

  @doc """
  Query unarchived events (seq > archived_seq) up to `limit`.
  """
  def query_unarchived(state, lane_id, session_id, limit) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, %Session{} = session} <- fetch_session(lane, session_id) do
      Session.events_from_cursor(session, session.archived_seq, limit)
    else
      _ -> []
    end
  end

  @doc """
  Query lagging session IDs (`last_seq > archived_seq`) for a group.

  Returns up to `limit` IDs in deterministic order, with pagination metadata.
  """
  def query_lagging_sessions(state, limit, after_session_id \\ nil)
      when is_integer(limit) and limit > 0 and
             (is_nil(after_session_id) or
                (is_binary(after_session_id) and after_session_id != "")) do
    lagging_ids =
      state.lanes
      |> Enum.flat_map(fn {_lane_id, %Lane{sessions: sessions}} ->
        Enum.reduce(sessions, [], fn
          {session_id, %Session{last_seq: last_seq, archived_seq: archived_seq}}, acc
          when is_integer(last_seq) and is_integer(archived_seq) and last_seq > archived_seq ->
            [session_id | acc]

          _, acc ->
            acc
        end)
      end)
      |> Enum.sort()

    remaining =
      case after_session_id do
        nil -> lagging_ids
        after_id -> Enum.drop_while(lagging_ids, &(&1 <= after_id))
      end

    page = Enum.take(remaining, limit)
    has_more = length(remaining) > length(page)
    next_after = if has_more and page != [], do: List.last(page), else: nil

    {:ok, %{session_ids: page, has_more: has_more, next_after: next_after}}
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

  defp guard_archive_backpressure(_session, nil), do: :ok

  defp guard_archive_backpressure(%Session{last_seq: last_seq, archived_seq: archived_seq}, max)
       when is_integer(max) and max > 0 do
    lag = max(last_seq - archived_seq, 0)

    if lag >= max do
      {:error, {:archive_backpressure, lag, max}}
    else
      :ok
    end
  end

  defp guard_archive_backpressure(_session, _), do: :ok

  defp guard_archived_seq(%Session{archived_seq: archived_seq}, expected_archived_seq)
       when is_integer(expected_archived_seq) and expected_archived_seq >= 0 and
              archived_seq == expected_archived_seq,
       do: :ok

  defp guard_archived_seq(%Session{archived_seq: archived_seq}, expected_archived_seq)
       when is_integer(expected_archived_seq) and expected_archived_seq >= 0,
       do: {:error, {:archived_seq_mismatch, expected_archived_seq, archived_seq}}

  defp guard_archived_seq(_session, _expected_archived_seq),
    do: {:error, :invalid_expected_archived_seq}

  defp apply_archive_ack(state, lane_id, lane, session_id, session, upto_seq) do
    {updated_session, trimmed} = Session.persist_ack(session, upto_seq)
    new_lane = %{lane | sessions: Map.put(lane.sessions, session_id, updated_session)}
    new_state = put_in(state.lanes[lane_id], new_lane)

    tail_size =
      updated_session.event_log
      |> EventLog.entries()
      |> length()

    Starcite.Observability.Telemetry.archive_ack_applied(
      session_id,
      updated_session.last_seq,
      updated_session.archived_seq,
      trimmed,
      updated_session.retention.tail_keep,
      tail_size
    )

    {new_state, {:reply, {:ok, %{archived_seq: updated_session.archived_seq, trimmed: trimmed}}}}
  end

  defp archive_backpressure_limit do
    Application.get_env(:starcite, :max_unarchived_events, 250_000)
  end

  defp build_effects(session_id, event) do
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

    if archive_effects_enabled_now?() do
      archive_event =
        {
          :mod_call,
          Starcite.Archive,
          :mark_dirty,
          [session_id, event.seq]
        }

      [stream_event, archive_event]
    else
      [stream_event]
    end
  end

  defp archive_effects_enabled_now? do
    archive_expected?() or :ets.whereis(:starcite_archive_sessions) != :undefined
  end

  defp archive_expected? do
    from_env =
      case System.get_env("STARCITE_ARCHIVER_ENABLED") do
        nil -> false
        val -> val not in ["", "false", "0", "no", "off"]
      end

    Application.get_env(:starcite, :archive_enabled, false) || from_env
  end
end
