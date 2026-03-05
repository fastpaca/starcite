defmodule Starcite.DataPlane.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores session state (including auth-critical fields) and applies ordered commands through Raft consensus.
  """

  @behaviour :ra_machine

  alias Starcite.DataPlane.{CursorUpdate, EventStore, SessionDiscovery}
  alias Starcite.Session
  alias Starcite.Session.ProducerIndex
  @machine_version 2
  @default_min_idle_polls 3
  @default_max_freeze_batch_size 50
  @default_hydrate_grace_polls 0

  @checkpoint_interval_entries Application.compile_env(
                                 :starcite,
                                 :raft_checkpoint_interval_entries,
                                 2_048
                               )

  defstruct [:group_id, :sessions, :poll_epoch, :last_checkpoint_index]

  @type session_state :: %Session{producer_cursors: ProducerIndex.t()}

  @type t :: %__MODULE__{
          group_id: term(),
          sessions: %{optional(String.t()) => session_state()},
          poll_epoch: non_neg_integer(),
          last_checkpoint_index: non_neg_integer() | nil
        }

  @impl true
  def init(%{group_id: group_id}) do
    %__MODULE__{
      group_id: group_id,
      sessions: %{},
      poll_epoch: 0,
      last_checkpoint_index: nil
    }
  end

  @impl true
  def version, do: @machine_version

  @impl true
  def which_module(0), do: __MODULE__

  def which_module(1), do: __MODULE__

  def which_module(@machine_version), do: __MODULE__

  def which_module(version) do
    raise ArgumentError, "unsupported Raft FSM version: #{inspect(version)}"
  end

  @impl true
  def state_enter(_ra_state, _state), do: []

  @impl true
  def apply(
        meta,
        {:create_session, session_id, title, creator_principal, tenant_id, metadata},
        state
      ) do
    poll_epoch = Map.get(state, :poll_epoch, 0)

    case Map.get(state.sessions, session_id) do
      nil ->
        session =
          Session.new(session_id,
            title: title,
            creator_principal: creator_principal,
            tenant_id: tenant_id,
            metadata: metadata,
            last_progress_poll: poll_epoch
          )

        new_state = %{state | sessions: Map.put(state.sessions, session_id, session)}
        effect = build_session_created_effect(session)

        reply_with_optional_effects(
          meta,
          new_state,
          {:reply, {:ok, Session.to_map(session)}},
          [effect]
        )

      %Session{} ->
        reply_with_optional_effects(
          meta,
          state,
          {:reply, {:error, :session_exists}}
        )
    end
  end

  @impl true
  def apply(meta, {:append_event, session_id, input, expected_seq}, state) do
    poll_epoch = Map.get(state, :poll_epoch, 0)

    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <- guard_expected_seq(session, expected_seq),
         {:ok, updated_session, reply, event_to_store} <- append_one_to_session(session, input) do
      updated_session = maybe_mark_progress(updated_session, event_to_store, poll_epoch)
      :ok = put_appended_event(session_id, updated_session.tenant_id, event_to_store)
      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}

      effect = build_effect_for_event(session_id, event_to_store)
      reply = {:reply, {:ok, reply}}
      effects = if is_nil(effect), do: [], else: [effect]

      reply_with_optional_effects(meta, new_state, reply, effects)
    else
      {:error, reason} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
    end
  end

  @impl true
  def apply(meta, {:append_events, _session_id, [], _opts}, state) do
    reply_with_optional_effects(meta, state, {:reply, {:error, :invalid_event}})
  end

  @impl true
  def apply(meta, {:append_events, session_id, inputs, opts}, state)
      when is_list(inputs) and (is_list(opts) or is_nil(opts)) do
    poll_epoch = Map.get(state, :poll_epoch, 0)

    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <- guard_expected_seq(session, expected_seq_from_opts(opts)),
         {:ok, updated_session, replies, events_to_store} <- append_to_session(session, inputs) do
      updated_session = maybe_mark_progress(updated_session, events_to_store, poll_epoch)
      :ok = put_appended_events(session_id, updated_session.tenant_id, events_to_store)
      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}

      effects = build_effects_for_events(session_id, events_to_store)
      reply = {:reply, {:ok, %{results: replies, last_seq: updated_session.last_seq}}}
      reply_with_optional_effects(meta, new_state, reply, effects)
    else
      {:error, reason} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
    end
  end

  @impl true
  def apply(meta, {:ack_archived, session_id, upto_seq}, state) do
    poll_epoch = Map.get(state, :poll_epoch, 0)

    with {:ok, session} <- fetch_session(state.sessions, session_id) do
      previous_archived_seq = session.archived_seq
      {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)

      updated_session =
        maybe_mark_progress(
          updated_session,
          previous_archived_seq,
          updated_session.archived_seq,
          poll_epoch
        )

      new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}

      evicted =
        evict_archived_events(session_id, previous_archived_seq, updated_session.archived_seq)

      tail_size = Session.tail_size(updated_session)
      tenant_id = updated_session.tenant_id

      Starcite.Observability.Telemetry.archive_ack_applied(
        session_id,
        tenant_id,
        updated_session.last_seq,
        updated_session.archived_seq,
        evicted,
        updated_session.retention.tail_keep,
        tail_size
      )

      reply = {:reply, {:ok, %{archived_seq: updated_session.archived_seq, trimmed: evicted}}}

      effects =
        case release_cursor_effect(
               meta,
               previous_archived_seq,
               updated_session.archived_seq,
               new_state
             ) do
          nil -> []
          effect -> [effect]
        end

      reply_with_optional_effects(meta, new_state, reply, effects)
    else
      {:error, reason} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
    end
  end

  @impl true
  def apply(meta, {:eviction_tick, opts}, state) do
    current_poll_epoch = Map.get(state, :poll_epoch, 0)

    with {:ok, min_idle_polls, max_freeze_batch_size, hydrate_grace_polls} <-
           resolve_eviction_opts(opts) do
      poll_epoch = current_poll_epoch + 1
      next_state = %{state | poll_epoch: poll_epoch}
      hot_sessions = map_size(next_state.sessions)

      candidates =
        next_state.sessions
        |> freeze_candidates(poll_epoch, min_idle_polls, hydrate_grace_polls)
        |> Enum.take(max_freeze_batch_size)

      reply_with_optional_effects(
        meta,
        next_state,
        {:reply,
         {:ok, %{poll_epoch: poll_epoch, candidates: candidates, hot_sessions: hot_sessions}}}
      )
    else
      {:error, reason} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
    end
  end

  @impl true
  def apply(
        meta,
        {:freeze_session, session_id, expected_last_seq, expected_archived_seq,
         expected_last_progress_poll},
        state
      ) do
    with {:ok, session} <- fetch_session(state.sessions, session_id),
         :ok <-
           guard_freeze_session(
             session,
             expected_last_seq,
             expected_archived_seq,
             expected_last_progress_poll
           ) do
      next_state = %{state | sessions: Map.delete(state.sessions, session_id)}
      effect = build_session_frozen_effect(session_id, session.tenant_id)
      reply_with_optional_effects(meta, next_state, {:reply, {:ok, %{id: session_id}}}, [effect])
    else
      {:error, :session_not_found} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, :session_not_found}})

      {:error, :freeze_conflict} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, :freeze_conflict}})
    end
  end

  @impl true
  def apply(meta, {:hydrate_session, snapshot}, state) do
    poll_epoch = Map.get(state, :poll_epoch, 0)

    with {:ok, session_id} <- snapshot_session_id(snapshot) do
      case Map.get(state.sessions, session_id) do
        %Session{} ->
          reply_with_optional_effects(meta, state, {:reply, {:ok, :already_hot}})

        nil ->
          with {:ok, session} <- session_from_snapshot(snapshot, poll_epoch) do
            next_state = %{state | sessions: Map.put(state.sessions, session_id, session)}
            effect = build_session_hydrated_effect(session.id, session.tenant_id)
            reply_with_optional_effects(meta, next_state, {:reply, {:ok, :hydrated}}, [effect])
          else
            {:error, reason} ->
              reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
          end
      end
    else
      {:error, reason} ->
        reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
    end
  end

  @impl true
  def apply(_meta, {:machine_version, from, to}, state)
      when is_integer(from) and is_integer(to) do
    {migrate_state(state), :ok}
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

  defp maybe_mark_progress(%Session{} = session, nil, _poll_epoch), do: session
  defp maybe_mark_progress(%Session{} = session, [], _poll_epoch), do: session

  defp maybe_mark_progress(%Session{} = session, _event_or_events, poll_epoch)
       when is_integer(poll_epoch) and poll_epoch >= 0 do
    %Session{session | last_progress_poll: poll_epoch}
  end

  defp maybe_mark_progress(
         %Session{} = session,
         previous_archived_seq,
         updated_archived_seq,
         poll_epoch
       )
       when is_integer(previous_archived_seq) and is_integer(updated_archived_seq) and
              is_integer(poll_epoch) and poll_epoch >= 0 do
    if updated_archived_seq > previous_archived_seq do
      %Session{session | last_progress_poll: poll_epoch}
    else
      session
    end
  end

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

  defp put_appended_events(_session_id, _tenant_id, []), do: :ok

  defp put_appended_events(session_id, tenant_id, events)
       when is_binary(session_id) and is_binary(tenant_id) and is_list(events) do
    EventStore.put_events(session_id, tenant_id, events)
  end

  defp put_appended_event(_session_id, _tenant_id, nil), do: :ok

  defp put_appended_event(session_id, tenant_id, %{seq: seq} = event)
       when is_binary(session_id) and is_binary(tenant_id) and is_integer(seq) and seq > 0 do
    EventStore.put_event(session_id, tenant_id, event)
  end

  defp reply_with_optional_effects(meta, %__MODULE__{} = state, reply, effects \\ [])
       when is_list(effects) do
    {next_state, next_effects} = maybe_add_checkpoint_effect(meta, state, effects)

    case next_effects do
      [] -> {next_state, reply}
      _ -> {next_state, reply, next_effects}
    end
  end

  defp maybe_add_checkpoint_effect(
         %{index: raft_index},
         %__MODULE__{} = state,
         effects
       )
       when is_integer(raft_index) and raft_index > 0 and is_list(effects) do
    interval = checkpoint_interval_entries()
    last_checkpoint_index = Map.get(state, :last_checkpoint_index) || 0

    # Keep checkpoint opportunities independent of archive ack / release-cursor cadence.
    if raft_index - last_checkpoint_index >= interval do
      updated_state = Map.put(state, :last_checkpoint_index, raft_index)
      {updated_state, effects ++ [{:checkpoint, raft_index, updated_state}]}
    else
      {state, effects}
    end
  end

  defp maybe_add_checkpoint_effect(_meta, %__MODULE__{} = state, effects)
       when is_list(effects) do
    {state, effects}
  end

  defp checkpoint_interval_entries do
    case @checkpoint_interval_entries do
      value when is_integer(value) and value > 0 -> value
      _ -> 2_048
    end
  end

  defp migrate_state(%__MODULE__{} = state) do
    poll_epoch =
      case Map.get(state, :poll_epoch) do
        value when is_integer(value) and value >= 0 -> value
        _ -> 0
      end

    last_checkpoint_index =
      case Map.get(state, :last_checkpoint_index) do
        value when is_integer(value) and value >= 0 -> value
        _ -> nil
      end

    sessions =
      state.sessions
      |> Enum.into(%{}, fn {session_id, session} ->
        {session_id, migrate_session(session, poll_epoch)}
      end)

    state
    |> Map.put(:poll_epoch, poll_epoch)
    |> Map.put(:last_checkpoint_index, last_checkpoint_index)
    |> Map.put(:sessions, sessions)
  end

  defp migrate_session(%Session{} = session, poll_epoch) do
    last_progress_poll =
      case Map.get(session, :last_progress_poll) do
        value when is_integer(value) and value >= 0 -> value
        _ -> poll_epoch
      end

    last_hydrated_poll =
      case Map.get(session, :last_hydrated_poll) do
        value when is_integer(value) and value >= 0 -> value
        _ -> nil
      end

    session
    |> Map.put(:last_progress_poll, last_progress_poll)
    |> Map.put(:last_hydrated_poll, last_hydrated_poll)
  end

  defp resolve_eviction_opts(opts) when is_list(opts) do
    min_idle_polls = Keyword.get(opts, :min_idle_polls, @default_min_idle_polls)

    max_freeze_batch_size =
      Keyword.get(opts, :max_freeze_batch_size, @default_max_freeze_batch_size)

    hydrate_grace_polls = Keyword.get(opts, :hydrate_grace_polls, @default_hydrate_grace_polls)

    validate_eviction_opts(min_idle_polls, max_freeze_batch_size, hydrate_grace_polls)
  end

  defp resolve_eviction_opts(%{} = opts) do
    min_idle_polls =
      Map.get_lazy(opts, :min_idle_polls, fn ->
        Map.get(opts, "min_idle_polls", @default_min_idle_polls)
      end)

    max_freeze_batch_size =
      Map.get_lazy(opts, :max_freeze_batch_size, fn ->
        Map.get(opts, "max_freeze_batch_size", @default_max_freeze_batch_size)
      end)

    hydrate_grace_polls =
      Map.get_lazy(opts, :hydrate_grace_polls, fn ->
        Map.get(opts, "hydrate_grace_polls", @default_hydrate_grace_polls)
      end)

    validate_eviction_opts(min_idle_polls, max_freeze_batch_size, hydrate_grace_polls)
  end

  defp resolve_eviction_opts(nil) do
    {:ok, @default_min_idle_polls, @default_max_freeze_batch_size, @default_hydrate_grace_polls}
  end

  defp resolve_eviction_opts(_opts), do: {:error, :invalid_eviction_opts}

  defp validate_eviction_opts(min_idle_polls, max_freeze_batch_size, hydrate_grace_polls)
       when is_integer(min_idle_polls) and min_idle_polls >= 0 and
              is_integer(max_freeze_batch_size) and max_freeze_batch_size > 0 and
              is_integer(hydrate_grace_polls) and hydrate_grace_polls >= 0 do
    {:ok, min_idle_polls, max_freeze_batch_size, hydrate_grace_polls}
  end

  defp validate_eviction_opts(_min_idle_polls, _max_freeze_batch_size, _hydrate_grace_polls),
    do: {:error, :invalid_eviction_opts}

  defp freeze_candidates(sessions, poll_epoch, min_idle_polls, hydrate_grace_polls)
       when is_map(sessions) and is_integer(poll_epoch) and poll_epoch >= 0 and
              is_integer(min_idle_polls) and min_idle_polls >= 0 and
              is_integer(hydrate_grace_polls) and hydrate_grace_polls >= 0 do
    sessions
    |> Enum.reduce([], fn {session_id, %Session{} = session}, acc ->
      idle_polls = max(poll_epoch - session.last_progress_poll, 0)

      if session.last_seq == session.archived_seq and idle_polls >= min_idle_polls and
           hydrate_grace_elapsed?(session, poll_epoch, hydrate_grace_polls) do
        [{session_id, session.last_seq, session.archived_seq, session.last_progress_poll} | acc]
      else
        acc
      end
    end)
    |> Enum.sort_by(fn {session_id, _last_seq, _archived_seq, last_progress_poll} ->
      {last_progress_poll, session_id}
    end)
  end

  defp guard_freeze_session(
         %Session{} = session,
         expected_last_seq,
         expected_archived_seq,
         expected_last_progress_poll
       )
       when is_integer(expected_last_seq) and expected_last_seq >= 0 and
              is_integer(expected_archived_seq) and expected_archived_seq >= 0 and
              is_integer(expected_last_progress_poll) and expected_last_progress_poll >= 0 do
    if session.last_seq == expected_last_seq and session.archived_seq == expected_archived_seq and
         session.last_progress_poll == expected_last_progress_poll and
         session.last_seq == session.archived_seq do
      :ok
    else
      {:error, :freeze_conflict}
    end
  end

  defp guard_freeze_session(
         _session,
         _expected_last_seq,
         _expected_archived_seq,
         _expected_last_progress_poll
       ),
       do: {:error, :freeze_conflict}

  defp hydrate_grace_elapsed?(_session, _poll_epoch, 0), do: true

  defp hydrate_grace_elapsed?(%Session{} = session, poll_epoch, hydrate_grace_polls)
       when is_integer(poll_epoch) and poll_epoch >= 0 and is_integer(hydrate_grace_polls) and
              hydrate_grace_polls > 0 do
    case Map.get(session, :last_hydrated_poll) do
      value when is_integer(value) and value >= 0 ->
        poll_epoch - value >= hydrate_grace_polls

      _ ->
        true
    end
  end

  defp snapshot_session_id(%{} = snapshot) do
    case snapshot_get(snapshot, :id) do
      {:ok, session_id} when is_binary(session_id) and session_id != "" -> {:ok, session_id}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_session_id(_snapshot), do: {:error, :invalid_snapshot}

  defp session_from_snapshot(%{} = snapshot, poll_epoch)
       when is_integer(poll_epoch) and poll_epoch >= 0 do
    with {:ok, session_id} <- snapshot_get_required_binary(snapshot, :id),
         {:ok, tenant_id} <- snapshot_get_required_binary(snapshot, :tenant_id),
         {:ok, title} <- snapshot_get_optional_binary(snapshot, :title),
         {:ok, creator_principal} <- snapshot_get_optional_creator_principal(snapshot),
         {:ok, metadata} <- snapshot_get_optional_map(snapshot, :metadata, %{}),
         {:ok, inserted_at} <- snapshot_get_inserted_at(snapshot),
         {:ok, last_seq} <- snapshot_get_optional_non_neg_integer(snapshot, :last_seq, 0),
         {:ok, archived_seq} <- snapshot_get_optional_non_neg_integer(snapshot, :archived_seq, 0),
         {:ok, last_progress_poll} <-
           snapshot_get_optional_non_neg_integer(
             snapshot,
             :last_progress_poll,
             poll_epoch
           ),
         {:ok, retention} <- snapshot_get_optional_retention(snapshot),
         {:ok, producer_cursors} <- snapshot_get_optional_producer_cursors(snapshot),
         :ok <- validate_snapshot_cursors(last_seq, archived_seq) do
      session =
        Session.new(session_id,
          title: title,
          creator_principal: creator_principal,
          tenant_id: tenant_id,
          metadata: metadata,
          timestamp: inserted_at,
          tail_keep: retention.tail_keep,
          producer_max_entries: retention.producer_max_entries,
          last_progress_poll: last_progress_poll,
          last_hydrated_poll: poll_epoch
        )

      {:ok,
       %Session{
         session
         | last_seq: last_seq,
           archived_seq: archived_seq,
           producer_cursors: producer_cursors
       }}
    end
  end

  defp session_from_snapshot(_snapshot, _poll_epoch), do: {:error, :invalid_snapshot}

  defp snapshot_get_required_binary(snapshot, key) do
    case snapshot_get(snapshot, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_optional_binary(snapshot, key) do
    case snapshot_get(snapshot, key) do
      :error -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_optional_creator_principal(snapshot) do
    case snapshot_get(snapshot, :creator_principal) do
      :error -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, %Starcite.Auth.Principal{} = principal} -> {:ok, principal}
      {:ok, principal} when is_map(principal) -> {:ok, principal}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_optional_map(snapshot, key, default) when is_map(default) do
    case snapshot_get(snapshot, key) do
      :error -> {:ok, default}
      {:ok, value} when is_map(value) -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_optional_non_neg_integer(snapshot, key, default)
       when is_integer(default) and default >= 0 do
    case snapshot_get(snapshot, key) do
      :error -> {:ok, default}
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_inserted_at(snapshot) do
    case snapshot_get(snapshot, :inserted_at) do
      {:ok, %DateTime{} = datetime} -> {:ok, DateTime.to_naive(datetime)}
      {:ok, %NaiveDateTime{} = datetime} -> {:ok, datetime}
      {:ok, value} when is_binary(value) -> parse_naive_datetime(value)
      :error -> snapshot_get_created_at(snapshot)
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_created_at(snapshot) do
    case snapshot_get(snapshot, :created_at) do
      {:ok, %DateTime{} = datetime} -> {:ok, DateTime.to_naive(datetime)}
      {:ok, %NaiveDateTime{} = datetime} -> {:ok, datetime}
      {:ok, value} when is_binary(value) -> parse_naive_datetime(value)
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp parse_naive_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_naive(datetime)}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp snapshot_get_optional_retention(snapshot) do
    with {:ok, retention_map} <- snapshot_get_optional_map(snapshot, :retention, %{}),
         {:ok, tail_keep} <- retention_value(retention_map, :tail_keep),
         {:ok, producer_max_entries} <- retention_value(retention_map, :producer_max_entries) do
      {:ok, %{tail_keep: tail_keep, producer_max_entries: producer_max_entries}}
    end
  end

  defp retention_value(retention, key) do
    case map_fetch_key(retention, key) do
      :error -> {:ok, retention_default(key)}
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp retention_default(:tail_keep) do
    Application.get_env(:starcite, :tail_keep, 1_000)
  end

  defp retention_default(:producer_max_entries) do
    Application.get_env(:starcite, :producer_max_entries, 10_000)
  end

  defp snapshot_get_optional_producer_cursors(snapshot) do
    with {:ok, raw_cursors} <- snapshot_get_optional_map(snapshot, :producer_cursors, %{}) do
      decode_producer_cursors(raw_cursors)
    end
  end

  defp decode_producer_cursors(cursors) when map_size(cursors) == 0, do: {:ok, %{}}

  defp decode_producer_cursors(cursors) when is_map(cursors) do
    cursors
    |> Enum.reduce_while({:ok, %{}}, fn
      {producer_id, cursor}, {:ok, acc} when is_binary(producer_id) and producer_id != "" ->
        case decode_producer_cursor(cursor) do
          {:ok, decoded} -> {:cont, {:ok, Map.put(acc, producer_id, decoded)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      _, _acc ->
        {:halt, {:error, :invalid_snapshot}}
    end)
  end

  defp decode_producer_cursor(%{} = cursor) do
    with {:ok, producer_seq} <- map_fetch_pos_integer(cursor, :producer_seq),
         {:ok, session_seq} <- map_fetch_pos_integer(cursor, :session_seq),
         {:ok, hash} <- map_fetch_binary(cursor, :hash) do
      {:ok,
       %{producer_seq: producer_seq, session_seq: session_seq, hash: decode_cursor_hash(hash)}}
    end
  end

  defp decode_producer_cursor(_cursor), do: {:error, :invalid_snapshot}

  defp decode_cursor_hash(hash) when is_binary(hash) do
    case Base.url_decode64(hash, padding: false) do
      {:ok, decoded} when decoded != "" -> decoded
      _ -> hash
    end
  end

  defp map_fetch_pos_integer(map, key) when is_map(map) do
    case map_fetch_key(map, key) do
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp map_fetch_binary(map, key) when is_map(map) do
    case map_fetch_key(map, key) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :invalid_snapshot}
    end
  end

  defp validate_snapshot_cursors(last_seq, archived_seq)
       when is_integer(last_seq) and is_integer(archived_seq) and last_seq >= 0 and
              archived_seq >= 0 and
              archived_seq <= last_seq do
    :ok
  end

  defp validate_snapshot_cursors(_last_seq, _archived_seq), do: {:error, :invalid_snapshot}

  defp snapshot_get(map, key) when is_map(map) do
    map_fetch_key(map, key)
  end

  defp map_fetch_key(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(key))
    end
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

  defp build_session_created_effect(%Session{
         id: session_id,
         tenant_id: tenant_id,
         inserted_at: inserted_at
       })
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    {
      :mod_call,
      SessionDiscovery,
      :publish_created,
      [session_id, tenant_id, [occurred_at: iso8601_utc(inserted_at)]]
    }
  end

  defp build_session_frozen_effect(session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    {
      :mod_call,
      SessionDiscovery,
      :publish_frozen,
      [session_id, tenant_id, []]
    }
  end

  defp build_session_hydrated_effect(session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    {
      :mod_call,
      SessionDiscovery,
      :publish_hydrated,
      [session_id, tenant_id, []]
    }
  end

  defp iso8601_utc(%NaiveDateTime{} = inserted_at) do
    inserted_at
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end
end
