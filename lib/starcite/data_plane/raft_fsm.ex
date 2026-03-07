defmodule Starcite.DataPlane.RaftFSM do
  @moduledoc """
  Raft state machine for Starcite sessions.

  Stores session state (including auth-critical fields) and applies ordered commands through Raft consensus.
  """

  @behaviour :ra_machine

  alias Starcite.DataPlane.{CursorUpdate, EventStore}
  alias Starcite.Session
  alias Starcite.Session.ProducerIndex

  @machine_version 1

  @checkpoint_interval_entries Application.compile_env(
                                 :starcite,
                                 :raft_checkpoint_interval_entries,
                                 2_048
                               )

  defstruct [:group_id, :sessions, :last_checkpoint_index]

  @type session_state :: %Session{producer_cursors: ProducerIndex.t()}

  @type t :: %__MODULE__{
          group_id: term(),
          sessions: %{optional(String.t()) => session_state()},
          last_checkpoint_index: non_neg_integer() | nil
        }

  @impl true
  def init(%{group_id: group_id}) do
    %__MODULE__{group_id: group_id, sessions: %{}, last_checkpoint_index: nil}
  end

  @impl true
  def version, do: @machine_version

  @impl true
  def which_module(0), do: __MODULE__

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
    case Map.get(state.sessions, session_id) do
      nil ->
        session = Session.new_raft(session_id, title, creator_principal, tenant_id, metadata)

        new_state = %{state | sessions: Map.put(state.sessions, session_id, session)}

        reply_with_optional_effects(
          meta,
          new_state,
          {:reply, {:ok, Session.to_map(session)}}
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
    case Map.get(state.sessions, session_id) do
      %Session{} = session ->
        with :ok <- guard_expected_seq(session, expected_seq),
             {:ok, updated_session, reply, event_to_store} <-
               append_one_to_session(session, input) do
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

      nil ->
        reply_with_optional_effects(meta, state, {:reply, {:error, :session_not_found}})
    end
  end

  @impl true
  def apply(meta, {:append_events, _session_id, [], _expected_seq_or_opts}, state) do
    reply_with_optional_effects(meta, state, {:reply, {:error, :invalid_event}})
  end

  @impl true
  def apply(meta, {:append_events, session_id, inputs, expected_seq}, state)
      when is_list(inputs) and
             (is_nil(expected_seq) or (is_integer(expected_seq) and expected_seq >= 0)) do
    case Map.get(state.sessions, session_id) do
      %Session{} = session ->
        with :ok <- guard_expected_seq(session, expected_seq),
             {:ok, updated_session, replies, events_to_store} <-
               append_to_session(session, inputs) do
          :ok = put_appended_events(session_id, updated_session.tenant_id, events_to_store)
          new_state = %{state | sessions: Map.put(state.sessions, session_id, updated_session)}

          effects = build_effects_for_events(session_id, events_to_store)
          reply = {:reply, {:ok, %{results: replies, last_seq: updated_session.last_seq}}}
          reply_with_optional_effects(meta, new_state, reply, effects)
        else
          {:error, reason} ->
            reply_with_optional_effects(meta, state, {:reply, {:error, reason}})
        end

      nil ->
        reply_with_optional_effects(meta, state, {:reply, {:error, :session_not_found}})
    end
  end

  @impl true
  def apply(meta, {:append_events, session_id, inputs, opts}, state)
      when is_list(inputs) and is_list(opts) do
    __MODULE__.apply(
      meta,
      {:append_events, session_id, inputs, expected_seq_from_opts(opts)},
      state
    )
  end

  @impl true
  def apply(meta, {:ack_archived, entries}, state) when is_list(entries) and entries != [] do
    {new_state, applied, failed, archived_advanced?} =
      Enum.reduce(entries, {state, [], [], false}, fn
        {session_id, upto_seq},
        {%__MODULE__{sessions: sessions} = state_acc, applied_acc, failed_acc,
         archived_advanced_acc}
        when is_binary(session_id) and session_id != "" and is_integer(upto_seq) and upto_seq >= 0 ->
          case Map.get(sessions, session_id) do
            %Session{} = session ->
              previous_archived_seq = session.archived_seq
              {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)
              session_evicted? = evict_session_after_ack?(updated_session, previous_archived_seq)

              new_sessions =
                if session_evicted? do
                  Map.delete(sessions, session_id)
                else
                  Map.put(sessions, session_id, updated_session)
                end

              next_state = %{state_acc | sessions: new_sessions}

              trimmed =
                evict_archived_events(
                  session_id,
                  previous_archived_seq,
                  updated_session.archived_seq
                )

              tail_size = Session.tail_size(updated_session)
              tenant_id = updated_session.tenant_id

              Starcite.Observability.Telemetry.archive_ack_applied(
                session_id,
                tenant_id,
                updated_session.last_seq,
                updated_session.archived_seq,
                trimmed,
                updated_session.retention.tail_keep,
                tail_size
              )

              if session_evicted? do
                :ok =
                  Starcite.Observability.Telemetry.session_freeze(
                    session_id,
                    tenant_id,
                    :ok,
                    :archive_ack
                  )
              end

              applied_entry = %{
                session_id: session_id,
                archived_seq: updated_session.archived_seq,
                trimmed: trimmed
              }

              {next_state, [applied_entry | applied_acc], failed_acc,
               archived_advanced_acc or updated_session.archived_seq > previous_archived_seq}

            nil ->
              {state_acc, applied_acc,
               [%{session_id: session_id, reason: :session_not_found} | failed_acc],
               archived_advanced_acc}
          end

        _entry, {state_acc, applied_acc, failed_acc, archived_advanced_acc} ->
          {state_acc, applied_acc,
           [%{session_id: nil, reason: :invalid_archive_ack} | failed_acc], archived_advanced_acc}
      end)

    reply =
      {:reply, {:ok, %{applied: Enum.reverse(applied), failed: Enum.reverse(failed)}}}

    effects =
      case release_cursor_effect(meta, archived_advanced?, new_state) do
        nil -> []
        effect -> [effect]
      end

    reply_with_optional_effects(meta, new_state, reply, effects)
  end

  @impl true
  def apply(meta, {:ack_archived, session_id, upto_seq}, state) do
    __MODULE__.apply(meta, {:ack_archived, [{session_id, upto_seq}]}, state)
  end

  @impl true
  def apply(meta, {:hydrate_session, %Session{id: session_id} = session}, state) do
    case Map.get(state.sessions, session_id) do
      %Session{} ->
        reply_with_optional_effects(meta, state, {:reply, {:ok, :already_hot}})

      nil ->
        next_state = %{state | sessions: Map.put(state.sessions, session_id, session)}
        reply_with_optional_effects(meta, next_state, {:reply, {:ok, :hydrated}})
    end
  end

  @impl true
  def apply(_meta, {:machine_version, from, to}, state)
      when is_integer(from) and is_integer(to) do
    {ensure_state_schema(state), :ok}
  end

  # Queries

  @doc """
  Query one session by ID.
  """
  @spec query_session(t(), String.t()) :: {:ok, session_state()} | {:error, :session_not_found}
  def query_session(state, session_id) do
    case Map.get(state.sessions, session_id) do
      %Session{} = session -> {:ok, session}
      nil -> {:error, :session_not_found}
    end
  end

  # Helpers

  defp guard_expected_seq(_session, nil), do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0 and last_seq == expected_seq,
       do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0,
       do: {:error, {:expected_seq_conflict, expected_seq, last_seq}}

  defp expected_seq_from_opts(opts) when is_list(opts), do: opts[:expected_seq]

  defp evict_session_after_ack?(%Session{} = session, previous_archived_seq)
       when is_integer(previous_archived_seq) and previous_archived_seq >= 0 do
    session.last_seq > 0 and session.archived_seq == session.last_seq and
      session.archived_seq > previous_archived_seq
  end

  defp evict_archived_events(_session_id, previous_archived_seq, updated_archived_seq)
       when updated_archived_seq <= previous_archived_seq do
    0
  end

  defp evict_archived_events(session_id, _previous_archived_seq, updated_archived_seq) do
    EventStore.delete_below(session_id, updated_archived_seq + 1)
  end

  defp release_cursor_effect(%{index: raft_index}, true, %__MODULE__{} = state)
       when is_integer(raft_index) and raft_index > 0 do
    {:release_cursor, raft_index, state}
  end

  defp release_cursor_effect(_meta, _archived_advanced?, _state), do: nil

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
    last_checkpoint_index = state.last_checkpoint_index || 0

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

  defp ensure_state_schema(%__MODULE__{} = state) do
    last_checkpoint_index =
      case Map.get(state, :last_checkpoint_index) do
        value when is_integer(value) and value >= 0 -> value
        _ -> nil
      end

    sessions =
      case Map.get(state, :sessions) do
        value when is_map(value) -> value
        _ -> %{}
      end

    state
    |> Map.put(:last_checkpoint_index, last_checkpoint_index)
    |> Map.put(:sessions, sessions)
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
end
