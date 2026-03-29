defmodule Starcite.DataPlane.SessionLog do
  @moduledoc """
  In-memory session log for one session replica.

  The owner log sequences and batches local writes. Follower logs only accept
  replicated state from `SessionQuorum`.
  """

  @behaviour :gen_batch_server

  alias Starcite.DataPlane.{CursorUpdate, EventStore, SessionStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.Routing.SessionRouter
  alias Starcite.Session

  @registry Starcite.DataPlane.SessionLogRegistry
  @min_batch_size Application.compile_env(:starcite, :session_log_batch_min_size, 8)
  @max_batch_size Application.compile_env(:starcite, :session_log_batch_max_size, 256)

  @type role :: :owner | :follower
  @type data :: %{
          required(:session) => Session.t(),
          required(:producer_cursors) => map(),
          required(:replicate_fun) => (Session.t(), [map()], [node()] -> :ok | {:error, term()}),
          required(:role) => role(),
          required(:idle_timeout_ms) => pos_integer() | :infinity,
          required(:idle_check_interval_ms) => pos_integer(),
          required(:last_activity_mono_ms) => integer(),
          required(:idle_timer_ref) => reference() | nil
        }

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)
    start_reason = Keyword.get(opts, :start_reason, :startup)
    replicate_fun = Keyword.fetch!(opts, :replicate_fun)
    idle_timeout_ms = validate_idle_timeout_ms!(Keyword.fetch!(opts, :idle_timeout_ms))

    idle_check_interval_ms =
      validate_idle_check_interval_ms!(Keyword.fetch!(opts, :idle_check_interval_ms))

    :gen_batch_server.start_link(
      via(session.id),
      __MODULE__,
      %{
        session: session,
        start_reason: start_reason,
        replicate_fun: replicate_fun,
        idle_timeout_ms: idle_timeout_ms,
        idle_check_interval_ms: idle_check_interval_ms
      },
      min_batch_size: @min_batch_size,
      max_batch_size: @max_batch_size
    )
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)

    %{
      id: {__MODULE__, session.id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      shutdown: 500,
      type: :worker
    }
  end

  @spec via(String.t()) :: {:via, Registry, {module(), String.t()}}
  def via(session_id) when is_binary(session_id) and session_id != "" do
    {:via, Registry, {@registry, session_id}}
  end

  @impl true
  def init(%{
        session: %Session{} = session,
        start_reason: start_reason,
        replicate_fun: replicate_fun,
        idle_timeout_ms: idle_timeout_ms,
        idle_check_interval_ms: idle_check_interval_ms
      })
      when is_function(replicate_fun, 3) do
    Process.flag(:message_queue_data, :off_heap)

    resolved_session =
      case SessionStore.get_session_cached(session.id) do
        {:ok, %Session{} = loaded} -> loaded
        :error -> session
      end

    log_session =
      resolved_session
      |> normalize_session_epoch()
      |> apply_routing_epoch()

    role = role_for_session(log_session.id)

    if start_reason == :hydrate do
      :ok = emit_lifecycle(log_session, role, "session.hydrating")
      :ok = Telemetry.session_hydrate(log_session.id, log_session.tenant_id, :ok, :hydrate)
    end

    :ok = emit_lifecycle(log_session, role, "session.activated")
    now = now_monotonic_ms()

    data =
      %{
        session: log_session,
        producer_cursors: %{},
        replicate_fun: replicate_fun,
        role: role,
        idle_timeout_ms: idle_timeout_ms,
        idle_check_interval_ms: idle_check_interval_ms,
        last_activity_mono_ms: now,
        idle_timer_ref: nil
      }
      |> schedule_idle_tick()

    {:ok, data}
  end

  @impl true
  def handle_batch(batch, data) when is_list(batch) do
    data = if activity_batch?(batch), do: touch_activity(data), else: data

    case process_batch(batch, data, []) do
      {:stop, reason} ->
        {:stop, reason}

      {next_data, actions} ->
        {:ok, actions, next_data}
    end
  end

  defp process_batch([], data, actions), do: {data, actions}

  defp process_batch(batch, %{role: :owner} = data, actions) do
    case collect_append_group(batch) do
      {[], _rest} ->
        [op | rest] = batch

        case process_single(op, data, actions) do
          {:stop, reason} -> {:stop, reason}
          {next_data, next_actions} -> process_batch(rest, next_data, next_actions)
        end

      {group, rest} ->
        {next_data, next_actions} = process_append_group(group, data, actions)
        process_batch(rest, next_data, next_actions)
    end
  end

  defp process_batch([op | rest], data, actions) do
    case process_single(op, data, actions) do
      {:stop, reason} -> {:stop, reason}
      {next_data, next_actions} -> process_batch(rest, next_data, next_actions)
    end
  end

  defp collect_append_group(batch), do: collect_append_group(batch, nil, [])

  defp collect_append_group(
         [
           {:call, from, {:append_event, input, nil, replicas}} | rest
         ],
         nil,
         acc
       )
       when is_map(input) and is_list(replicas) do
    request = %{from: from, kind: :append_event, inputs: [input], replicas: replicas}
    collect_append_group(rest, replicas, [request | acc])
  end

  defp collect_append_group(
         [
           {:call, from, {:append_events, inputs, nil, replicas}} | rest
         ],
         nil,
         acc
       )
       when is_list(inputs) and inputs != [] and is_list(replicas) do
    request = %{from: from, kind: :append_events, inputs: inputs, replicas: replicas}
    collect_append_group(rest, replicas, [request | acc])
  end

  defp collect_append_group(
         [
           {:call, from, {:append_event, input, nil, replicas}} | rest
         ],
         replicas,
         acc
       )
       when is_map(input) and is_list(replicas) do
    request = %{from: from, kind: :append_event, inputs: [input], replicas: replicas}
    collect_append_group(rest, replicas, [request | acc])
  end

  defp collect_append_group(
         [
           {:call, from, {:append_events, inputs, nil, replicas}} | rest
         ],
         replicas,
         acc
       )
       when is_list(inputs) and inputs != [] and is_list(replicas) do
    request = %{from: from, kind: :append_events, inputs: inputs, replicas: replicas}
    collect_append_group(rest, replicas, [request | acc])
  end

  defp collect_append_group(rest, _replicas, []), do: {[], rest}
  defp collect_append_group(rest, _replicas, acc), do: {Enum.reverse(acc), rest}

  defp process_single(
         {:info, {:timeout, idle_timer_ref, :idle_tick}},
         %{idle_timer_ref: idle_timer_ref} = data,
         actions
       )
       when is_reference(idle_timer_ref) do
    data = %{data | idle_timer_ref: nil}

    if should_freeze?(data) do
      freeze_session(data)
    else
      {schedule_idle_tick(data), actions}
    end
  end

  defp process_single({:info, _message}, data, actions), do: {data, actions}

  defp process_single({:call, from, :get_role}, %{role: role} = data, actions) do
    {data, actions ++ [{:reply, from, role}]}
  end

  defp process_single({:call, from, :describe}, %{role: role, session: session} = data, actions) do
    {data, actions ++ [{:reply, from, {:ok, %{role: role, session: session}}}]}
  end

  defp process_single({:call, from, :get_session}, %{session: session} = data, actions) do
    {data, actions ++ [{:reply, from, {:ok, session}}]}
  end

  defp process_single({:call, from, :fetch_cursor_snapshot}, %{session: session} = data, actions) do
    {data, actions ++ [{:reply, from, {:ok, cursor_snapshot(session)}}]}
  end

  defp process_single({:call, from, :fetch_archived_seq}, %{session: session} = data, actions) do
    {data, actions ++ [{:reply, from, {:ok, session.archived_seq}}]}
  end

  defp process_single(
         {:call, from, {:append_event, input, expected_seq, replicas}},
         %{role: :owner} = data,
         actions
       )
       when is_map(input) and is_list(replicas) do
    process_single_append(
      %{
        from: from,
        kind: :append_event,
        inputs: [input],
        expected_seq: expected_seq,
        replicas: replicas
      },
      data,
      actions
    )
  end

  defp process_single(
         {:call, from, {:append_events, inputs, expected_seq, replicas}},
         %{role: :owner} = data,
         actions
       )
       when is_list(inputs) and inputs != [] and is_list(replicas) do
    process_single_append(
      %{
        from: from,
        kind: :append_events,
        inputs: inputs,
        expected_seq: expected_seq,
        replicas: replicas
      },
      data,
      actions
    )
  end

  defp process_single(
         {:call, from, {:append_event, _input, _expected_seq, _replicas}},
         %{role: :owner} = data,
         actions
       ) do
    {data, actions ++ [{:reply, from, {:error, :invalid_event}}]}
  end

  defp process_single(
         {:call, from, {:append_events, _inputs, _expected_seq, _replicas}},
         %{role: :owner} = data,
         actions
       ) do
    {data, actions ++ [{:reply, from, {:error, :invalid_event}}]}
  end

  defp process_single(
         {:call, from, {:append_event, _input, _expected_seq, _replicas}},
         data,
         actions
       ) do
    :ok = Telemetry.routing_fence(data.session.id, :session_log, :not_owner)
    {data, actions ++ [{:reply, from, {:error, :not_owner}}]}
  end

  defp process_single(
         {:call, from, {:append_events, _inputs, _expected_seq, _replicas}},
         data,
         actions
       ) do
    :ok = Telemetry.routing_fence(data.session.id, :session_log, :not_owner)
    {data, actions ++ [{:reply, from, {:error, :not_owner}}]}
  end

  defp process_single(
         {:call, from, {:ack_archived, upto_seq, replicas}},
         %{role: :owner, session: session} = data,
         actions
       )
       when is_integer(upto_seq) and upto_seq >= 0 and is_list(replicas) do
    previous_archived_seq = session.archived_seq
    {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)
    next_session = normalize_session_epoch(updated_session)

    case replicate_state(data, next_session, [], replicas) do
      :ok ->
        evicted =
          evict_archived_events(next_session.id, previous_archived_seq, next_session.archived_seq)

        tail_size = Session.tail_size(next_session)

        Telemetry.archive_ack_applied(
          next_session.id,
          next_session.tenant_id,
          next_session.last_seq,
          next_session.archived_seq,
          evicted,
          Session.tail_keep(),
          tail_size
        )

        :ok = SessionStore.put_session(next_session)

        reply = %{
          archived_seq: next_session.archived_seq,
          trimmed: evicted,
          committed_cursor: %{epoch: next_session.epoch, seq: next_session.archived_seq}
        }

        {%{data | session: next_session}, actions ++ [{:reply, from, {:ok, reply}}]}

      {:error, _reason} = error ->
        {data, actions ++ [{:reply, from, error}]}
    end
  end

  defp process_single(
         {:call, from, {:ack_archived, _upto_seq, _replicas}},
         %{role: :owner} = data,
         actions
       ) do
    {data, actions ++ [{:reply, from, {:error, :invalid_event}}]}
  end

  defp process_single({:call, from, {:ack_archived, _upto_seq, _replicas}}, data, actions) do
    :ok = Telemetry.routing_fence(data.session.id, :session_log, :not_owner)
    {data, actions ++ [{:reply, from, {:error, :not_owner}}]}
  end

  defp process_single(
         {:call, from,
          {:apply_replica, %Session{id: session_id} = incoming_session, incoming_events}},
         %{session: current_session} = data,
         actions
       )
       when is_binary(session_id) and session_id != "" and is_list(incoming_events) do
    normalized_session = normalize_session_epoch(incoming_session)
    normalized_events = put_events_epoch(incoming_events, normalized_session.epoch)

    if should_apply_replication?(normalized_session, current_session) do
      previous_archived_seq = current_session.archived_seq

      :ok =
        put_appended_events(
          normalized_session.id,
          normalized_session.tenant_id,
          normalized_events
        )

      _evicted =
        evict_archived_events(
          normalized_session.id,
          previous_archived_seq,
          normalized_session.archived_seq
        )

      :ok = SessionStore.put_session(normalized_session)

      {%{data | session: normalized_session, role: :follower}, actions ++ [{:reply, from, :ok}]}
    else
      {data, actions ++ [{:reply, from, :ok}]}
    end
  end

  defp process_single(
         {:call, from, {:apply_replica, _incoming_session, _incoming_events}},
         data,
         actions
       ) do
    {data, actions ++ [{:reply, from, {:error, :invalid_event}}]}
  end

  defp process_single({:call, from, _message}, data, actions) do
    {data, actions ++ [{:reply, from, {:error, :unsupported_command}}]}
  end

  defp process_single(_other, data, actions), do: {data, actions}

  defp process_single_append(
         request,
         %{session: session, producer_cursors: producer_cursors} = data,
         actions
       ) do
    with :ok <- guard_expected_seq(session, request.expected_seq),
         {:ok, next_session, next_cursors, next_events, outcome} <-
           execute_append_request(session, producer_cursors, request) do
      next_session = normalize_session_epoch(next_session)
      events = put_events_epoch(next_events, next_session.epoch)

      commit_single_append(
        data,
        actions,
        request.from,
        next_session,
        next_cursors,
        events,
        outcome,
        request.replicas
      )
    else
      {:error, reason} ->
        {data, actions ++ [{:reply, request.from, {:error, reason}}]}
    end
  end

  defp process_append_group(
         requests,
         %{session: session, producer_cursors: producer_cursors} = data,
         actions
       ) do
    replicas = hd(requests).replicas

    {next_session, next_cursors, events_acc, outcomes} =
      Enum.reduce(requests, {session, producer_cursors, [], []}, fn request,
                                                                    {current_session,
                                                                     current_cursors, events,
                                                                     outcomes} ->
        case execute_append_request(current_session, current_cursors, request) do
          {:ok, updated_session, updated_cursors, request_events, outcome} ->
            {
              normalize_session_epoch(updated_session),
              updated_cursors,
              events ++ request_events,
              outcomes ++ [outcome]
            }

          {:error, reason} ->
            {current_session, current_cursors, events,
             outcomes ++ [{:error, request.from, reason}]}
        end
      end)

    if successful_outcomes?(outcomes) do
      committed_events = put_events_epoch(events_acc, next_session.epoch)

      commit_append_group(
        data,
        actions,
        next_session,
        next_cursors,
        committed_events,
        outcomes,
        replicas
      )
    else
      reply_actions =
        Enum.map(outcomes, fn outcome ->
          {:reply, outcome_from(outcome), finalize_outcome(outcome, session)}
        end)

      {data, actions ++ reply_actions}
    end
  end

  defp successful_outcomes?(outcomes) when is_list(outcomes) do
    Enum.any?(
      outcomes,
      &(match?({:single_success, _, _}, &1) or match?({:multi_success, _, _}, &1))
    )
  end

  defp execute_append_request(
         session,
         producer_cursors,
         %{kind: :append_event, from: from, inputs: [input]}
       ) do
    with {:ok, updated_session, updated_cursors, reply, event_to_store} <-
           append_one_to_session(session, producer_cursors, input) do
      {:ok, updated_session, updated_cursors, event_list(event_to_store),
       {:single_success, from, reply}}
    end
  end

  defp execute_append_request(
         session,
         producer_cursors,
         %{kind: :append_events, from: from, inputs: inputs}
       ) do
    with {:ok, updated_session, updated_cursors, replies, events_to_store} <-
           append_to_session(session, producer_cursors, inputs) do
      response = %{results: replies, last_seq: updated_session.last_seq}
      {:ok, updated_session, updated_cursors, events_to_store, {:multi_success, from, response}}
    end
  end

  defp finalize_outcome({:single_success, _from, reply}, %Session{
         epoch: epoch,
         archived_seq: archived_seq
       }) do
    {:ok, finalize_success_outcome({:single_success, nil, reply}, epoch, archived_seq)}
  end

  defp finalize_outcome({:multi_success, _from, response}, %Session{} = session) do
    {:ok,
     finalize_success_outcome(
       {:multi_success, nil, response},
       session.epoch,
       session.archived_seq
     )}
  end

  defp finalize_outcome({:error, _from, reason}, _session), do: {:error, reason}

  defp finalize_failure_outcome({:error, _from, reason}, _error), do: {:error, reason}
  defp finalize_failure_outcome({_kind, _from, _payload}, {:error, _reason} = error), do: error

  defp finalize_success_outcome({:single_success, _from, reply}, epoch, committed_seq) do
    decorate_append_reply(reply, epoch, committed_seq)
  end

  defp finalize_success_outcome({:multi_success, _from, response}, epoch, committed_seq) do
    decorated_results =
      Enum.map(response.results, &decorate_append_reply(&1, epoch, committed_seq))

    %{
      results: decorated_results,
      last_seq: response.last_seq,
      epoch: epoch,
      cursor: %{epoch: epoch, seq: response.last_seq},
      committed_cursor: %{epoch: epoch, seq: committed_seq}
    }
  end

  defp outcome_from({_kind, from, _payload}), do: from

  defp commit_single_append(
         data,
         actions,
         from,
         next_session,
         next_cursors,
         [],
         outcome,
         _replicas
       ) do
    {:ok, publish_session} = commit_session(next_session, [])
    reply = finalize_success_outcome(outcome, publish_session.epoch, publish_session.archived_seq)

    {%{data | session: publish_session, producer_cursors: next_cursors},
     actions ++ [{:reply, from, {:ok, reply}}]}
  end

  defp commit_single_append(
         data,
         actions,
         from,
         next_session,
         next_cursors,
         events,
         outcome,
         replicas
       )
       when is_list(events) do
    case replicate_and_commit(data, next_session, events, replicas) do
      {:ok, publish_session} ->
        reply =
          finalize_success_outcome(outcome, publish_session.epoch, publish_session.archived_seq)

        {%{data | session: publish_session, producer_cursors: next_cursors},
         actions ++ [{:reply, from, {:ok, reply}}]}

      {:error, _reason} = error ->
        {data, actions ++ [{:reply, from, error}]}
    end
  end

  defp commit_append_group(data, actions, next_session, next_cursors, [], outcomes, _replicas) do
    {:ok, publish_session} = commit_session(next_session, [])

    reply_actions =
      Enum.map(outcomes, fn outcome ->
        {:reply, outcome_from(outcome), finalize_outcome(outcome, publish_session)}
      end)

    {%{data | session: publish_session, producer_cursors: next_cursors}, actions ++ reply_actions}
  end

  defp commit_append_group(
         data,
         actions,
         next_session,
         next_cursors,
         committed_events,
         outcomes,
         replicas
       )
       when is_list(committed_events) do
    case replicate_and_commit(data, next_session, committed_events, replicas) do
      {:ok, publish_session} ->
        reply_actions =
          Enum.map(outcomes, fn outcome ->
            {:reply, outcome_from(outcome), finalize_outcome(outcome, publish_session)}
          end)

        {%{data | session: publish_session, producer_cursors: next_cursors},
         actions ++ reply_actions}

      {:error, _reason} = error ->
        reply_actions =
          Enum.map(outcomes, fn outcome ->
            {:reply, outcome_from(outcome), finalize_failure_outcome(outcome, error)}
          end)

        {data, actions ++ reply_actions}
    end
  end

  defp replicate_and_commit(data, next_session, events, replicas)
       when is_map(data) and is_struct(next_session, Session) and is_list(events) do
    :ok =
      Telemetry.append_boundary(
        next_session.id,
        next_session.tenant_id,
        :before_quorum_replicate,
        length(events)
      )

    with :ok <- replicate_state(data, next_session, events, replicas) do
      commit_session(next_session, events)
    end
  end

  defp commit_session(%Session{} = next_session, events) when is_list(events) do
    publish_session = preserve_publication_watermark(next_session)
    :ok = put_appended_events(next_session.id, next_session.tenant_id, events)
    if events != [], do: :ok = publish_events(publish_session, events)
    :ok = SessionStore.put_session(publish_session)

    if events != [] do
      :ok =
        Telemetry.append_boundary(
          publish_session.id,
          publish_session.tenant_id,
          :after_commit_before_reply,
          length(events)
        )
    end

    {:ok, publish_session}
  end

  defp role_for_session(session_id) when is_binary(session_id) and session_id != "" do
    case SessionRouter.ensure_local_owner(session_id) do
      :ok -> :owner
      _other -> :follower
    end
  end

  defp cursor_snapshot(%Session{} = session) do
    %{
      epoch: session.epoch,
      last_seq: session.last_seq,
      committed_seq: session.archived_seq
    }
  end

  defp guard_expected_seq(_session, nil), do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0 and last_seq == expected_seq,
       do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0,
       do: {:error, {:expected_seq_conflict, expected_seq, last_seq}}

  defp guard_expected_seq(_session, _expected_seq), do: {:error, :invalid_event}

  defp append_to_session(%Session{} = session, producer_cursors, inputs)
       when is_map(producer_cursors) and is_list(inputs) and inputs != [] do
    do_append_to_session(session, producer_cursors, inputs, [], [])
  end

  defp append_to_session(%Session{}, _producer_cursors, []), do: {:error, :invalid_event}

  defp append_one_to_session(%Session{} = session, producer_cursors, input)
       when is_map(producer_cursors) and is_map(input) do
    with :ok <- guard_event_tenant(session, input) do
      case Session.append_event(session, producer_cursors, input) do
        {:appended, updated_session, updated_cursors, event} ->
          reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
          {:ok, updated_session, updated_cursors, reply, event}

        {:deduped, updated_session, updated_cursors, seq} ->
          reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
          {:ok, updated_session, updated_cursors, reply, nil}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp append_one_to_session(%Session{}, _producer_cursors, _input), do: {:error, :invalid_event}

  defp do_append_to_session(
         %Session{} = session,
         producer_cursors,
         [input | rest],
         replies,
         events
       ) do
    with :ok <- guard_event_tenant(session, input) do
      case Session.append_event(session, producer_cursors, input) do
        {:appended, updated_session, updated_cursors, event} ->
          reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}

          do_append_to_session(
            updated_session,
            updated_cursors,
            rest,
            [reply | replies],
            [event | events]
          )

        {:deduped, updated_session, updated_cursors, seq} ->
          reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
          do_append_to_session(updated_session, updated_cursors, rest, [reply | replies], events)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp do_append_to_session(%Session{} = session, producer_cursors, [], replies, events) do
    {:ok, session, producer_cursors, Enum.reverse(replies), Enum.reverse(events)}
  end

  defp normalize_session_epoch(%Session{epoch: epoch} = session)
       when is_integer(epoch) and epoch >= 0 do
    session
  end

  defp normalize_session_epoch(%Session{} = session), do: %Session{session | epoch: 0}

  defp apply_routing_epoch(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    normalized_session = normalize_session_epoch(session)
    fallback_epoch = normalize_epoch(normalized_session.epoch)
    routing_epoch = SessionRouter.local_owner_epoch(session_id, fallback_epoch)
    %Session{normalized_session | epoch: routing_epoch}
  end

  defp put_event_epoch(nil, _epoch), do: nil

  defp put_event_epoch(%{seq: seq} = event, epoch)
       when is_integer(seq) and seq > 0 and is_integer(epoch) and epoch >= 0 do
    Map.put(event, :epoch, epoch)
  end

  defp put_events_epoch(events, epoch)
       when is_list(events) and is_integer(epoch) and epoch >= 0 do
    Enum.map(events, &put_event_epoch(&1, epoch))
  end

  defp event_list(nil), do: []
  defp event_list(event) when is_map(event), do: [event]

  defp put_appended_events(_session_id, _tenant_id, []), do: :ok

  defp put_appended_events(session_id, tenant_id, [%{seq: seq} | _rest] = events)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(seq) and seq > 0 do
    EventStore.put_events(session_id, tenant_id, events)
  end

  defp publish_event(session_id, tenant_id, last_seq, %{seq: seq} = event)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(last_seq) and last_seq >= seq and is_integer(seq) and seq > 0 do
    message = CursorUpdate.message(session_id, tenant_id, event, last_seq)

    Phoenix.PubSub.broadcast(Starcite.PubSub, CursorUpdate.topic(session_id), message)
    Telemetry.cursor_update_emitted(session_id, tenant_id, seq, last_seq)
  end

  defp publish_events(%Session{id: session_id, tenant_id: tenant_id, last_seq: last_seq}, events)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(last_seq) and last_seq >= 0 and is_list(events) and events != [] do
    Enum.each(events, &publish_event(session_id, tenant_id, last_seq, &1))
    :ok
  end

  defp preserve_publication_watermark(
         %Session{id: session_id, tenant_id: tenant_id, epoch: epoch, archived_seq: archived_seq} =
           next_session
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(epoch) and epoch >= 0 and
              is_integer(archived_seq) and archived_seq >= 0 do
    case SessionStore.peek_session_cached(session_id) do
      {:ok, %Session{epoch: ^epoch, archived_seq: cached_archived_seq}}
      when is_integer(cached_archived_seq) and cached_archived_seq > archived_seq ->
        :ok =
          Telemetry.data_plane_invariant(
            session_id,
            tenant_id,
            :publish_events,
            :publication_watermark_regression
          )

        %Session{next_session | archived_seq: cached_archived_seq}

      _other ->
        next_session
    end
  end

  defp evict_archived_events(_session_id, previous_archived_seq, updated_archived_seq)
       when updated_archived_seq <= previous_archived_seq do
    0
  end

  defp evict_archived_events(session_id, _previous_archived_seq, updated_archived_seq) do
    EventStore.delete_below(session_id, updated_archived_seq + 1)
  end

  defp should_apply_replication?(%Session{} = incoming, %Session{} = current) do
    incoming_epoch = normalize_epoch(incoming.epoch)
    current_epoch = normalize_epoch(current.epoch)

    cond do
      incoming_epoch > current_epoch ->
        true

      incoming_epoch < current_epoch ->
        false

      incoming.last_seq > current.last_seq ->
        true

      incoming.last_seq < current.last_seq ->
        false

      incoming.archived_seq > current.archived_seq ->
        true

      incoming.archived_seq < current.archived_seq ->
        false

      true ->
        incoming != current
    end
  end

  defp normalize_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: epoch
  defp normalize_epoch(_epoch), do: 0

  defp decorate_append_reply(%{seq: seq} = reply, epoch, committed_seq)
       when is_integer(seq) and seq >= 0 and is_integer(epoch) and epoch >= 0 and
              is_integer(committed_seq) and committed_seq >= 0 do
    Map.merge(reply, %{
      epoch: epoch,
      cursor: %{epoch: epoch, seq: seq},
      committed_cursor: %{epoch: epoch, seq: committed_seq}
    })
  end

  defp guard_event_tenant(%Session{tenant_id: session_tenant_id}, %{metadata: metadata})
       when is_binary(session_tenant_id) and session_tenant_id != "" and is_map(metadata) do
    case event_principal_tenant_id(metadata) do
      nil -> :ok
      ^session_tenant_id -> :ok
      _other -> {:error, :forbidden_tenant}
    end
  end

  defp guard_event_tenant(%Session{}, _input), do: :ok

  defp event_principal_tenant_id(%{"starcite_principal" => %{"tenant_id" => tenant_id}})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_principal_tenant_id(%{"starcite_principal" => %{tenant_id: tenant_id}})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_principal_tenant_id(%{starcite_principal: %{"tenant_id" => tenant_id}})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_principal_tenant_id(%{starcite_principal: %{tenant_id: tenant_id}})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_principal_tenant_id(_metadata), do: nil

  defp activity_batch?(batch) when is_list(batch) do
    Enum.any?(batch, fn
      {:call, _from, message} -> activity_message?(message)
      _other -> false
    end)
  end

  # Idle detection is based on elapsed time, not `{epoch, seq}`. Read activity
  # can keep a session hot without advancing the session cursor.
  defp touch_activity(%{last_activity_mono_ms: _last} = data) do
    %{data | last_activity_mono_ms: now_monotonic_ms()}
  end

  # Only user-facing reads/writes keep a session resident. Internal quorum and
  # archiver calls must not pin a log in memory by accident.
  defp activity_message?(:get_session), do: true
  defp activity_message?(:fetch_cursor_snapshot), do: true
  defp activity_message?({:append_event, _input, _expected_seq, _replicas}), do: true
  defp activity_message?({:append_events, _inputs, _expected_seq, _replicas}), do: true
  defp activity_message?(_message), do: false

  defp should_freeze?(%{
         role: :owner,
         session: %Session{id: session_id, last_seq: last_seq, archived_seq: archived_seq},
         idle_timeout_ms: idle_timeout_ms,
         last_activity_mono_ms: last_activity_mono_ms
       })
       when is_binary(session_id) and session_id != "" and is_integer(last_seq) and
              is_integer(archived_seq) do
    idle_timeout_ms != :infinity and last_seq == archived_seq and
      idle_elapsed_ms(last_activity_mono_ms) >= idle_timeout_ms and
      SessionRouter.ensure_local_owner(session_id) == :ok
  end

  defp should_freeze?(_data), do: false

  defp freeze_session(%{session: %Session{} = session, role: role}) when role == :owner do
    :ok = emit_lifecycle(session, role, "session.freezing")
    :ok = Telemetry.session_freeze(session.id, session.tenant_id, :ok, :idle_timeout)
    :ok = emit_lifecycle(session, role, "session.frozen")
    {:stop, :normal}
  end

  defp now_monotonic_ms, do: System.monotonic_time(:millisecond)

  defp idle_elapsed_ms(last_activity_mono_ms)
       when is_integer(last_activity_mono_ms) do
    max(now_monotonic_ms() - last_activity_mono_ms, 0)
  end

  defp schedule_idle_tick(%{idle_timeout_ms: :infinity} = data) do
    cancel_idle_tick(data.idle_timer_ref)
    %{data | idle_timer_ref: nil}
  end

  defp schedule_idle_tick(
         %{idle_timeout_ms: idle_timeout_ms, idle_check_interval_ms: check_interval_ms} = data
       )
       when is_integer(idle_timeout_ms) and idle_timeout_ms > 0 and
              is_integer(check_interval_ms) and check_interval_ms > 0 do
    cancel_idle_tick(data.idle_timer_ref)

    idle_timer_ref =
      :erlang.start_timer(min(idle_timeout_ms, check_interval_ms), self(), :idle_tick)

    %{data | idle_timer_ref: idle_timer_ref}
  end

  defp cancel_idle_tick(idle_timer_ref) do
    if is_reference(idle_timer_ref) do
      _ = :erlang.cancel_timer(idle_timer_ref, [{:async, false}, {:info, false}])
    end

    :ok
  end

  defp validate_idle_timeout_ms!(:infinity), do: :infinity

  defp validate_idle_timeout_ms!(value) when is_integer(value) and value > 0 do
    value
  end

  defp validate_idle_timeout_ms!(value) do
    raise ArgumentError,
          "invalid value for :idle_timeout_ms: #{inspect(value)} " <>
            "(expected positive integer or :infinity)"
  end

  defp validate_idle_check_interval_ms!(value) when is_integer(value) and value > 0 do
    value
  end

  defp validate_idle_check_interval_ms!(value) do
    raise ArgumentError,
          "invalid value for :idle_check_interval_ms: #{inspect(value)} " <>
            "(expected positive integer)"
  end

  defp replicate_state(
         %{replicate_fun: replicate_fun},
         %Session{} = session,
         events,
         replicas
       )
       when is_function(replicate_fun, 3) and is_list(events) and is_list(replicas) do
    replicate_fun.(session, events, replicas)
  end

  defp emit_lifecycle(%Session{} = session, :owner, kind) when is_binary(kind) and kind != "" do
    Phoenix.PubSub.broadcast(
      Starcite.PubSub,
      "lifecycle:" <> session.tenant_id,
      {:session_lifecycle,
       %{
         kind: kind,
         session_id: session.id,
         tenant_id: session.tenant_id
       }}
    )
  end

  defp emit_lifecycle(%Session{}, _role, _kind), do: :ok
end
