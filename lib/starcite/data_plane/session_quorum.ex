defmodule Starcite.DataPlane.SessionQuorum do
  @moduledoc """
  Replica-set runtime for one session log.

  `SessionQuorum` is the boundary around per-session `SessionLog` processes.
  It is responsible for:

  - routing client commands to the local owner log
  - bootstrapping or restarting local logs when routing epochs change
  - replicating committed session state to standby logs and waiting for quorum
  """

  alias Starcite.Archive.SessionCatalog
  alias Starcite.DataPlane.{EventStore, SessionLog, SessionStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.Routing.{SessionRouter, Store}
  alias Starcite.Session

  @dialyzer {:nowarn_function,
             [
               append_event: 3,
               append_events: 3,
               ack_archived: 2,
               fetch_cursor_snapshot: 1
             ]}

  @registry Starcite.DataPlane.SessionLogRegistry
  @supervisor Starcite.DataPlane.SessionLogSupervisor
  @call_timeout Application.compile_env(:starcite, :session_log_call_timeout_ms, 2_000)
  @peer_bootstrap_rpc_timeout_ms Application.compile_env(
                                   :starcite,
                                   :session_log_peer_bootstrap_rpc_timeout_ms,
                                   1_000
                                 )
  @rpc_timeout_ms Application.compile_env(:starcite, :session_quorum_rpc_timeout_ms, 1_000)
  @max_concurrency Application.compile_env(:starcite, :session_quorum_max_concurrency, 16)

  @type replication_failure :: {node(), term()}
  @type routing_assignment :: %{
          required(:owner) => node(),
          required(:epoch) => non_neg_integer(),
          required(:replicas) => [node()]
        }
  @type replication_membership :: [node()] | %{required(:replicas) => [node()]}
  @type log_description :: %{
          required(:role) => SessionLog.role(),
          required(:session) => Session.t()
        }
  @type replication_error ::
          {:replication_quorum_not_met,
           %{
             required_remote_acks: non_neg_integer(),
             successful_remote_acks: non_neg_integer(),
             failures: [replication_failure()]
           }}

  @spec start_session(Session.t()) :: :ok | {:error, :session_exists | term()}
  def start_session(%Session{} = session) do
    case lookup_log(session.id) do
      {:ok, _pid} ->
        {:error, :session_exists}

      :error ->
        case start_log(session) do
          {:ok, _pid} -> :ok
          {:error, :session_exists} -> {:error, :session_exists}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @spec append_event(String.t(), map(), non_neg_integer() | nil) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_event(session_id, input, expected_seq)
      when is_binary(session_id) and session_id != "" and is_map(input) do
    log_lookup = lookup_log(session_id)

    with {:ok, assignment} <- require_local_owner_assignment(session_id),
         {:ok, pid} <- ensure_log_loaded_from_lookup(session_id, log_lookup),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid, assignment) do
      safe_log_call(
        session_id,
        current_pid,
        {:append_event, input, expected_seq, assignment.replicas}
      )
    end
  end

  def append_event(_session_id, _input, _expected_seq), do: {:error, :invalid_event}

  @spec append_events(String.t(), [map()], keyword()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_events(session_id, inputs, opts)
      when is_binary(session_id) and session_id != "" and is_list(inputs) and is_list(opts) do
    expected_seq = opts[:expected_seq]
    log_lookup = lookup_log(session_id)

    with {:ok, assignment} <- require_local_owner_assignment(session_id),
         {:ok, pid} <- ensure_log_loaded_from_lookup(session_id, log_lookup),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid, assignment) do
      safe_log_call(
        session_id,
        current_pid,
        {:append_events, inputs, expected_seq, assignment.replicas}
      )
    end
  end

  def append_events(_session_id, _inputs, _opts), do: {:error, :invalid_event}

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(session_id, upto_seq)
      when is_binary(session_id) and session_id != "" and is_integer(upto_seq) and upto_seq >= 0 do
    log_lookup = lookup_log(session_id)

    with {:ok, assignment} <- require_local_owner_assignment(session_id),
         {:ok, pid} <- ensure_log_loaded_from_lookup(session_id, log_lookup),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid, assignment) do
      safe_log_call(session_id, current_pid, {:ack_archived, upto_seq, assignment.replicas})
    end
  end

  def ack_archived(_session_id, _upto_seq), do: {:error, :invalid_event}

  @spec get_session(String.t()) ::
          {:ok, Session.t()} | {:error, term()} | {:timeout, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, pid} <- ensure_log_loaded(session_id),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid) do
      safe_log_call(session_id, current_pid, :get_session)
    end
  end

  def get_session(_session_id), do: {:error, :invalid_session_id}

  @spec query_session_with_epoch(String.t()) ::
          {:ok, %{session: Session.t(), epoch: non_neg_integer()}}
          | {:error, term()}
          | {:timeout, term()}
  def query_session_with_epoch(session_id)
      when is_binary(session_id) and session_id != "" do
    with {:ok, session} <- get_session(session_id) do
      {:ok, %{session: session, epoch: normalize_epoch(session.epoch)}}
    end
  end

  def query_session_with_epoch(_session_id), do: {:error, :invalid_session_id}

  @spec fetch_cursor_snapshot(String.t()) ::
          {:ok,
           %{
             epoch: non_neg_integer(),
             last_seq: non_neg_integer(),
             committed_seq: non_neg_integer()
           }}
          | {:error, term()}
          | {:timeout, term()}
  def fetch_cursor_snapshot(session_id) when is_binary(session_id) and session_id != "" do
    log_lookup = lookup_log(session_id)

    with {:ok, assignment} <- require_local_owner_assignment(session_id),
         {:ok, pid} <- ensure_log_loaded_from_lookup(session_id, log_lookup),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid, assignment) do
      safe_log_call(session_id, current_pid, :fetch_cursor_snapshot)
    end
  end

  def fetch_cursor_snapshot(_session_id), do: {:error, :invalid_session_id}

  @spec fetch_archived_seq(String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:timeout, term()}
  def fetch_archived_seq(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, pid} <- ensure_log_loaded(session_id),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid) do
      safe_log_call(session_id, current_pid, :fetch_archived_seq)
    end
  end

  def fetch_archived_seq(_session_id), do: {:error, :invalid_session_id}

  @doc false
  @spec apply_replica(Session.t(), [map()]) :: :ok | {:error, term()} | {:timeout, term()}
  def apply_replica(%Session{id: session_id} = session, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    with {:ok, pid} <- ensure_log_started(session) do
      case safe_call(pid, {:apply_replica, session, events}) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
        {:timeout, reason} -> {:timeout, reason}
        other -> {:error, {:invalid_replication_response, other}}
      end
    end
  end

  def apply_replica(_session, _events), do: {:error, :invalid_event}

  @spec replicate_state(Session.t(), [map()]) :: :ok | {:error, replication_error()}
  def replicate_state(%Session{} = session, events), do: replicate_state(session, events, nil)

  @spec replicate_state(Session.t(), [map()], replication_membership() | nil) ::
          :ok | {:error, replication_error()}
  def replicate_state(
        %Session{id: session_id, tenant_id: tenant_id} = session,
        events,
        assignment_or_replicas
      )
      when is_binary(session_id) and session_id != "" and is_list(events) do
    started_ms = System.monotonic_time(:millisecond)
    replicas = replicas_for_replication(session_id, assignment_or_replicas)
    local_node = Node.self()
    quorum_size = quorum_size(length(replicas))
    local_acks = if local_node in replicas, do: 1, else: 0
    required_remote_acks = max(quorum_size - local_acks, 0)
    standby_nodes = Enum.reject(replicas, &(&1 == local_node))
    standby_count = length(standby_nodes)

    {result, successful_remote_acks, failures} =
      case required_remote_acks do
        0 ->
          {:ok, 0, []}

        _other ->
          {successful_remote_acks, failures} =
            collect_remote_results_parallel(standby_nodes, session, events)

          finalize_quorum_result(required_remote_acks, successful_remote_acks, failures)
      end

    duration_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)
    outcome = if result == :ok, do: :ok, else: :quorum_not_met
    failure_reason = primary_failure_reason(failures)

    :ok =
      Telemetry.session_replication(
        session_id,
        tenant_id,
        outcome,
        duration_ms,
        standby_count,
        required_remote_acks,
        successful_remote_acks,
        length(failures),
        failure_reason
      )

    result
  end

  @spec local_owner?(String.t()) :: boolean()
  def local_owner?(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, pid} <- ensure_log_loaded(session_id),
         {:ok, current_pid} <- ensure_log_epoch_current(session_id, pid),
         {:ok, %{role: :owner}} <- describe_log(session_id, current_pid) do
      true
    else
      _other -> false
    end
  end

  def local_owner?(_session_id), do: false

  @spec handoff_snapshot(String.t()) ::
          {:ok, %{session: Session.t(), events: [map()]}} | {:error, term()} | {:timeout, term()}
  def handoff_snapshot(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, session} <- get_session(session_id) do
      tail_count = max(session.last_seq - session.archived_seq, 0)

      events =
        if tail_count == 0 do
          []
        else
          EventStore.from_cursor(session_id, session.archived_seq, tail_count)
        end

      {:ok, %{session: session, events: events}}
    end
  end

  def handoff_snapshot(_session_id), do: {:error, :invalid_session_id}

  @spec receive_handoff(Session.t(), [map()]) :: :ok | {:error, term()} | {:timeout, term()}
  def receive_handoff(%Session{} = session, events) when is_list(events) do
    apply_replica(session, events)
  end

  def receive_handoff(_session, _events), do: {:error, :invalid_event}

  @spec local_session_ids() :: [String.t()]
  def local_session_ids do
    case {Process.whereis(@supervisor), Process.whereis(@registry)} do
      {supervisor, registry} when is_pid(supervisor) and is_pid(registry) ->
        supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.flat_map(fn
          {_id, pid, _type, _modules} when is_pid(pid) -> Registry.keys(@registry, pid)
          _other -> []
        end)
        |> Enum.uniq()

      _other ->
        []
    end
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    case Process.whereis(@supervisor) do
      nil ->
        :ok

      _pid ->
        @supervisor
        |> DynamicSupervisor.which_children()
        |> Enum.each(fn
          {_id, pid, _type, _modules} when is_pid(pid) ->
            _ = DynamicSupervisor.terminate_child(@supervisor, pid)
            :ok

          _other ->
            :ok
        end)
    end

    :ok
  end

  @doc false
  @spec stop_session(String.t()) :: :ok
  def stop_session(session_id) when is_binary(session_id) and session_id != "" do
    case lookup_log(session_id) do
      {:ok, pid} ->
        _ = DynamicSupervisor.terminate_child(@supervisor, pid)
        :ok

      :error ->
        :ok
    end
  end

  def stop_session(_session_id), do: :ok

  defp ensure_log_loaded(session_id) when is_binary(session_id) and session_id != "" do
    ensure_log_loaded_from_lookup(session_id, lookup_log(session_id))
  end

  defp ensure_log_loaded_from_lookup(session_id, {:ok, pid})
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    if Process.alive?(pid) do
      {:ok, pid}
    else
      ensure_log_loaded_from_lookup(session_id, :error)
    end
  end

  defp ensure_log_loaded_from_lookup(session_id, :error)
       when is_binary(session_id) and session_id != "" do
    case load_bootstrap_source(session_id) do
      {:ok, {:local, %Session{} = session}} ->
        start_log_from_bootstrap(session_id, session)

      {:ok, {:peer, %Session{} = session, events}} ->
        with :ok <- prime_local_bootstrap(session, events) do
          start_log_from_bootstrap(session_id, session)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_log_started(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    case lookup_log(session_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        case start_log(session) do
          {:ok, pid} -> {:ok, pid}
          {:error, :session_exists} -> lookup_log(session_id)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp lookup_log(session_id) when is_binary(session_id) and session_id != "" do
    case Registry.lookup(@registry, session_id) do
      [{pid, _value} | _rest] when is_pid(pid) ->
        if Process.alive?(pid), do: {:ok, pid}, else: :error

      _ ->
        :error
    end
  end

  defp start_log(%Session{} = session) do
    case DynamicSupervisor.start_child(@supervisor, {SessionLog, session: session}) do
      {:ok, pid} when is_pid(pid) ->
        {:ok, pid}

      {:error, {:already_started, pid}} when is_pid(pid) ->
        {:ok, pid}

      {:error, {:already_present, _pid}} ->
        {:error, :session_exists}

      {:error, :already_present} ->
        {:error, :session_exists}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp start_log_from_bootstrap(session_id, %Session{} = session)
       when is_binary(session_id) and session_id != "" do
    with {:ok, _pid} <- start_log(session) do
      lookup_log(session_id)
    else
      {:error, :session_exists} -> lookup_log(session_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_call(pid, message) when is_pid(pid) do
    :gen_batch_server.call(pid, message, @call_timeout)
  catch
    :exit, reason -> normalize_call_exit(reason)
  end

  defp safe_log_call(session_id, pid, message)
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    case safe_call(pid, message) do
      {:error, :session_log_unavailable} ->
        with {:ok, replacement_pid} <- ensure_log_loaded(session_id) do
          safe_call(replacement_pid, message)
        end

      other ->
        other
    end
  end

  defp normalize_call_exit({reason, {:gen_batch_server, :call, _args}}) do
    normalize_call_exit(reason)
  end

  defp normalize_call_exit({:timeout, _details}), do: {:timeout, :session_log_call_timeout}
  defp normalize_call_exit(:timeout), do: {:timeout, :session_log_call_timeout}
  defp normalize_call_exit(:noproc), do: {:error, :session_log_unavailable}
  defp normalize_call_exit({:noproc, _details}), do: {:error, :session_log_unavailable}
  defp normalize_call_exit(:normal), do: {:error, :session_log_unavailable}
  defp normalize_call_exit(:shutdown), do: {:error, :session_log_unavailable}
  defp normalize_call_exit({:shutdown, _details}), do: {:error, :session_log_unavailable}
  defp normalize_call_exit(_reason), do: {:timeout, :session_log_call_timeout}

  defp ensure_log_epoch_current(session_id, pid, assignment \\ nil)
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    with {:ok, %{role: role, session: %Session{} = session}} <- describe_log(session_id, pid) do
      current_epoch = effective_epoch(session_id, session, assignment)
      desired_role = desired_role(session_id, assignment)

      cond do
        current_epoch > normalize_epoch(session.epoch) or desired_role != role ->
          restart_log_for_epoch(session_id, session, current_epoch)

        follower_refresh_needed?(session_id, role, desired_role, session) ->
          restart_log_for_epoch(session_id, session, current_epoch)

        true ->
          {:ok, pid}
      end
    else
      {:timeout, :session_log_unavailable} ->
        ensure_log_loaded(session_id)

      other ->
        {:error, {:invalid_session_log_state, other}}
    end
  end

  defp describe_log(session_id, pid)
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    case safe_log_call(session_id, pid, :describe) do
      {:ok, %{role: role, session: %Session{} = session}}
      when role in [:owner, :follower] ->
        {:ok, %{role: role, session: session}}

      {:error, reason} ->
        {:error, reason}

      {:timeout, reason} ->
        {:timeout, reason}

      other ->
        {:error, {:invalid_log_description, other}}
    end
  end

  @spec require_local_owner_assignment(String.t()) ::
          {:ok, routing_assignment()} | {:error, term()}
  defp require_local_owner_assignment(session_id)
       when is_binary(session_id) and session_id != "" do
    local_node = Node.self()

    case local_owner_assignment(session_id, :low_latency) do
      {:ok, %{owner: owner}} when owner == local_node ->
        local_owner_assignment(session_id, :consistency)

      {:ok, assignment} ->
        {:ok, assignment}

      {:error, {:not_leader, _reason}} ->
        local_owner_assignment(session_id, :consistency)

      {:error, :ownership_transfer_in_progress} = error ->
        error

      {:error, :not_found} = error ->
        error

      {:error, reason} ->
        case local_owner_assignment(session_id, :consistency) do
          {:ok, assignment} -> {:ok, assignment}
          _other -> {:error, reason}
        end
    end
  end

  defp local_owner_assignment(session_id, favor)
       when is_binary(session_id) and session_id != "" and favor in [:low_latency, :consistency] do
    with {:ok, assignment} <- Store.get_assignment(session_id, favor: favor),
         :ok <- SessionRouter.ensure_local_owner(session_id, assignment: assignment) do
      {:ok, assignment}
    end
  end

  defp effective_epoch(session_id, %Session{} = session, nil)
       when is_binary(session_id) and session_id != "" do
    SessionRouter.local_owner_epoch(session_id, normalize_epoch(session.epoch))
  end

  defp effective_epoch(session_id, %Session{} = session, assignment)
       when is_binary(session_id) and session_id != "" do
    SessionRouter.local_owner_epoch(
      session_id,
      normalize_epoch(session.epoch),
      assignment: assignment
    )
  end

  defp desired_role(session_id, nil) when is_binary(session_id) and session_id != "" do
    case SessionRouter.ensure_local_owner(session_id) do
      :ok -> :owner
      _other -> :follower
    end
  end

  defp desired_role(session_id, assignment)
       when is_binary(session_id) and session_id != "" do
    case SessionRouter.ensure_local_owner(session_id, assignment: assignment) do
      :ok -> :owner
      _other -> :follower
    end
  end

  defp follower_refresh_needed?(session_id, :follower, :follower, %Session{} = session)
       when is_binary(session_id) and session_id != "" do
    case freshest_peer_bootstrap(session_id) do
      %{session: %Session{} = peer_session} ->
        fresher_session?(peer_session, session)

      _other ->
        false
    end
  end

  defp follower_refresh_needed?(_session_id, _role, _desired_role, _session), do: false

  defp load_bootstrap_source(session_id) when is_binary(session_id) and session_id != "" do
    local_session = local_cached_session(session_id)
    freshest_peer = eligible_peer_bootstrap(session_id)

    cond do
      freshest_peer == nil and local_session == nil ->
        {:error, :session_not_found}

      freshest_peer == nil ->
        {:ok, {:local, local_session}}

      local_session == nil ->
        {:ok, {:peer, freshest_peer.session, freshest_peer.events}}

      fresher_session?(freshest_peer.session, local_session) ->
        {:ok, {:peer, freshest_peer.session, freshest_peer.events}}

      true ->
        {:ok, {:local, local_session}}
    end
  end

  defp local_cached_session(session_id) when is_binary(session_id) and session_id != "" do
    case SessionStore.get_session_cached(session_id) do
      {:ok, %Session{} = session} -> session
      :error -> nil
    end
  end

  defp freshest_peer_bootstrap(session_id) when is_binary(session_id) and session_id != "" do
    session_id
    |> peer_bootstrap_nodes()
    |> Enum.reduce(nil, fn peer_node, freshest ->
      case fetch_peer_bootstrap(peer_node, session_id) do
        {:ok, snapshot} -> choose_fresher_snapshot(snapshot, freshest)
        _other -> freshest
      end
    end)
  end

  defp eligible_peer_bootstrap(session_id) when is_binary(session_id) and session_id != "" do
    case freshest_peer_bootstrap(session_id) do
      %{session: %Session{last_seq: 0, archived_seq: 0}} = snapshot ->
        case SessionCatalog.get_session(session_id) do
          {:ok, %Session{}} -> snapshot
          _other -> nil
        end

      snapshot ->
        snapshot
    end
  end

  defp peer_bootstrap_nodes(session_id) when is_binary(session_id) and session_id != "" do
    case Store.get_assignment(session_id, favor: :consistency) do
      {:ok, %{replicas: replicas}} when is_list(replicas) ->
        Enum.reject(replicas, &(&1 == Node.self()))

      _other ->
        []
    end
  end

  defp fetch_peer_bootstrap(peer_node, session_id)
       when is_atom(peer_node) and is_binary(session_id) and session_id != "" do
    case :rpc.call(
           peer_node,
           SessionStore,
           :get_session_cached,
           [session_id],
           @peer_bootstrap_rpc_timeout_ms
         ) do
      {:ok, %Session{} = session} ->
        with {:ok, events} <- fetch_peer_tail(peer_node, session) do
          {:ok, %{node: peer_node, session: session, events: events}}
        end

      :error ->
        :error

      {:badrpc, _reason} = error ->
        {:error, error}

      other ->
        {:error, {:invalid_peer_session, other}}
    end
  end

  defp fetch_peer_tail(_peer_node, %Session{last_seq: last_seq, archived_seq: archived_seq})
       when is_integer(last_seq) and is_integer(archived_seq) and last_seq <= archived_seq do
    {:ok, []}
  end

  defp fetch_peer_tail(
         peer_node,
         %Session{id: session_id, last_seq: last_seq, archived_seq: archived_seq} = session
       )
       when is_atom(peer_node) and is_binary(session_id) and session_id != "" and
              is_integer(last_seq) and last_seq > archived_seq and is_integer(archived_seq) and
              archived_seq >= 0 do
    tail_count = last_seq - archived_seq

    case :rpc.call(
           peer_node,
           EventStore,
           :from_cursor,
           [session_id, archived_seq, tail_count],
           @peer_bootstrap_rpc_timeout_ms
         ) do
      events when is_list(events) ->
        if valid_peer_tail?(events, session) do
          {:ok, events}
        else
          {:error, :invalid_peer_tail}
        end

      {:badrpc, _reason} = error ->
        {:error, error}

      other ->
        {:error, {:invalid_peer_tail, other}}
    end
  end

  defp valid_peer_tail?(
         events,
         %Session{last_seq: last_seq, archived_seq: archived_seq, epoch: epoch}
       )
       when is_list(events) and is_integer(last_seq) and is_integer(archived_seq) and
              is_integer(epoch) do
    expected_count = last_seq - archived_seq

    length(events) == expected_count and
      Enum.reduce_while(events, archived_seq, fn
        %{seq: seq} = event, previous_seq when is_integer(seq) and seq == previous_seq + 1 ->
          case Map.get(event, :epoch) do
            nil ->
              {:cont, seq}

            ^epoch ->
              {:cont, seq}

            _other ->
              {:halt, :invalid}
          end

        _event, _previous_seq ->
          {:halt, :invalid}
      end) == last_seq
  end

  defp choose_fresher_snapshot(snapshot, nil) when is_map(snapshot), do: snapshot

  defp choose_fresher_snapshot(
         %{session: %Session{} = incoming} = snapshot,
         %{session: %Session{} = current} = current_snapshot
       ) do
    if fresher_session?(incoming, current), do: snapshot, else: current_snapshot
  end

  defp restart_log_for_epoch(session_id, %Session{} = session, target_epoch)
       when is_binary(session_id) and session_id != "" and is_integer(target_epoch) and
              target_epoch >= 0 do
    case freshest_peer_bootstrap(session_id) do
      %{session: %Session{} = peer_session, events: events} when is_list(events) ->
        if fresher_session?(peer_session, session) do
          refreshed_session = session_with_epoch(peer_session, target_epoch)

          :ok = stop_session(session_id)
          :ok = prime_local_bootstrap(refreshed_session, events)
          start_log_from_bootstrap(session_id, refreshed_session)
        else
          restart_log_from_local_session(session_id, session, target_epoch)
        end

      _other ->
        restart_log_from_local_session(session_id, session, target_epoch)
    end
  end

  defp prime_local_bootstrap(%Session{} = session, events) when is_list(events) do
    :ok = SessionStore.put_session(session)
    :ok = replace_local_pending_events(session, events)
    :ok
  end

  defp session_with_epoch(%Session{} = session, epoch)
       when is_integer(epoch) and epoch >= 0 do
    %Session{session | epoch: max(normalize_epoch(session.epoch), epoch)}
  end

  defp restart_log_from_local_session(session_id, %Session{} = session, target_epoch)
       when is_binary(session_id) and session_id != "" and is_integer(target_epoch) and
              target_epoch >= 0 do
    refreshed_session = session_with_epoch(session, target_epoch)

    :ok = stop_session(session_id)
    :ok = SessionStore.put_session(refreshed_session)
    start_log_from_bootstrap(session_id, refreshed_session)
  end

  defp replace_local_pending_events(%Session{id: session_id, tenant_id: tenant_id}, events)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_list(events) do
    :ok = clear_local_pending_events(session_id)

    case events do
      [] -> :ok
      _events -> EventStore.put_events(session_id, tenant_id, events)
    end
  end

  defp clear_local_pending_events(session_id) when is_binary(session_id) and session_id != "" do
    case EventStore.max_seq(session_id) do
      {:ok, max_seq} when is_integer(max_seq) and max_seq > 0 ->
        _deleted = EventStore.delete_below(session_id, max_seq + 1)
        :ok

      :error ->
        :ok
    end
  end

  defp collect_remote_results_parallel(standby_nodes, session, events)
       when is_list(standby_nodes) and is_struct(session, Session) and is_list(events) do
    case standby_nodes do
      [] ->
        {0, []}

      [standby_node] ->
        case replicate_to_standby(standby_node, session, events) do
          :ok ->
            {1, []}

          {:error, reason} ->
            {0, [{standby_node, {:error, reason}}]}
        end

      [first_node, second_node] ->
        collect_two_remote_results(first_node, second_node, session, events)

      _other ->
        standby_nodes
        |> Task.async_stream(
          fn standby_node ->
            {standby_node, replicate_to_standby(standby_node, session, events)}
          end,
          ordered: false,
          timeout: @rpc_timeout_ms,
          on_timeout: :kill_task,
          max_concurrency: max_concurrency(length(standby_nodes))
        )
        |> Enum.reduce({0, []}, fn result, {acks, failures} ->
          case result_to_ack_result(result) do
            {:ack, _node} ->
              {acks + 1, failures}

            {:fail, failure} ->
              {acks, [failure | failures]}
          end
        end)
    end
  end

  defp collect_two_remote_results(first_node, second_node, session, events)
       when is_atom(first_node) and is_atom(second_node) and is_struct(session, Session) and
              is_list(events) do
    tasks = [
      {first_node, Task.async(fn -> replicate_to_standby(first_node, session, events) end)},
      {second_node, Task.async(fn -> replicate_to_standby(second_node, session, events) end)}
    ]

    Enum.reduce(tasks, {0, []}, fn {standby_node, task}, {acks, failures} ->
      case Task.yield(task, @rpc_timeout_ms) || Task.shutdown(task, :brutal_kill) do
        {:ok, :ok} ->
          {acks + 1, failures}

        {:ok, {:error, reason}} ->
          {acks, [{standby_node, {:error, reason}} | failures]}

        {:exit, reason} ->
          {acks, [{standby_node, {:error, {:task_exit, reason}}} | failures]}

        nil ->
          {acks, [{standby_node, {:error, {:task_timeout, @rpc_timeout_ms}}} | failures]}
      end
    end)
  end

  defp replicate_to_standby(standby_node, session, events)
       when is_atom(standby_node) and is_struct(session, Session) and is_list(events) do
    case :rpc.call(
           standby_node,
           __MODULE__,
           :apply_replica,
           [session, events],
           @rpc_timeout_ms
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}

      {:timeout, reason} ->
        {:error, {:timeout, reason}}

      {:badrpc, reason} ->
        {:error, {:badrpc, reason}}

      other ->
        {:error, {:unexpected_response, other}}
    end
  end

  defp result_to_ack_result({:ok, {standby_node, :ok}}) when is_atom(standby_node) do
    {:ack, standby_node}
  end

  defp result_to_ack_result({:ok, {standby_node, {:error, reason}}})
       when is_atom(standby_node) do
    {:fail, {standby_node, {:error, reason}}}
  end

  defp result_to_ack_result({:exit, reason}) do
    {:fail, {:unknown, {:error, {:task_exit, reason}}}}
  end

  defp result_to_ack_result(other) do
    {:fail, {:unknown, {:error, {:task_error, other}}}}
  end

  defp quorum_size(replica_count) when is_integer(replica_count) and replica_count > 0 do
    div(replica_count, 2) + 1
  end

  defp quorum_size(_replica_count), do: 1

  defp finalize_quorum_result(required_remote_acks, successful_remote_acks, failures)
       when is_integer(required_remote_acks) and required_remote_acks >= 0 and
              is_integer(successful_remote_acks) and successful_remote_acks >= 0 and
              is_list(failures) do
    if successful_remote_acks >= required_remote_acks do
      {:ok, successful_remote_acks, failures}
    else
      error =
        {:error,
         {:replication_quorum_not_met,
          %{
            required_remote_acks: required_remote_acks,
            successful_remote_acks: successful_remote_acks,
            failures: Enum.reverse(failures)
          }}}

      {error, successful_remote_acks, failures}
    end
  end

  defp max_concurrency(standby_count)
       when is_integer(standby_count) and standby_count >= 0 do
    configured =
      case @max_concurrency do
        value when is_integer(value) and value > 0 -> value
        _ -> 1
      end

    max(1, min(configured, max(standby_count, 1)))
  end

  defp primary_failure_reason([]), do: :none

  defp primary_failure_reason([{_node, {:error, {:badrpc, _reason}}} | _]), do: :badrpc
  defp primary_failure_reason([{_node, {:error, {:timeout, _reason}}} | _]), do: :timeout

  defp primary_failure_reason([{_node, {:error, {:unexpected_response, _response}}} | _]),
    do: :unexpected_response

  defp primary_failure_reason([{_node, {:error, {:task_exit, _reason}}} | _]), do: :task_exit
  defp primary_failure_reason([{_node, {:error, {:task_error, _reason}}} | _]), do: :task_error
  defp primary_failure_reason([{_node, {:error, reason}} | _]) when is_atom(reason), do: reason
  defp primary_failure_reason([{_node, {:error, _reason}} | _]), do: :error
  defp primary_failure_reason([{_node, {:badrpc, _reason}} | _]), do: :badrpc
  defp primary_failure_reason([{_node, reason} | _]) when is_atom(reason), do: reason
  defp primary_failure_reason([_ | _]), do: :error

  defp replicas_for_replication(_session_id, %{replicas: replicas})
       when is_list(replicas) and replicas != [] do
    replicas
  end

  defp replicas_for_replication(_session_id, replicas)
       when is_list(replicas) and replicas != [] do
    replicas
  end

  defp replicas_for_replication(session_id, _assignment_or_replicas)
       when is_binary(session_id) and session_id != "" do
    SessionRouter.replica_nodes(session_id)
  end

  defp fresher_session?(%Session{} = incoming, %Session{} = current) do
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

      true ->
        false
    end
  end

  defp normalize_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: epoch
  defp normalize_epoch(_epoch), do: 0
end
