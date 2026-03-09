defmodule Starcite.DataPlane.SessionOwners do
  @moduledoc """
  Session-owner process registry and call boundary.

  This module routes append/ack/query operations to one in-memory owner
  process per session.
  """

  alias Starcite.Routing.SessionRouter
  alias Starcite.DataPlane.{EventStore, SessionOwner, SessionStore}
  alias Starcite.Session

  @registry Starcite.DataPlane.SessionOwnerRegistry
  @supervisor Starcite.DataPlane.SessionOwnerSupervisor
  @call_timeout Application.compile_env(:starcite, :session_owner_call_timeout_ms, 2_000)
  @peer_bootstrap_rpc_timeout_ms Application.compile_env(
                                   :starcite,
                                   :session_owner_peer_bootstrap_rpc_timeout_ms,
                                   1_000
                                 )

  @spec start_session(Session.t()) :: :ok | {:error, :session_exists | term()}
  def start_session(%Session{} = session) do
    case lookup_owner(session.id) do
      {:ok, _pid} ->
        {:error, :session_exists}

      :error ->
        case start_owner(session) do
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
    owner_lookup = lookup_owner(session_id)

    with :ok <- ensure_local_owner(session_id),
         {:ok, pid} <- ensure_owner_loaded_from_lookup(session_id, owner_lookup),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, {:append_event, input, expected_seq})
    end
  end

  def append_event(_session_id, _input, _expected_seq), do: {:error, :invalid_event}

  @doc false
  @spec replicate_state(Session.t(), [map()]) :: :ok | {:error, term()} | {:timeout, term()}
  def replicate_state(%Session{id: session_id} = session, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    with {:ok, pid} <- ensure_owner_started(session) do
      case safe_call(pid, {:replicate_state, session, events}) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
        {:timeout, reason} -> {:timeout, reason}
        other -> {:error, {:invalid_replication_response, other}}
      end
    end
  end

  def replicate_state(_session, _events), do: {:error, :invalid_event}

  @spec append_events(String.t(), [map()], keyword()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def append_events(session_id, inputs, opts)
      when is_binary(session_id) and session_id != "" and is_list(inputs) and is_list(opts) do
    expected_seq = opts[:expected_seq]
    owner_lookup = lookup_owner(session_id)

    with :ok <- ensure_local_owner(session_id),
         {:ok, pid} <- ensure_owner_loaded_from_lookup(session_id, owner_lookup),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, {:append_events, inputs, expected_seq})
    end
  end

  def append_events(_session_id, _inputs, _opts), do: {:error, :invalid_event}

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(session_id, upto_seq)
      when is_binary(session_id) and session_id != "" and is_integer(upto_seq) and upto_seq >= 0 do
    owner_lookup = lookup_owner(session_id)

    with :ok <- ensure_local_owner(session_id),
         {:ok, pid} <- ensure_owner_loaded_from_lookup(session_id, owner_lookup),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, {:ack_archived, upto_seq})
    end
  end

  def ack_archived(_session_id, _upto_seq), do: {:error, :invalid_event}

  @spec get_session(String.t()) ::
          {:ok, Session.t()} | {:error, term()} | {:timeout, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, pid} <- ensure_owner_loaded(session_id),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, :get_session)
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
    owner_lookup = lookup_owner(session_id)

    with :ok <- ensure_local_owner(session_id),
         {:ok, pid} <- ensure_owner_loaded_from_lookup(session_id, owner_lookup),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, :fetch_cursor_snapshot)
    end
  end

  def fetch_cursor_snapshot(_session_id), do: {:error, :invalid_session_id}

  @spec fetch_archived_seq(String.t()) ::
          {:ok, non_neg_integer()} | {:error, term()} | {:timeout, term()}
  def fetch_archived_seq(session_id) when is_binary(session_id) and session_id != "" do
    with {:ok, pid} <- ensure_owner_loaded(session_id),
         {:ok, current_pid} <- ensure_owner_epoch_current(session_id, pid) do
      safe_owner_call(session_id, current_pid, :fetch_archived_seq)
    end
  end

  def fetch_archived_seq(_session_id), do: {:error, :invalid_session_id}

  @spec local_owner?(String.t()) :: boolean()
  def local_owner?(session_id) when is_binary(session_id) and session_id != "" do
    case lookup_owner(session_id) do
      {:ok, _pid} -> true
      :error -> false
    end
  end

  def local_owner?(_session_id), do: false

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
    case lookup_owner(session_id) do
      {:ok, pid} ->
        _ = DynamicSupervisor.terminate_child(@supervisor, pid)
        :ok

      :error ->
        :ok
    end
  end

  def stop_session(_session_id), do: :ok

  defp ensure_owner_loaded(session_id) when is_binary(session_id) and session_id != "" do
    ensure_owner_loaded_from_lookup(session_id, lookup_owner(session_id))
  end

  defp ensure_owner_loaded_from_lookup(session_id, {:ok, pid})
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    {:ok, pid}
  end

  defp ensure_owner_loaded_from_lookup(session_id, :error)
       when is_binary(session_id) and session_id != "" do
    case load_bootstrap_source(session_id) do
      {:ok, {:local, %Session{} = session}} ->
        start_owner_from_bootstrap(session_id, session)

      {:ok, {:peer, %Session{} = session, events}} ->
        with :ok <- prime_local_bootstrap(session, events) do
          start_owner_from_bootstrap(session_id, session)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_owner_started(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    case lookup_owner(session_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        case start_owner(session) do
          {:ok, pid} -> {:ok, pid}
          {:error, :session_exists} -> lookup_owner(session_id)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp lookup_owner(session_id) when is_binary(session_id) and session_id != "" do
    case Registry.lookup(@registry, session_id) do
      [{pid, _value} | _rest] when is_pid(pid) -> {:ok, pid}
      _ -> :error
    end
  end

  defp start_owner(%Session{} = session) do
    case DynamicSupervisor.start_child(@supervisor, {SessionOwner, session: session}) do
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

  defp start_owner_from_bootstrap(session_id, %Session{} = session)
       when is_binary(session_id) and session_id != "" do
    with {:ok, _pid} <- start_owner(session) do
      lookup_owner(session_id)
    else
      {:error, :session_exists} -> lookup_owner(session_id)
      {:error, reason} -> {:error, reason}
    end
  end

  defp safe_call(pid, message) when is_pid(pid) do
    GenServer.call(pid, message, @call_timeout)
  catch
    :exit, _reason -> {:timeout, :session_owner_unavailable}
  end

  defp safe_owner_call(session_id, pid, message)
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    case safe_call(pid, message) do
      {:timeout, :session_owner_unavailable} ->
        with {:ok, replacement_pid} <- ensure_owner_loaded(session_id) do
          safe_call(replacement_pid, message)
        end

      other ->
        other
    end
  end

  defp ensure_owner_epoch_current(session_id, pid)
       when is_binary(session_id) and session_id != "" and is_pid(pid) do
    case safe_call(pid, :get_session) do
      {:ok, %Session{} = session} ->
        current_epoch =
          SessionRouter.local_owner_epoch(session_id, normalize_epoch(session.epoch))

        if current_epoch > normalize_epoch(session.epoch) do
          restart_owner_for_epoch(session_id, session, current_epoch)
        else
          {:ok, pid}
        end

      {:timeout, :session_owner_unavailable} ->
        ensure_owner_loaded(session_id)

      other ->
        {:error, {:invalid_session_owner_state, other}}
    end
  end

  defp ensure_local_owner(session_id) when is_binary(session_id) and session_id != "" do
    SessionRouter.ensure_local_owner(session_id)
  end

  defp load_bootstrap_source(session_id) when is_binary(session_id) and session_id != "" do
    local_session = local_cached_session(session_id)
    freshest_peer = freshest_peer_bootstrap(session_id)

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

  defp peer_bootstrap_nodes(session_id) when is_binary(session_id) and session_id != "" do
    session_id
    |> SessionRouter.replica_nodes()
    |> Enum.reject(&(&1 == Node.self()))
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
        %{seq: seq} = event, previous_seq
        when is_integer(seq) and seq == previous_seq + 1 ->
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

  defp restart_owner_for_epoch(session_id, %Session{} = session, target_epoch)
       when is_binary(session_id) and session_id != "" and is_integer(target_epoch) and
              target_epoch >= 0 do
    case freshest_peer_bootstrap(session_id) do
      %{session: %Session{} = peer_session, events: events} when is_list(events) ->
        if fresher_session?(peer_session, session) do
          refreshed_session = session_with_epoch(peer_session, target_epoch)

          :ok = stop_session(session_id)
          :ok = prime_local_bootstrap(refreshed_session, events)
          start_owner_from_bootstrap(session_id, refreshed_session)
        else
          restart_owner_from_local_session(session_id, session, target_epoch)
        end

      _other ->
        restart_owner_from_local_session(session_id, session, target_epoch)
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

  defp restart_owner_from_local_session(session_id, %Session{} = session, target_epoch)
       when is_binary(session_id) and session_id != "" and is_integer(target_epoch) and
              target_epoch >= 0 do
    refreshed_session = session_with_epoch(session, target_epoch)

    :ok = stop_session(session_id)
    :ok = SessionStore.put_session(refreshed_session)
    start_owner_from_bootstrap(session_id, refreshed_session)
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
