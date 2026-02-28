defmodule Starcite.DataPlane.ReplicaRouter do
  @moduledoc """
  Replica-aware request routing for write/read path operations.

  Uses write-node liveness snapshots and short-lived leader hints to choose
  execution targets. This module does not mutate Raft membership.
  """

  require Logger

  alias Starcite.ControlPlane.Observer
  alias Starcite.DataPlane.RaftManager

  @rpc_timeout Application.compile_env(:starcite, :rpc_timeout_ms, 5_000)
  @leader_cache_table :starcite_replica_router_leader_cache
  @leader_cache_ttl_ms Application.compile_env(:starcite, :route_leader_cache_ttl_ms, 10_000)
  @leader_probe_timeout_ms 50

  @spec call_on_replica(
          non_neg_integer(),
          module(),
          atom(),
          [term()],
          module(),
          atom(),
          [term()],
          keyword()
        ) :: term()
  def call_on_replica(
        group_id,
        remote_module,
        remote_fun,
        remote_args,
        local_module,
        local_fun,
        local_args,
        route_opts \\ []
      )
      when is_integer(group_id) and group_id >= 0 and is_atom(remote_module) and
             is_atom(remote_fun) and is_list(remote_args) and is_atom(local_module) and
             is_atom(local_fun) and is_list(local_args) and is_list(route_opts) do
    target = route_target(group_id, route_opts)

    case target do
      {:local, _node} ->
        apply(local_module, local_fun, local_args)

      {:remote, []} ->
        {:error, {:no_available_replicas, []}}

      {:remote, nodes} ->
        allowed_nodes = MapSet.new(nodes)

        {result, _stats} =
          try_remote(
            nodes,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            %{},
            [],
            %{attempts: 0, leader_redirects: 0}
          )

        result
    end
  end

  @doc false
  @spec route_target(non_neg_integer(), keyword()) :: {:local, node()} | {:remote, [node()]}
  def route_target(group_id, opts \\ []) when is_integer(group_id) and group_id >= 0 do
    {target, _meta} = route_target_with_meta(group_id, opts)
    target
  end

  defp route_target_with_meta(group_id, opts) when is_integer(group_id) and group_id >= 0 do
    ensure_leader_cache_table()

    self_node = Keyword.get(opts, :self, Node.self())
    replicas = Keyword.get(opts, :replicas, RaftManager.replicas_for_group(group_id))

    route_candidates =
      case Keyword.fetch(opts, :route_candidates) do
        {:ok, candidates} when is_map(candidates) ->
          candidates

        _ ->
          case Keyword.fetch(opts, :ready_nodes) do
            {:ok, ready_nodes} when is_list(ready_nodes) ->
              %{ready: ready_nodes, fallbacks: replicas -- ready_nodes}

            _ ->
              Observer.route_candidates(replicas)
          end
      end

    ready_nodes = Map.get(route_candidates, :ready, [])
    fallback_nodes = Map.get(route_candidates, :fallbacks, [])
    local_running = Keyword.get(opts, :local_running, group_running?(group_id))
    allow_local = Keyword.get(opts, :allow_local, true)
    prefer_leader = Keyword.get(opts, :prefer_leader, false)
    now_ms = System.monotonic_time(:millisecond)

    remote_replicas =
      if local_running and allow_local do
        replicas
      else
        replicas -- [self_node]
      end

    leader_hint =
      if prefer_leader do
        resolve_leader_hint(group_id, self_node, now_ms, replicas)
      else
        nil
      end

    leader_hint_status = leader_hint_status(prefer_leader, leader_hint)
    replica_count = length(replicas)
    ready_count = Enum.count(replicas, &(&1 in ready_nodes))

    target =
      cond do
        prefer_leader and node_hint?(leader_hint) and leader_hint == self_node and local_running and
          allow_local and self_node in ready_nodes ->
          {:local, self_node}

        self_node in replicas and remote_replicas == [] and allow_local and
            (self_node in ready_nodes or self_node in fallback_nodes) ->
          {:local, self_node}

        prefer_leader ->
          {:remote,
           remote_candidates_with_leader_first(
             remote_replicas,
             ready_nodes,
             fallback_nodes,
             leader_hint
           )}

        self_node in replicas and local_running and allow_local and self_node in ready_nodes ->
          {:local, self_node}

        true ->
          ready_candidates = Enum.filter(remote_replicas, &(&1 in ready_nodes))
          fallbacks = Enum.filter(remote_replicas, &(&1 in fallback_nodes))

          {:remote, Enum.uniq(ready_candidates ++ fallbacks)}
      end

    meta = %{
      target: target_to_atom(target),
      prefer_leader: prefer_leader,
      leader_hint: leader_hint_status,
      replica_count: replica_count,
      ready_count: ready_count
    }

    {target, meta}
  end

  defp try_remote(
         [],
         _remote_module,
         _remote_fun,
         _remote_args,
         _allowed_nodes,
         _visited,
         failures,
         stats
       ) do
    {{:error, {:no_available_replicas, Enum.reverse(failures)}}, stats}
  end

  defp try_remote(
         [node | rest],
         remote_module,
         remote_fun,
         remote_args,
         allowed_nodes,
         visited,
         failures,
         stats
       ) do
    if Map.has_key?(visited, node) do
      try_remote(
        rest,
        remote_module,
        remote_fun,
        remote_args,
        allowed_nodes,
        visited,
        failures,
        stats
      )
    else
      visited = Map.put(visited, node, true)
      stats = %{stats | attempts: stats.attempts + 1}

      case safe_rpc_call(node, remote_module, remote_fun, remote_args) do
        {:badrpc, reason} ->
          Logger.warning("ReplicaRouter RPC to #{inspect(node)} failed: #{inspect(reason)}")

          try_remote(
            rest,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            visited,
            [{node, {:badrpc, reason}} | failures],
            stats
          )

        {:error, {:not_leader, leader}} ->
          maybe_cache_leader_hint(leader)
          rest = maybe_enqueue_leader(leader, rest, visited, allowed_nodes)
          stats = bump_leader_redirects(stats)

          try_remote(
            rest,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            visited,
            [{node, {:not_leader, leader}} | failures],
            stats
          )

        {:error, :not_leader} ->
          try_remote(
            rest,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            visited,
            [{node, :not_leader} | failures],
            stats
          )

        {:error, {:timeout, leader}} ->
          maybe_cache_leader_hint(leader)
          rest = maybe_enqueue_leader(leader, rest, visited, allowed_nodes)
          stats = bump_leader_redirects(stats)

          try_remote(
            rest,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            visited,
            [{node, {:timeout, leader}} | failures],
            stats
          )

        {:timeout, leader} ->
          maybe_cache_leader_hint(leader)
          rest = maybe_enqueue_leader(leader, rest, visited, allowed_nodes)
          stats = bump_leader_redirects(stats)

          try_remote(
            rest,
            remote_module,
            remote_fun,
            remote_args,
            allowed_nodes,
            visited,
            [{node, {:timeout, leader}} | failures],
            stats
          )

        {:error, reason} ->
          if retriable_error?(reason) do
            Logger.warning("ReplicaRouter RPC to #{inspect(node)} errored: #{inspect(reason)}")

            try_remote(
              rest,
              remote_module,
              remote_fun,
              remote_args,
              allowed_nodes,
              visited,
              [{node, {:error, reason}} | failures],
              stats
            )
          else
            {{:error, reason}, stats}
          end

        other ->
          {other, stats}
      end
    end
  end

  defp retriable_error?(reason)
       when reason in [
              :invalid_session,
              :invalid_session_id,
              :invalid_event,
              :invalid_cursor,
              :session_not_found,
              :session_exists,
              :producer_replay_conflict,
              :event_store_backpressure
            ] do
    false
  end

  defp retriable_error?({:expected_seq_conflict, _current}), do: false
  defp retriable_error?({:expected_seq_conflict, _expected, _current}), do: false
  defp retriable_error?({:producer_seq_conflict, _producer_id, _expected, _got}), do: false
  defp retriable_error?(_reason), do: true

  defp safe_rpc_call(node, remote_module, remote_fun, remote_args) do
    :rpc.call(node, remote_module, remote_fun, remote_args, @rpc_timeout)
  catch
    :exit, reason ->
      {:badrpc, reason}
  end

  defp maybe_enqueue_leader({server_id, leader_node}, rest, visited, allowed_nodes)
       when is_atom(server_id) and not is_nil(server_id) and is_atom(leader_node) and
              not is_nil(leader_node) and is_map(allowed_nodes) do
    cond do
      not MapSet.member?(allowed_nodes, leader_node) -> rest
      Map.has_key?(visited, leader_node) -> rest
      Enum.member?(rest, leader_node) -> rest
      true -> [leader_node | rest]
    end
  end

  defp maybe_enqueue_leader(_leader, rest, _visited, _allowed_nodes), do: rest

  defp group_running?(group_id) do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp remote_candidates_with_leader_first(
         remote_replicas,
         ready_nodes,
         fallback_nodes,
         leader_hint
       ) do
    ready_candidates = Enum.filter(remote_replicas, &(&1 in ready_nodes))
    fallbacks = Enum.filter(remote_replicas, &(&1 in fallback_nodes))

    leader_first =
      case leader_hint do
        node when is_atom(node) and not is_nil(node) ->
          if node in ready_candidates, do: [node], else: []

        _ ->
          []
      end

    remainder_ready = ready_candidates -- leader_first
    remainder_fallbacks = fallbacks -- leader_first

    Enum.uniq(leader_first ++ remainder_ready ++ remainder_fallbacks)
  end

  defp resolve_leader_hint(group_id, self_node, now_ms, replicas) do
    case cached_leader_hint(group_id, now_ms) do
      node when is_atom(node) and not is_nil(node) ->
        if Enum.member?(replicas, node), do: node, else: nil

      _ ->
        maybe_probe_leader_hint(group_id, self_node, now_ms, replicas)
    end
  end

  defp maybe_probe_leader_hint(group_id, self_node, now_ms, replicas) do
    if leader_probe_on_miss?() do
      case probe_leader(group_id, self_node) do
        node when is_atom(node) and not is_nil(node) ->
          if Enum.member?(replicas, node) do
            put_leader_hint(group_id, node, now_ms)
            node
          else
            nil
          end

        _ ->
          nil
      end
    else
      nil
    end
  end

  defp leader_probe_on_miss? do
    Application.get_env(:starcite, :route_leader_probe_on_miss, false) == true
  end

  defp probe_leader(group_id, self_node) when is_integer(group_id) and is_atom(self_node) do
    server_id = RaftManager.server_id(group_id)

    case :ra.members({server_id, self_node}, @leader_probe_timeout_ms) do
      {:ok, _members, {^server_id, leader_node}} when is_atom(leader_node) ->
        leader_node

      _ ->
        nil
    end
  end

  defp maybe_cache_leader_hint({server_id, leader_node})
       when is_atom(server_id) and not is_nil(server_id) and is_atom(leader_node) and
              not is_nil(leader_node) do
    case parse_group_id_from_server_id(server_id) do
      {:ok, group_id} ->
        put_leader_hint(group_id, leader_node, System.monotonic_time(:millisecond))
        :ok

      :error ->
        :ok
    end
  end

  defp maybe_cache_leader_hint(_leader), do: :ok

  defp parse_group_id_from_server_id(server_id) when is_atom(server_id) do
    case Atom.to_string(server_id) do
      "raft_group_" <> suffix ->
        case Integer.parse(suffix) do
          {group_id, ""} when group_id >= 0 -> {:ok, group_id}
          _ -> :error
        end

      _ ->
        :error
    end
  end

  defp ensure_leader_cache_table do
    case :ets.whereis(@leader_cache_table) do
      :undefined ->
        try do
          :ets.new(@leader_cache_table, [
            :set,
            :named_table,
            :public,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> :ok
        end

      _table ->
        :ok
    end

    :ok
  end

  defp cached_leader_hint(group_id, now_ms)
       when is_integer(group_id) and group_id >= 0 and is_integer(now_ms) do
    case :ets.lookup(@leader_cache_table, group_id) do
      [{^group_id, leader_node, seen_at_ms}]
      when is_atom(leader_node) and is_integer(seen_at_ms) and
             now_ms - seen_at_ms <= @leader_cache_ttl_ms ->
        leader_node

      _ ->
        nil
    end
  end

  defp put_leader_hint(group_id, leader_node, now_ms)
       when is_integer(group_id) and group_id >= 0 and is_atom(leader_node) and is_integer(now_ms) do
    :ets.insert(@leader_cache_table, {group_id, leader_node, now_ms})
    :ok
  end

  defp target_to_atom({:local, _node}), do: :local
  defp target_to_atom({:remote, _nodes}), do: :remote

  defp leader_hint_status(false, _leader_hint), do: :disabled

  defp leader_hint_status(true, leader_hint)
       when is_atom(leader_hint) and not is_nil(leader_hint), do: :hit

  defp leader_hint_status(true, _leader_hint), do: :miss

  defp bump_leader_redirects(%{leader_redirects: count} = stats)
       when is_integer(count) and count >= 0 do
    %{stats | leader_redirects: count + 1}
  end

  defp node_hint?(value) when is_atom(value) and not is_nil(value), do: true
  defp node_hint?(_value), do: false
end
