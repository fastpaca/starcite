defmodule Fastpaca.Runtime.RaftManager do
  @moduledoc """
  Pure utility module for Raft group placement and lifecycle.

  ## Responsibilities

  - Compute replica placement via rendezvous hashing
  - Bootstrap or join Raft clusters
  - Query Presence for cluster membership (single source of truth)

  ## NOT Responsible For

  - Rebalancing (RaftTopology handles this)
  - Tracking started groups (stateless)
  - Monitoring cluster changes (RaftTopology handles this)
  """

  require Logger

  alias Fastpaca.Runtime.{RaftFSM, RaftTopology}

  # TODO: make these configurable
  @num_groups 256
  @num_lanes 16
  @replication_factor 3

  @doc false
  def num_groups, do: @num_groups

  @doc "Map conversation_id → group_id (0..#{@num_groups - 1})"
  def group_for_conversation(conversation_id) do
    :erlang.phash2(conversation_id, @num_groups)
  end

  @doc "Map conversation_id → lane_id (0..#{@num_lanes - 1}) within its group"
  def lane_for_conversation(conversation_id) do
    :erlang.phash2(conversation_id, @num_lanes)
  end

  @doc "Get Ra server ID (process name) for a group"
  def server_id(group_id) do
    String.to_atom("raft_group_#{group_id}")
  end

  @doc "Get cluster name for Ra API calls"
  def cluster_name(group_id) do
    String.to_atom("raft_cluster_#{group_id}")
  end

  @doc """
  Determine which nodes should host replicas for a group.

  CRITICAL: Uses all_nodes() (joining + ready) for placement.
  Ensures new nodes get scheduled as replicas during their join phase.

  Falls back to current_nodes() if Presence empty (boot scenario).
  """
  def replicas_for_group(group_id) do
    nodes =
      case RaftTopology.all_nodes() do
        [] ->
          # Boot fallback before Presence syncs
          [Node.self() | Node.list()] |> Enum.uniq() |> Enum.sort()

        all_nodes ->
          # Placement uses ALL nodes (joining + ready), not just ready!
          all_nodes
      end

    # Rendezvous (HRW) hashing for deterministic placement
    nodes
    |> Enum.map(fn node ->
      # TODO: don't use phash2 - use eg. xxhash for better
      # distribution if this ever becomes a problem
      score = :erlang.phash2({group_id, node})
      {score, node}
    end)
    |> Enum.sort()
    |> Enum.take(@replication_factor)
    |> Enum.map(fn {_score, node} -> node end)
  end

  @doc "Check if this node should participate in a group"
  def should_participate?(group_id) do
    Node.self() in replicas_for_group(group_id)
  end

  @doc """
  Start a Raft group on this node.

  Called by RaftTopology's Task.Supervisor (async).

  ## Process

  1. Check if already running (idempotent)
  2. Determine if bootstrap node (lowest in replica set)
  3. Bootstrap: call :ra.start_cluster with all replicas
  4. Join: call :ra.start_server to join existing cluster

  Returns:
  - :ok if started or already running
  - {:error, reason} if failed
  """
  def start_group(group_id) do
    server_id = server_id(group_id)

    # Already running?
    if Process.whereis(server_id) do
      :ok
    else
      my_node = Node.self()
      cluster_name = cluster_name(group_id)
      machine = {:module, RaftFSM, %{group_id: group_id}}

      replica_nodes = replicas_for_group(group_id)
      server_ids = for node <- replica_nodes, do: {server_id, node}

      # Deterministic coordinator = lowest node name
      bootstrap_node = Enum.min(replica_nodes)
      am_bootstrap = my_node == bootstrap_node

      Logger.debug(
        "RaftManager: Starting group #{group_id} with #{length(replica_nodes)} replicas (bootstrap: #{bootstrap_node == my_node})"
      )

      if am_bootstrap do
        # Ensure only one node bootstraps this group across the cluster
        :global.trans(
          {:raft_bootstrap, group_id},
          fn ->
            bootstrap_cluster(group_id, cluster_name, machine, server_ids, {server_id, my_node})
          end,
          [Node.self()],
          10_000
        )
      else
        # Retry join until bootstrap node creates cluster
        join_cluster_with_retry(group_id, server_id, cluster_name, machine,
          retries: 10,
          attempts: 0
        )
      end
    end
  end

  defp bootstrap_cluster(group_id, cluster_name, machine, server_ids, my_server_id) do
    case :ra.start_cluster(:default, cluster_name, machine, server_ids) do
      {:ok, started, not_started} ->
        cond do
          my_server_id in started ->
            Logger.debug("RaftManager: Bootstrapped group #{group_id}")
            :ok

          my_server_id in not_started ->
            join_cluster(group_id, elem(my_server_id, 0), cluster_name, machine)

          true ->
            Logger.warning("RaftManager: Not in member list for group #{group_id}")
            :ok
        end

      {:error, {:already_started, _}} ->
        Logger.debug("RaftManager: Group #{group_id} exists, joining")
        join_cluster(group_id, elem(my_server_id, 0), cluster_name, machine)

      {:error, reason} ->
        Logger.error("RaftManager: Failed to bootstrap group #{group_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp join_cluster_with_retry(group_id, _server_id, _cluster_name, _machine,
         retries: 0,
         attempts: _attempts
       ) do
    Logger.error("RaftManager: Failed to join group #{group_id} after retries")
    {:error, :join_timeout}
  end

  defp join_cluster_with_retry(group_id, server_id, cluster_name, machine,
         retries: retries,
         attempts: _attempts
       ) do
    case join_cluster(group_id, server_id, cluster_name, machine) do
      :ok ->
        :ok

      {:error, :enoent} ->
        # Cluster doesn't exist yet, retry with small backoff
        Process.sleep(100)

        join_cluster_with_retry(group_id, server_id, cluster_name, machine,
          retries: retries - 1,
          attempts: 0
        )

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp join_cluster(group_id, server_id, cluster_name, machine) do
    my_node = Node.self()
    node_name = my_node |> Atom.to_string() |> String.replace(~r/[^a-zA-Z0-9_]/, "_")

    # Per-node Ra data directory, which avoid conflicts in case we have
    # multiple groups & nodes on the same host.
    data_dir_root = Application.get_env(:fastpaca, :raft_data_dir, "priv/raft")
    data_dir = Path.join([data_dir_root, "group_#{group_id}", node_name])
    File.mkdir_p!(data_dir)

    # No cluster token enforcement
    server_conf = %{
      id: {server_id, my_node},
      uid: "raft_group_#{group_id}_#{node_name}",
      cluster_name: cluster_name,
      log_init_args: %{
        uid: "raft_log_#{group_id}_#{node_name}",
        data_dir: data_dir
      },
      machine: machine
    }

    case :ra.start_server(:default, server_conf) do
      :ok ->
        Logger.debug("RaftManager: Joined group #{group_id}")
        :ok

      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :ok

      # Handle supervisor shutdown when child already started
      {:error, {:shutdown, {:failed_to_start_child, _, {:already_started, _}}}} ->
        Logger.debug("RaftManager: Group #{group_id} already running locally")
        :ok

      {:error, reason} ->
        Logger.error("RaftManager: Failed to join group #{group_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @doc false
  def machine(group_id) do
    {:module, RaftFSM, %{group_id: group_id}}
  end

  # (cluster token helpers removed)
end
