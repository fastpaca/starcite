defmodule Starcite.Runtime.RaftTopology do
  @moduledoc """
  Topology controller for Raft groups on this node.

  Handles startup/join and keeps membership converged as cluster nodes come and
  go. This module controls when topology work runs; it does not define the
  placement algorithm or state-machine semantics.

  ## Responsibilities

  - Coordinate bootstrap/join flow for local Raft groups.
  - Report local readiness via `ready?/0`.
  - Reconcile membership after node up/down and on periodic checks.
  - Apply membership transitions safely (add before remove).

  ## NOT Responsible For

  - Replica placement policy (`RaftManager`).
  - Raft state-machine command handling (`RaftFSM`).
  - Request routing, storage API, or client behavior.
  """

  use GenServer
  require Logger

  alias Starcite.Runtime.RaftManager

  @reconcile_interval_ms 10_000
  @rebalance_debounce_ms 2_000
  @ready_call_timeout_ms 1_000
  @group_task_max_concurrency 32
  @group_bootstrap_poll_ms 100
  @group_bootstrap_max_attempts 300

  # Client API

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns true if this node has local Raft groups running."
  def ready? do
    case Process.whereis(__MODULE__) do
      nil -> false
      pid -> safe_ready_call(pid)
    end
  end

  @doc "Returns all nodes in cluster (strongly consistent via Erlang distribution)."
  def all_nodes do
    [Node.self() | Node.list()] |> Enum.uniq() |> Enum.sort()
  end

  @doc "Backwards compatibility shim for code expecting ready_nodes/0."
  def ready_nodes, do: all_nodes()

  # GenServer callbacks

  @impl true
  def init(_opts) do
    # Start Ra system
    :ok = :ra.start()

    :logger.set_application_level(:ra, :error)

    # Monitor cluster changes via boring Erlang
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)

    Logger.info("RaftTopology: starting on #{Node.self()}")

    # Bootstrap immediately, then reconcile continuously
    send(self(), :bootstrap)
    schedule_reconcile()

    {:ok,
     %{
       rebalance_timer: nil,
       startup_complete?: false,
       sync_ref: nil
     }}
  end

  @impl true
  def handle_call(:ready?, _from, state) do
    # Readiness = startup phase complete + local raft presence.
    my_groups = compute_my_groups()
    local_ready = my_groups == [] or Enum.all?(my_groups, &group_running?/1)
    ready = state.startup_complete? and local_ready

    {:reply, ready, state}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    cluster = all_nodes()

    # Wait for expected cluster if CLUSTER_NODES is set (avoids bootstrap races in tests)
    expected = parse_expected_cluster_size()

    if coordinator?(cluster) and expected > 1 and length(cluster) < expected do
      Logger.debug("RaftTopology: waiting for cluster #{length(cluster)}/#{expected}")
      Process.send_after(self(), :bootstrap, 500)
      {:noreply, state}
    else
      if coordinator?(cluster) and expected > 1 and not raft_system_ready?(cluster) do
        Logger.debug("RaftTopology: waiting for Ra systems to start on all nodes")
        Process.send_after(self(), :bootstrap, 500)
        {:noreply, state}
      else
        if coordinator?(cluster) do
          Logger.info(
            "RaftTopology: coordinator bootstrapping #{RaftManager.num_groups()} groups"
          )

          bootstrap_all_groups_async(cluster, self())

          {:noreply, state}
        else
          Logger.info("RaftTopology: non-coordinator joining existing groups")
          join_my_groups_async(cluster, self())

          {:noreply, state}
        end
      end
    end
  end

  @impl true
  def handle_info({:startup_complete, mode}, state)
      when mode in [:coordinator, :follower] do
    case mode do
      :coordinator ->
        Logger.info("RaftTopology: bootstrap complete")
        send(self(), :rebalance)

      :follower ->
        Logger.info("RaftTopology: join complete")
    end

    {:noreply, %{state | startup_complete?: true}}
  end

  @impl true
  def handle_info({ref, _result}, %{sync_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | sync_ref: nil}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{sync_ref: ref} = state) do
    Logger.warning("RaftTopology: topology sync task failed: #{inspect(reason)}")
    {:noreply, %{state | sync_ref: nil}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info({ref, _result}, state) when is_reference(ref) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:reconcile, state) do
    cluster = all_nodes()

    state =
      if coordinator?(cluster) and state.startup_complete? do
        maybe_start_sync(state, cluster, :reconcile)
      else
        state
      end

    schedule_reconcile()
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("RaftTopology: node up #{inspect(node)}")
    {:noreply, schedule_rebalance(state)}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("RaftTopology: node down #{inspect(node)}")
    {:noreply, schedule_rebalance(state)}
  end

  @impl true
  def handle_info(:rebalance, state) do
    cluster = all_nodes()

    state =
      if coordinator?(cluster) and state.startup_complete? do
        Logger.info("RaftTopology: rebalancing #{length(cluster)} nodes")
        maybe_start_sync(state, cluster, :rebalance)
      else
        state
      end

    {:noreply, %{state | rebalance_timer: nil}}
  end

  # Private functions

  defp schedule_reconcile do
    Process.send_after(self(), :reconcile, @reconcile_interval_ms)
  end

  defp schedule_rebalance(state) do
    if state.rebalance_timer do
      Process.cancel_timer(state.rebalance_timer)
    end

    timer = Process.send_after(self(), :rebalance, @rebalance_debounce_ms)
    %{state | rebalance_timer: timer}
  end

  defp coordinator?(cluster) do
    Node.self() == List.first(cluster)
  end

  defp maybe_start_sync(%{sync_ref: nil} = state, cluster, trigger) do
    task =
      Task.Supervisor.async_nolink(Starcite.RaftTaskSupervisor, fn ->
        run_groups_parallel(
          0..(RaftManager.num_groups() - 1),
          "#{trigger} sync",
          &reconcile_group(&1, cluster)
        )
      end)

    Logger.debug("RaftTopology: started #{trigger} sync task")
    %{state | sync_ref: task.ref}
  end

  defp maybe_start_sync(state, _cluster, _trigger), do: state

  defp safe_ready_call(pid) do
    GenServer.call(pid, :ready?, @ready_call_timeout_ms)
  catch
    :exit, _reason -> false
  end

  defp raft_system_ready?(cluster) do
    Enum.all?(cluster, fn node ->
      case :rpc.call(node, Process, :whereis, [:ra_server_sup_sup]) do
        pid when is_pid(pid) -> true
        _ -> false
      end
    end)
  end

  defp bootstrap_all_groups_async(cluster, owner) do
    # Bootstrap async to avoid blocking GenServer
    Task.Supervisor.start_child(Starcite.RaftTaskSupervisor, fn ->
      run_groups_parallel(
        0..(RaftManager.num_groups() - 1),
        "bootstrap",
        &ensure_group_exists(&1, cluster)
      )

      send(owner, {:startup_complete, :coordinator})
    end)
  end

  defp join_my_groups_async(_cluster, owner) do
    # Join async to avoid blocking GenServer
    # Non-coordinators wait for groups to be bootstrapped by coordinator
    Task.Supervisor.start_child(Starcite.RaftTaskSupervisor, fn ->
      await_cluster_min_size()
      my_groups = compute_my_groups()

      run_groups_parallel(my_groups, "join", &wait_and_join_group/1)

      Logger.info("RaftTopology: joined #{length(my_groups)} groups")
      send(owner, {:startup_complete, :follower})
    end)
  end

  defp wait_and_join_group(group_id) do
    server_id = RaftManager.server_id(group_id)

    # Poll until group exists somewhere in cluster (coordinator bootstrapping)
    case wait_for_group_exists(group_id, server_id) do
      :ok ->
        # Group exists, start locally if not running
        if group_running?(group_id) do
          :ok
        else
          RaftManager.start_group(group_id)
        end

      :timeout ->
        Logger.warning("RaftTopology: timeout waiting for group #{group_id} to be bootstrapped")
        {:error, :bootstrap_timeout}
    end
  end

  defp await_cluster_min_size do
    expected = parse_expected_cluster_size()

    if expected > 1 do
      wait_for_min_size(2, 20)
    else
      :ok
    end
  end

  defp wait_for_min_size(_min_size, 0), do: :ok

  defp wait_for_min_size(min_size, attempts) do
    if length(all_nodes()) >= min_size do
      :ok
    else
      Process.sleep(500)
      wait_for_min_size(min_size, attempts - 1)
    end
  end

  defp wait_for_group_exists(group_id, server_id) do
    wait_for_group_exists(group_id, server_id, 0, @group_bootstrap_max_attempts)
  end

  defp wait_for_group_exists(_group_id, _server_id, attempt, max) when attempt >= max do
    :timeout
  end

  defp wait_for_group_exists(group_id, server_id, attempt, max) do
    nodes = replicas_for_group(group_id, all_nodes())

    case find_group_member(server_id, nodes) do
      :ok ->
        :ok

      :missing ->
        Process.sleep(@group_bootstrap_poll_ms)
        wait_for_group_exists(group_id, server_id, attempt + 1, max)
    end
  end

  defp find_group_member(server_id, nodes) do
    Enum.reduce_while(nodes, :missing, fn node, _acc ->
      case :ra.members({server_id, node}) do
        {:ok, _members, _leader} -> {:halt, :ok}
        _ -> {:cont, :missing}
      end
    end)
  end

  defp ensure_group_exists(group_id, cluster) do
    server_id = RaftManager.server_id(group_id)
    should_host_group? = Node.self() in replicas_for_group(group_id, cluster)

    case cluster_members(server_id, cluster) do
      {:ok, _members, _leader} ->
        # Group exists; ensure local server is running when this node is a replica.
        maybe_start_local_group(group_id, should_host_group?)
        :ok

      :missing ->
        # Group doesn't exist, bootstrap it
        bootstrap_group(group_id, cluster)

      :timeout ->
        # Might exist, start locally if we're a replica
        maybe_start_local_group(group_id, should_host_group?)
        :ok
    end
  end

  defp maybe_start_local_group(_group_id, false), do: :ok

  defp maybe_start_local_group(group_id, true) do
    if group_running?(group_id) do
      :ok
    else
      RaftManager.start_group(group_id)
    end
  end

  defp bootstrap_group(group_id, cluster) do
    replica_nodes = replicas_for_group(group_id, cluster)
    server_id = RaftManager.server_id(group_id)
    cluster_name = RaftManager.cluster_name(group_id)
    machine = {:module, Starcite.Runtime.RaftFSM, %{group_id: group_id}}

    server_ids = for node <- replica_nodes, do: {server_id, node}

    case :ra.start_cluster(:default, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} ->
        :ok

      {:error, reason} ->
        Logger.warning("RaftTopology: failed to bootstrap group #{group_id}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp reconcile_group(group_id, cluster) do
    ensure_group_exists(group_id, cluster)

    # After ensuring exists, reconcile membership
    rebalance_group(group_id, cluster)
  end

  defp rebalance_group(group_id, cluster) do
    server_id = RaftManager.server_id(group_id)

    desired_members =
      replicas_for_group(group_id, cluster)
      |> Enum.map(&{server_id, &1})

    case cluster_members(server_id, cluster) do
      {:ok, current, leader} ->
        to_add = desired_members -- current
        to_remove = current -- desired_members

        unless Enum.empty?(to_add) and Enum.empty?(to_remove) do
          Logger.info(
            "RaftTopology: rebalancing group #{group_id}: +#{length(to_add)} -#{length(to_remove)}"
          )
        end

        # Use leader for membership changes, fall back to any member
        leader_ref = leader || List.first(current)

        if leader_ref do
          # Add first
          Enum.each(to_add, &add_member(group_id, leader_ref, &1))

          case pending_promotions(server_id, cluster, leader_ref, to_add) do
            [] ->
              # Remove only after added peers are promoted to voters.
              Enum.each(to_remove, &remove_member(group_id, leader_ref, &1))

            pending ->
              Logger.debug(
                "RaftTopology: group #{group_id} waiting for #{length(pending)} promotable members before removals"
              )
          end
        end

      :missing ->
        :ok

      :timeout ->
        :ok
    end
  end

  defp cluster_members(server_id, cluster) do
    {result, saw_timeout} =
      Enum.reduce_while(cluster, {:missing, false}, fn node, {_acc, timed_out} ->
        case :ra.members({server_id, node}) do
          {:ok, members, leader} -> {:halt, {{:ok, members, leader}, timed_out}}
          {:timeout, _} -> {:cont, {:missing, true}}
          _ -> {:cont, {:missing, timed_out}}
        end
      end)

    case result do
      {:ok, _members, _leader} = ok -> ok
      :missing when saw_timeout -> :timeout
      :missing -> :missing
    end
  end

  defp cluster_members_info(server_id, cluster, leader_ref) do
    targets =
      [leader_ref | Enum.map(cluster, &{server_id, &1})]
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq()

    {result, saw_timeout} =
      Enum.reduce_while(targets, {:missing, false}, fn server_ref, {_acc, timed_out} ->
        case :ra.members_info(server_ref) do
          {:ok, members_info, _responder} when is_map(members_info) ->
            {:halt, {{:ok, members_info}, timed_out}}

          {:timeout, _} ->
            {:cont, {:missing, true}}

          _ ->
            {:cont, {:missing, timed_out}}
        end
      end)

    case result do
      {:ok, _members_info} = ok -> ok
      :missing when saw_timeout -> :timeout
      :missing -> :missing
    end
  end

  defp pending_promotions(_server_id, _cluster, _leader_ref, []), do: []

  defp pending_promotions(server_id, cluster, leader_ref, to_add) do
    case cluster_members_info(server_id, cluster, leader_ref) do
      {:ok, members_info} ->
        Enum.filter(to_add, fn member ->
          membership =
            members_info
            |> Map.get(member, %{})
            |> Map.get(:voter_status, %{})
            |> Map.get(:membership)

          membership != :voter
        end)

      :missing ->
        to_add

      :timeout ->
        to_add
    end
  end

  defp add_member(group_id, leader_ref, {_server_id, node} = member) do
    # Ensure group started on target
    :rpc.call(node, RaftManager, :start_group, [group_id])

    member_conf = %{
      id: member,
      membership: :promotable,
      uid: RaftManager.member_uid(group_id, node)
    }

    case :ra.add_member(leader_ref, member_conf) do
      {:ok, _index, _term} ->
        Logger.debug("RaftTopology: added #{inspect(node)} to group #{group_id}")
        :ok

      {:error, {:already_member, _}} ->
        :ok

      {:error, :not_leader} ->
        :ok

      {:error, reason} ->
        Logger.debug("RaftTopology: add failed for group #{group_id}: #{inspect(reason)}")
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  end

  defp remove_member(group_id, leader_ref, {server_id, node} = member) do
    case :ra.remove_member(leader_ref, member) do
      {:ok, _index, _term} ->
        Logger.debug("RaftTopology: removed #{inspect(node)} from group #{group_id}")
        :rpc.call(node, :ra, :stop_server, [:default, {server_id, node}])
        :ok

      {:error, {:not_member, _}} ->
        :ok

      {:error, :not_leader} ->
        :ok

      {:error, reason} ->
        Logger.debug("RaftTopology: remove failed for group #{group_id}: #{inspect(reason)}")
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  end

  defp run_groups_parallel(groups, label, fun) when is_function(fun, 1) do
    total = Enum.count(groups)

    if total == 0 do
      :ok
    else
      max_concurrency = min(total, @group_task_max_concurrency)
      started_at_ms = System.monotonic_time(:millisecond)

      Task.Supervisor.async_stream_nolink(
        Starcite.RaftTaskSupervisor,
        groups,
        fn group_id -> {group_id, fun.(group_id)} end,
        max_concurrency: max_concurrency,
        timeout: :infinity,
        ordered: false
      )
      |> Enum.each(&handle_group_result(&1, label))

      duration_ms = System.monotonic_time(:millisecond) - started_at_ms

      Logger.info(
        "RaftTopology: #{label} processed #{total} groups in #{duration_ms}ms (max_concurrency=#{max_concurrency})"
      )
    end
  end

  defp handle_group_result({:ok, {_group_id, :ok}}, _label), do: :ok

  defp handle_group_result({:ok, {group_id, {:error, reason}}}, label) do
    Logger.debug("RaftTopology: #{label} group #{group_id} failed: #{inspect(reason)}")
  end

  defp handle_group_result({:ok, {_group_id, _result}}, _label), do: :ok

  defp handle_group_result({:exit, reason}, label) do
    Logger.warning("RaftTopology: #{label} worker crashed: #{inspect(reason)}")
  end

  defp compute_my_groups(cluster \\ nil) do
    nodes = cluster || all_nodes()

    for group_id <- 0..(RaftManager.num_groups() - 1),
        Node.self() in replicas_for_group(group_id, nodes),
        do: group_id
  end

  defp replicas_for_group(group_id, cluster) do
    cluster
    |> Enum.map(fn node ->
      hash = :erlang.phash2({group_id, node})
      {hash, node}
    end)
    |> Enum.sort()
    |> Enum.take(3)
    |> Enum.map(fn {_hash, node} -> node end)
  end

  defp group_running?(group_id) do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp parse_expected_cluster_size do
    case System.get_env("CLUSTER_NODES") do
      nil ->
        1

      "" ->
        1

      nodes_str ->
        nodes_str
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.reject(&(&1 == ""))
        |> length()
    end
  end
end
