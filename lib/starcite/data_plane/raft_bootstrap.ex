defmodule Starcite.DataPlane.RaftBootstrap do
  @moduledoc """
  Topology bootstrap for static write-node Raft groups.

  Responsibilities:

  - Bootstrap groups from static write-node config.
  - Start/join local assigned groups.
  - Maintain intrinsic Raft readiness state used by health/routing gates.

  This module does not perform dynamic membership add/remove operations.
  """

  use GenServer
  require Logger

  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.{RaftHealth, RaftManager}

  @ready_call_timeout_ms 1_000
  @group_task_max_concurrency 32
  @group_task_timeout_ms 60_000
  @group_bootstrap_poll_ms 100
  @group_bootstrap_max_attempts 300
  @readiness_refresh_call_timeout_ms 5_000
  @runtime_reconcile_interval_ms 5_000
  @group_leader_probe_timeout_ms 250
  @group_election_timeout_ms 500

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns true if this node has all assigned local Raft groups running."
  def ready? do
    case Process.whereis(__MODULE__) do
      nil -> false
      pid -> safe_ready_call(pid)
    end
  end

  @doc "Returns local intrinsic readiness status used by health/routing gates."
  @spec readiness_status(keyword()) :: map()
  def readiness_status(opts \\ []) when is_list(opts) do
    fallback = RaftHealth.readiness_status_fallback()
    refresh? = Keyword.get(opts, :refresh?, false)
    timeout_ms = readiness_status_timeout_ms(refresh?)
    message = {:readiness_status, refresh?}

    case Process.whereis(__MODULE__) do
      nil ->
        fallback

      pid ->
        safe_readiness_status_call(pid, message, timeout_ms, fallback)
    end
  end

  @doc "Records write-path outcomes so readiness can fail fast on Raft timeouts."
  @spec record_write_outcome(
          :local_ok
          | :local_error
          | :local_timeout
          | :leader_retry_ok
          | :leader_retry_error
          | :leader_retry_timeout
        ) :: :ok
  def record_write_outcome(outcome)
      when outcome in [
             :local_ok,
             :local_error,
             :local_timeout,
             :leader_retry_ok,
             :leader_retry_error,
             :leader_retry_timeout
           ] do
    case Process.whereis(__MODULE__) do
      nil ->
        :ok

      pid ->
        GenServer.cast(pid, {:write_outcome, outcome})
    end
  end

  @impl true
  def init(_opts) do
    :ok = configure_ra_system_storage()
    :ok = :ra.start()
    :logger.set_application_level(:ra, :error)
    # Readiness is served on demand, so we subscribe to node liveness events to
    # keep the cached consensus state current and fail readiness immediately.
    :ok = :net_kernel.monitor_nodes(true, [:nodedown_reason])

    Logger.info(
      "RaftBootstrap: starting on #{Node.self()} (write_node=#{WriteNodes.write_node?(Node.self())})"
    )

    send(self(), :bootstrap)
    schedule_runtime_reconcile()

    {:ok,
     %{
       startup_complete?: false,
       startup_mode: nil,
       sync_ref: nil,
       consensus_ready?: false,
       consensus_last_probe_at_ms: nil,
       consensus_probe_detail: %{
         checked_groups: 0,
         failing_group_id: nil,
         probe_result: "startup_sync"
       }
     }}
  end

  @impl true
  def handle_call(:ready?, _from, state) do
    next_state = RaftHealth.maybe_refresh(state, true)
    status = RaftHealth.readiness_status(next_state)
    {:reply, status.ready?, next_state}
  end

  @impl true
  def handle_call({:readiness_status, refresh?}, _from, state) when is_boolean(refresh?) do
    next_state = RaftHealth.maybe_refresh(state, refresh?)
    {:reply, RaftHealth.readiness_status(next_state), next_state}
  end

  @impl true
  def handle_call(:readiness_status, _from, state) do
    next_state = RaftHealth.maybe_refresh(state, false)
    {:reply, RaftHealth.readiness_status(next_state), next_state}
  end

  @impl true
  def handle_cast({:write_outcome, outcome}, state)
      when outcome in [
             :local_ok,
             :local_error,
             :local_timeout,
             :leader_retry_ok,
             :leader_retry_error,
             :leader_retry_timeout
           ] do
    {:noreply, RaftHealth.record_write_outcome(state, outcome)}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    {:noreply, maybe_start_sync(state, :bootstrap)}
  end

  @impl true
  def handle_info({:startup_complete, mode}, state)
      when mode in [:coordinator, :follower, :router] do
    Logger.info("RaftBootstrap: startup complete (mode=#{mode})")

    {:noreply,
     state
     |> Map.put(:startup_complete?, true)
     |> Map.put(:startup_mode, mode)
     |> RaftHealth.clear()}
  end

  @impl true
  def handle_info({:nodedown, down_node, _reason}, state) when is_atom(down_node) do
    {:noreply, RaftHealth.note_peer_down(state, down_node)}
  end

  @impl true
  def handle_info({:nodedown, down_node}, state) when is_atom(down_node) do
    {:noreply, RaftHealth.note_peer_down(state, down_node)}
  end

  @impl true
  def handle_info({:nodeup, up_node, _info}, state) when is_atom(up_node) do
    {:noreply, RaftHealth.note_peer_up(state, up_node)}
  end

  @impl true
  def handle_info({:nodeup, up_node}, state) when is_atom(up_node) do
    {:noreply, RaftHealth.note_peer_up(state, up_node)}
  end

  @impl true
  def handle_info({ref, _result}, %{sync_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{state | sync_ref: nil}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{sync_ref: ref} = state) do
    Logger.warning("RaftBootstrap: bootstrap task failed: #{inspect(reason)}")
    {:noreply, %{state | sync_ref: nil}}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:runtime_reconcile, state) do
    schedule_runtime_reconcile()
    {:noreply, maybe_reconcile_local_groups(state)}
  end

  @impl true
  def handle_info({ref, _result}, state) when is_reference(ref) do
    {:noreply, state}
  end

  defp maybe_start_sync(%{sync_ref: nil} = state, trigger) do
    owner = self()

    task =
      Task.Supervisor.async_nolink(Starcite.RaftTaskSupervisor, fn ->
        run_sync(trigger, owner)
      end)

    %{state | sync_ref: task.ref}
  end

  defp maybe_start_sync(state, _trigger), do: state

  defp run_sync(:bootstrap, owner) do
    case WriteNodes.validate() do
      {:ok, _config} ->
        cond do
          not WriteNodes.write_node?(Node.self()) ->
            send(owner, {:startup_complete, :router})

          bootstrap_coordinator?() ->
            Logger.info(
              "RaftBootstrap: write coordinator bootstrapping #{WriteNodes.num_groups()} groups"
            )

            run_groups_parallel(
              0..(WriteNodes.num_groups() - 1),
              "bootstrap",
              &ensure_group_bootstrapped/1
            )

            ensure_all_local_groups_running()
            ensure_local_group_leaders(compute_my_groups())

            send(owner, {:startup_complete, :coordinator})

          true ->
            my_groups = compute_my_groups()

            run_groups_parallel(my_groups, "join", &wait_and_join_group/1)
            run_groups_parallel(my_groups, "ensure-local", &ensure_local_group_running/1)
            ensure_local_group_leaders(my_groups)

            Logger.info("RaftBootstrap: write follower joined #{length(my_groups)} groups")
            send(owner, {:startup_complete, :follower})
        end

      {:error, reason} ->
        raise ArgumentError,
              "RaftBootstrap bootstrap aborted due to invalid write-node config: #{reason}"
    end
  end

  defp safe_ready_call(pid) do
    GenServer.call(pid, :ready?, @ready_call_timeout_ms)
  catch
    :exit, _reason -> false
  end

  defp safe_readiness_status_call(pid, message, timeout_ms, fallback)
       when is_map(fallback) and is_integer(timeout_ms) and timeout_ms > 0 do
    GenServer.call(pid, message, timeout_ms)
  catch
    :exit, _reason -> fallback
  end

  defp readiness_status_timeout_ms(true), do: @readiness_refresh_call_timeout_ms
  defp readiness_status_timeout_ms(false), do: @ready_call_timeout_ms

  defp wait_and_join_group(group_id) do
    server_id = RaftManager.server_id(group_id)

    case wait_for_group_exists(group_id, server_id) do
      :ok ->
        ensure_local_group_running(group_id)

      :timeout ->
        Logger.warning("RaftBootstrap: timeout waiting for group #{group_id} bootstrap")
        ensure_local_group_running(group_id)
        {:error, :bootstrap_timeout}
    end
  end

  defp wait_for_group_exists(group_id, server_id) do
    wait_for_group_exists(group_id, server_id, 0, @group_bootstrap_max_attempts)
  end

  defp wait_for_group_exists(_group_id, _server_id, attempt, max) when attempt >= max do
    :timeout
  end

  defp wait_for_group_exists(group_id, server_id, attempt, max) do
    nodes = RaftManager.replicas_for_group(group_id)

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

  defp ensure_group_bootstrapped(group_id) do
    server_id = RaftManager.server_id(group_id)
    replica_nodes = RaftManager.replicas_for_group(group_id)

    case cluster_members(server_id, replica_nodes) do
      {:ok, _members, _leader} ->
        ensure_local_group_running(group_id)

      :missing ->
        _ = bootstrap_group(group_id, replica_nodes)
        ensure_local_group_running(group_id)

      :timeout ->
        ensure_local_group_running(group_id)
    end
  end

  defp bootstrap_group(group_id, replica_nodes) do
    server_id = RaftManager.server_id(group_id)
    cluster_name = RaftManager.cluster_name(group_id)
    machine = RaftManager.machine(group_id)
    server_ids = for node <- replica_nodes, do: {server_id, node}

    with :ok <- RaftManager.ensure_runtime_started() do
      case :ra.start_cluster(:default, cluster_name, machine, server_ids) do
        {:ok, _started, _not_started} ->
          :ok

        {:error, :cluster_not_formed} ->
          :ok
      end
    end
  end

  defp ensure_local_group_running(group_id) do
    if RaftManager.should_participate?(group_id) and not group_running?(group_id) do
      RaftManager.start_group(group_id)
    else
      :ok
    end
  end

  defp ensure_local_group_leaders(groups) do
    run_groups_parallel(groups, "ensure-leader", &ensure_group_has_leader/1)
  end

  defp ensure_group_has_leader(group_id) do
    if RaftManager.should_participate?(group_id) and group_running?(group_id) do
      server_ref = {RaftManager.server_id(group_id), Node.self()}

      case :ra.member_overview(server_ref, @group_leader_probe_timeout_ms) do
        {:ok, %{leader_id: {_, _}}, _} ->
          :ok

        {:ok, _overview, _} ->
          trigger_group_election(group_id, server_ref, :leader_unknown)

        {:timeout, _} ->
          trigger_group_election(group_id, server_ref, :leader_query_timeout)

        {:error, reason} ->
          trigger_group_election(group_id, server_ref, {:leader_query_error, reason})
      end
    else
      :ok
    end
  end

  defp trigger_group_election(group_id, server_ref, reason) do
    Logger.debug(
      "RaftBootstrap: triggering election for group #{group_id} (reason=#{inspect(reason)})"
    )

    try do
      :ok = :ra.trigger_election(server_ref, @group_election_timeout_ms)
      :ok
    catch
      :exit, exit_reason ->
        Logger.warning(
          "RaftBootstrap: trigger_election failed for group #{group_id}: #{inspect(exit_reason)}"
        )

        :ok
    end
  end

  defp cluster_members(server_id, nodes) do
    {result, saw_timeout} =
      Enum.reduce_while(nodes, {:missing, false}, fn node, {_acc, timed_out} ->
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
        fn group_id ->
          result =
            try do
              fun.(group_id)
            rescue
              error in [ArgumentError] ->
                {:error, {:invalid_config, Exception.message(error)}}
            end

          {group_id, result}
        end,
        max_concurrency: max_concurrency,
        timeout: @group_task_timeout_ms,
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.each(&handle_group_result(&1, label))

      duration_ms = System.monotonic_time(:millisecond) - started_at_ms

      Logger.info(
        "RaftBootstrap: #{label} processed #{total} groups in #{duration_ms}ms (max_concurrency=#{max_concurrency})"
      )
    end
  end

  defp handle_group_result({:ok, {_group_id, :ok}}, _label), do: :ok

  defp handle_group_result({:ok, {group_id, {:error, reason}}}, label) do
    Logger.debug("RaftBootstrap: #{label} group #{group_id} failed: #{inspect(reason)}")
  end

  defp handle_group_result({:ok, {_group_id, _result}}, _label), do: :ok

  defp handle_group_result({:exit, reason}, label) do
    Logger.warning("RaftBootstrap: #{label} worker crashed: #{inspect(reason)}")
  end

  defp ensure_all_local_groups_running do
    compute_my_groups()
    |> run_groups_parallel("ensure-local", &ensure_local_group_running/1)
  end

  defp compute_my_groups do
    for group_id <- 0..(WriteNodes.num_groups() - 1),
        RaftManager.should_participate?(group_id),
        do: group_id
  end

  defp group_running?(group_id) do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp bootstrap_coordinator? do
    case WriteNodes.nodes() do
      [first | _] -> Node.self() == first
      [] -> false
    end
  end

  defp configure_ra_system_storage do
    ra_system_dir = RaftManager.ra_system_data_dir()

    case File.mkdir_p(ra_system_dir) do
      :ok ->
        ra_system_dir_charlist = String.to_charlist(ra_system_dir)
        Application.put_env(:ra, :data_dir, ra_system_dir_charlist)
        Application.put_env(:ra, :wal_data_dir, ra_system_dir_charlist)
        :ok

      {:error, reason} ->
        raise ArgumentError,
              "failed to prepare :ra storage directory #{inspect(ra_system_dir)}: #{inspect(reason)}"
    end
  end

  defp maybe_reconcile_local_groups(%{startup_complete?: true} = state) do
    if WriteNodes.write_node?(Node.self()) do
      local_groups = compute_my_groups()
      down_groups = Enum.reject(local_groups, &group_running?/1)

      if down_groups == [] do
        state
      else
        Logger.warning(
          "RaftBootstrap: detected #{length(down_groups)} local groups down, attempting recovery"
        )

        run_groups_parallel(down_groups, "runtime-reconcile", &ensure_local_group_running/1)
        ensure_local_group_leaders(down_groups)
        Map.put(state, :consensus_last_probe_at_ms, nil)
      end
    else
      state
    end
  end

  defp maybe_reconcile_local_groups(state), do: state

  defp schedule_runtime_reconcile do
    Process.send_after(self(), :runtime_reconcile, @runtime_reconcile_interval_ms)
  end
end
