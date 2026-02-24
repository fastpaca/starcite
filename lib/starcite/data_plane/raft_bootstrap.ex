defmodule Starcite.DataPlane.RaftBootstrap do
  @moduledoc """
  Topology bootstrap for static write-node Raft groups.

  Responsibilities:

  - Bootstrap groups from static write-node config.
  - Start/join local assigned groups.

  This module does not perform dynamic membership add/remove operations or
  react to runtime nodeup/nodedown events.
  """

  use GenServer
  require Logger

  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.RaftManager

  @ready_call_timeout_ms 1_000
  @group_task_max_concurrency 32
  @group_task_timeout_ms 60_000
  @group_bootstrap_poll_ms 100
  @group_bootstrap_max_attempts 300
  @consensus_refresh_interval_ms 1_000
  @consensus_probe_timeout_ms 200
  @consensus_probe_ttl_ms 3_000
  @consensus_success_streak_required 1
  @consensus_failure_streak_required 1

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
  @spec readiness_status() :: map()
  def readiness_status do
    fallback = readiness_status_fallback()

    case Process.whereis(__MODULE__) do
      nil ->
        fallback

      pid ->
        safe_readiness_status_call(pid, fallback)
    end
  end

  @impl true
  def init(_opts) do
    :ok = :ra.start()
    :logger.set_application_level(:ra, :error)

    Logger.info(
      "RaftBootstrap: starting on #{Node.self()} (write_node=#{WriteNodes.write_node?(Node.self())})"
    )

    send(self(), :bootstrap)
    schedule_consensus_refresh()

    {:ok,
     %{
       startup_complete?: false,
       startup_mode: nil,
       sync_ref: nil,
       consensus_ready?: false,
       consensus_last_probe_at_ms: nil,
       consensus_probe_success_streak: 0,
       consensus_probe_failure_streak: 0,
       consensus_probe_detail: %{
         checked_groups: 0,
         failing_group_id: nil,
         probe_result: "startup_sync"
       }
     }}
  end

  @impl true
  def handle_call(:ready?, _from, state) do
    status = readiness_status_from_state(state)
    {:reply, status.ready?, state}
  end

  @impl true
  def handle_call(:readiness_status, _from, state) do
    {:reply, readiness_status_from_state(state), state}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    {:noreply, maybe_start_sync(state, :bootstrap)}
  end

  @impl true
  def handle_info({:startup_complete, mode}, state)
      when mode in [:coordinator, :follower, :router] do
    Logger.info("RaftBootstrap: startup complete (mode=#{mode})")

    next_state =
      state
      |> Map.put(:startup_complete?, true)
      |> Map.put(:startup_mode, mode)
      |> refresh_consensus_state()

    {:noreply, next_state}
  end

  @impl true
  def handle_info(:refresh_consensus_ready, state) do
    next_state =
      if state.startup_complete? do
        refresh_consensus_state(state)
      else
        clear_consensus_state(state)
      end

    schedule_consensus_refresh()

    {:noreply, next_state}
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

  defp schedule_consensus_refresh do
    Process.send_after(self(), :refresh_consensus_ready, @consensus_refresh_interval_ms)
  end

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

            send(owner, {:startup_complete, :coordinator})

          true ->
            my_groups = compute_my_groups()

            run_groups_parallel(my_groups, "join", &wait_and_join_group/1)
            run_groups_parallel(my_groups, "ensure-local", &ensure_local_group_running/1)

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

  defp safe_readiness_status_call(pid, fallback) when is_map(fallback) do
    GenServer.call(pid, :readiness_status, @ready_call_timeout_ms)
  catch
    :exit, _reason -> fallback
  end

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

    case :ra.start_cluster(:default, cluster_name, machine, server_ids) do
      {:ok, _started, _not_started} ->
        :ok

      {:error, :cluster_not_formed} ->
        :ok
    end
  end

  defp ensure_local_group_running(group_id) do
    if RaftManager.should_participate?(group_id) and not group_running?(group_id) do
      RaftManager.start_group(group_id)
    else
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

  defp refresh_consensus_state(state) when is_map(state) do
    now_ms = System.monotonic_time(:millisecond)

    case consensus_probe() do
      {:ok, detail} ->
        success_streak = state.consensus_probe_success_streak + 1

        %{
          state
          | consensus_ready?: success_streak >= @consensus_success_streak_required,
            consensus_last_probe_at_ms: now_ms,
            consensus_probe_success_streak: success_streak,
            consensus_probe_failure_streak: 0,
            consensus_probe_detail: detail
        }

      {:error, detail} ->
        failure_streak = state.consensus_probe_failure_streak + 1

        %{
          state
          | consensus_ready?: failure_streak < @consensus_failure_streak_required,
            consensus_last_probe_at_ms: now_ms,
            consensus_probe_success_streak: 0,
            consensus_probe_failure_streak: failure_streak,
            consensus_probe_detail: detail
        }
    end
  end

  defp clear_consensus_state(state) when is_map(state) do
    %{
      state
      | consensus_ready?: false,
        consensus_last_probe_at_ms: nil,
        consensus_probe_success_streak: 0,
        consensus_probe_failure_streak: 0,
        consensus_probe_detail: %{
          checked_groups: 0,
          failing_group_id: nil,
          probe_result: "startup_sync"
        }
    }
  end

  defp consensus_probe do
    case compute_my_groups() do
      [] ->
        {:ok, %{checked_groups: 0, failing_group_id: nil, probe_result: "ok"}}

      my_groups ->
        Enum.reduce_while(my_groups, {:ok, 0}, fn group_id, {:ok, checked_groups} ->
          case group_consensus_ready?(group_id) do
            :ok ->
              {:cont, {:ok, checked_groups + 1}}

            {:error, detail} ->
              {:halt, {:error, Map.put(detail, :checked_groups, checked_groups + 1)}}
          end
        end)
        |> case do
          {:ok, checked_groups} ->
            {:ok, %{checked_groups: checked_groups, failing_group_id: nil, probe_result: "ok"}}

          {:error, detail} ->
            {:error, detail}
        end
    end
  end

  defp consensus_gate_satisfied?(%{
         consensus_ready?: consensus_ready,
         consensus_last_probe_at_ms: consensus_last_probe_at_ms
       }) do
    consensus_ready and consensus_probe_fresh?(consensus_last_probe_at_ms)
  end

  defp readiness_status_from_state(state) when is_map(state) do
    ready? = state.startup_complete? and consensus_gate_satisfied?(state)
    probe_fresh? = consensus_probe_fresh?(state.consensus_last_probe_at_ms)

    probe_detail =
      Map.merge(readiness_detail_base(state, probe_fresh?), state.consensus_probe_detail)

    {reason, detail} =
      cond do
        ready? ->
          {:ok, probe_detail}

        not state.startup_complete? ->
          {:startup_sync, probe_detail}

        true ->
          {:raft_sync, probe_detail}
      end

    %{
      ready?: ready?,
      reason: reason,
      detail: detail
    }
  end

  defp readiness_detail_base(state, probe_fresh?)
       when is_map(state) and is_boolean(probe_fresh?) do
    %{
      startup_mode: state.startup_mode,
      consensus_ready?: state.consensus_ready?,
      consensus_probe_fresh?: probe_fresh?,
      consensus_probe_age_ms: consensus_probe_age_ms(state.consensus_last_probe_at_ms),
      consensus_success_streak: state.consensus_probe_success_streak,
      consensus_failure_streak: state.consensus_probe_failure_streak
    }
  end

  defp readiness_status_fallback do
    %{
      ready?: false,
      reason: :bootstrap_down,
      detail: %{
        startup_mode: nil,
        consensus_ready?: false,
        consensus_probe_fresh?: false,
        consensus_probe_age_ms: nil,
        consensus_success_streak: 0,
        consensus_failure_streak: 0,
        checked_groups: 0,
        failing_group_id: nil,
        probe_result: "bootstrap_down"
      }
    }
  end

  defp consensus_probe_fresh?(consensus_last_probe_at_ms)
       when is_integer(consensus_last_probe_at_ms) do
    System.monotonic_time(:millisecond) - consensus_last_probe_at_ms <= @consensus_probe_ttl_ms
  end

  defp consensus_probe_fresh?(_consensus_last_probe_at_ms), do: false

  defp consensus_probe_age_ms(consensus_last_probe_at_ms)
       when is_integer(consensus_last_probe_at_ms) do
    max(System.monotonic_time(:millisecond) - consensus_last_probe_at_ms, 0)
  end

  defp consensus_probe_age_ms(_consensus_last_probe_at_ms), do: nil

  defp group_consensus_ready?(group_id) when is_integer(group_id) and group_id >= 0 do
    server_id = RaftManager.server_id(group_id)

    if group_running?(group_id) do
      case :ra.consistent_query(
             {server_id, Node.self()},
             &consensus_probe_query/1,
             @consensus_probe_timeout_ms
           ) do
        {:ok, _reply, {^server_id, leader_node}}
        when is_atom(leader_node) and not is_nil(leader_node) ->
          :ok

        {:timeout, leader} ->
          {:error,
           %{
             failing_group_id: group_id,
             probe_result: "timeout",
             leader: normalize_leader_hint(leader)
           }}

        {:error, reason} ->
          {:error,
           %{
             failing_group_id: group_id,
             probe_result: "error",
             error: inspect(reason)
           }}

        _ ->
          {:error,
           %{
             failing_group_id: group_id,
             probe_result: "unexpected"
           }}
      end
    else
      {:error, %{failing_group_id: group_id, probe_result: "local_group_down"}}
    end
  end

  defp consensus_probe_query(_state), do: :ok

  defp normalize_leader_hint({server_id, leader_node})
       when is_atom(server_id) and not is_nil(server_id) and is_atom(leader_node) and
              not is_nil(leader_node) do
    %{
      server_id: Atom.to_string(server_id),
      node: Atom.to_string(leader_node)
    }
  end

  defp normalize_leader_hint(other), do: inspect(other)

  defp bootstrap_coordinator? do
    case WriteNodes.nodes() do
      [first | _] -> Node.self() == first
      [] -> false
    end
  end
end
