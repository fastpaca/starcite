defmodule Starcite.Routing.LeaseBootstrap do
  @moduledoc """
  Topology bootstrap for static routing-node lease groups.

  Responsibilities:

  - Bootstrap groups from static routing-node config.
  - Start/join local assigned groups.
  - Maintain intrinsic lease readiness state used by health/routing gates.

  This module does not perform dynamic membership add/remove operations.
  """

  use GenServer
  require Logger

  alias Starcite.Operations.Leadership
  alias Starcite.Routing.Topology
  alias Starcite.Routing.{LeaseHealth, LeaseManager}

  @ready_call_timeout_ms 1_000
  @group_task_max_concurrency 32
  @group_task_timeout_ms 60_000
  @readiness_refresh_call_timeout_ms 5_000
  @runtime_reconcile_interval_ms 5_000

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
    fallback = LeaseHealth.readiness_status_fallback()
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
      "LeaseBootstrap: starting on #{Node.self()} (routing_node=#{Topology.routing_node?(Node.self())})"
    )

    send(self(), :bootstrap)
    schedule_runtime_reconcile()

    {:ok,
     %{
       startup_complete?: false,
       startup_mode: nil,
       sync_ref: nil,
       sync_pending?: false,
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
    next_state = LeaseHealth.maybe_refresh(state, true)
    status = LeaseHealth.readiness_status(next_state)
    {:reply, status.ready?, next_state}
  end

  @impl true
  def handle_call({:readiness_status, refresh?}, _from, state) when is_boolean(refresh?) do
    next_state = LeaseHealth.maybe_refresh(state, refresh?)
    {:reply, LeaseHealth.readiness_status(next_state), next_state}
  end

  @impl true
  def handle_call(:readiness_status, _from, state) do
    next_state = LeaseHealth.maybe_refresh(state, false)
    {:reply, LeaseHealth.readiness_status(next_state), next_state}
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
    {:noreply, LeaseHealth.record_write_outcome(state, outcome)}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    {:noreply, maybe_start_sync(state, :bootstrap)}
  end

  @impl true
  def handle_info({:startup_complete, mode}, state)
      when mode in [:routing, :ingress] do
    if state.startup_complete? do
      {:noreply, Map.put(state, :startup_mode, mode)}
    else
      Logger.info("LeaseBootstrap: startup complete (mode=#{mode})")

      if mode == :routing do
        :ok = emit_local_lease_role_counts()
      end

      {:noreply,
       state
       |> Map.put(:startup_complete?, true)
       |> Map.put(:startup_mode, mode)
       |> LeaseHealth.clear()}
    end
  end

  @impl true
  def handle_info({:nodedown, down_node, _reason}, state) when is_atom(down_node) do
    {:noreply, LeaseHealth.note_peer_down(state, down_node)}
  end

  @impl true
  def handle_info({:nodedown, down_node}, state) when is_atom(down_node) do
    {:noreply, LeaseHealth.note_peer_down(state, down_node)}
  end

  @impl true
  def handle_info({:nodeup, up_node, _info}, state) when is_atom(up_node) do
    {:noreply,
     state
     |> LeaseHealth.note_peer_up(up_node)
     |> maybe_start_sync_on_routing_node_change(up_node)}
  end

  @impl true
  def handle_info({:nodeup, up_node}, state) when is_atom(up_node) do
    {:noreply,
     state
     |> LeaseHealth.note_peer_up(up_node)
     |> maybe_start_sync_on_routing_node_change(up_node)}
  end

  @impl true
  def handle_info({ref, _result}, %{sync_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, maybe_start_pending_sync(%{state | sync_ref: nil})}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, %{sync_ref: ref} = state) do
    Logger.warning("LeaseBootstrap: bootstrap task failed: #{inspect(reason)}")
    {:noreply, maybe_start_pending_sync(%{state | sync_ref: nil})}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(:runtime_reconcile, %{sync_ref: sync_ref} = state)
      when is_reference(sync_ref) do
    schedule_runtime_reconcile()

    :ok = emit_local_lease_role_counts()
    {:noreply, state}
  end

  @impl true
  def handle_info(:runtime_reconcile, state) do
    schedule_runtime_reconcile()
    next_state = maybe_reconcile_local_groups(state)
    :ok = emit_local_lease_role_counts()
    {:noreply, next_state}
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

    %{state | sync_ref: task.ref, sync_pending?: false}
  end

  defp maybe_start_sync(state, _trigger), do: Map.put(state, :sync_pending?, true)

  defp maybe_start_pending_sync(%{sync_pending?: true} = state) do
    state
    |> Map.put(:sync_pending?, false)
    |> maybe_start_sync(:nodeup)
  end

  defp maybe_start_pending_sync(state), do: state

  defp run_sync(trigger, owner)
       when trigger in [:bootstrap, :nodeup] do
    case Topology.validate() do
      {:ok, _config} ->
        run_sync_for_valid_config(trigger, owner)

      {:error, reason} ->
        raise ArgumentError,
              "LeaseBootstrap bootstrap aborted due to invalid routing-node config: #{reason}"
    end
  end

  defp run_sync_for_valid_config(trigger, owner) do
    if Topology.routing_node?(Node.self()) do
      my_groups = compute_my_groups()

      Logger.info(
        "LeaseBootstrap: reconciling #{length(my_groups)} local routing groups (trigger=#{trigger})"
      )

      run_groups_parallel(my_groups, "ensure-local", &ensure_local_group_running/1)

      send(owner, {:startup_complete, :routing})
    else
      send(owner, {:startup_complete, :ingress})
    end

    :ok
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

  defp ensure_local_group_running(group_id) do
    if LeaseManager.should_participate?(group_id) and not group_running?(group_id) do
      LeaseManager.start_group(group_id)
    else
      :ok
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
        "LeaseBootstrap: #{label} processed #{total} groups in #{duration_ms}ms (max_concurrency=#{max_concurrency})"
      )
    end
  end

  defp handle_group_result({:ok, {_group_id, :ok}}, _label), do: :ok

  defp handle_group_result({:ok, {group_id, {:error, reason}}}, label) do
    Logger.debug("LeaseBootstrap: #{label} group #{group_id} failed: #{inspect(reason)}")
  end

  defp handle_group_result({:ok, {_group_id, _result}}, _label), do: :ok

  defp handle_group_result({:exit, reason}, label) do
    Logger.warning("LeaseBootstrap: #{label} worker crashed: #{inspect(reason)}")
  end

  defp maybe_start_sync_on_routing_node_change(state, up_node) when is_atom(up_node) do
    if Topology.routing_node?(Node.self()) and up_node in Topology.nodes() do
      maybe_start_sync(state, :nodeup)
    else
      state
    end
  end

  defp compute_my_groups do
    for group_id <- 0..(Topology.num_groups() - 1),
        LeaseManager.should_participate?(group_id),
        do: group_id
  end

  defp group_running?(group_id) do
    Process.whereis(LeaseManager.server_id(group_id)) != nil
  end

  defp configure_ra_system_storage do
    ra_system_dir = LeaseManager.ra_system_data_dir()
    wal_data_dir = LeaseManager.ra_wal_data_dir()

    with :ok <- File.mkdir_p(ra_system_dir),
         :ok <- File.mkdir_p(wal_data_dir) do
      Application.put_env(:ra, :data_dir, String.to_charlist(ra_system_dir))
      Application.put_env(:ra, :wal_data_dir, String.to_charlist(wal_data_dir))
      :ok
    else
      {:error, reason} ->
        raise ArgumentError,
              "failed to prepare :ra storage directories #{inspect(ra_system_dir)} / #{inspect(wal_data_dir)}: #{inspect(reason)}"
    end
  end

  defp maybe_reconcile_local_groups(%{startup_complete?: true} = state) do
    if Topology.routing_node?(Node.self()) do
      local_groups = compute_my_groups()
      down_groups = Enum.reject(local_groups, &group_running?/1)

      if down_groups == [] do
        state
      else
        Logger.warning(
          "LeaseBootstrap: detected #{length(down_groups)} local groups down, attempting recovery"
        )

        run_groups_parallel(down_groups, "runtime-reconcile", &ensure_local_group_running/1)
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

  defp emit_local_lease_role_counts do
    if Topology.routing_node?(Node.self()) do
      :ok = Leadership.emit_local_lease_role_telemetry()
    end

    :ok
  end
end
