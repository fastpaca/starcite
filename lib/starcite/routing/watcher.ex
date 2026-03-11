defmodule Starcite.Routing.Watcher do
  @moduledoc false

  use GenServer

  alias Starcite.DataPlane.SessionQuorum
  alias Starcite.Routing.Store

  @poll_interval_ms Application.compile_env(:starcite, :routing_watcher_interval_ms, 500)
  @handoff_rpc_timeout_ms Application.compile_env(
                            :starcite,
                            :routing_handoff_rpc_timeout_ms,
                            2_000
                          )
  @suspect_table :starcite_routing_suspects

  @spec suspect?(node()) :: boolean()
  def suspect?(node) when is_atom(node) do
    :ets.member(@suspect_table, node)
  rescue
    ArgumentError ->
      false
  end

  @spec run_once() :: :ok | {:error, :not_running}
  def run_once do
    call_if_running(:run_once, 5_000, {:error, :not_running})
  end

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    :net_kernel.monitor_nodes(true, node_type: :visible)
    :ok = ensure_suspect_table()
    {:ok, %{boot_rejoin_pending: true}, {:continue, :schedule_tick}}
  end

  @impl true
  def handle_continue(:schedule_tick, state) do
    {:noreply, state |> rejoin_local_node() |> schedule_tick()}
  end

  @impl true
  def handle_call(:run_once, _from, state) do
    next_state = run_tick(state)
    {:reply, :ok, next_state}
  end

  @impl true
  def handle_info(:tick, state) do
    next_state = run_tick(state)

    {:noreply, schedule_tick(next_state)}
  end

  def handle_info({:nodeup, node, _info}, state) when is_atom(node) do
    true = :ets.delete(@suspect_table, node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) when is_atom(node) do
    true = :ets.insert(@suspect_table, {node, true})
    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp run_tick(state) when is_map(state) do
    state = rejoin_local_node(state)
    :ok = renew_local_lease()
    start_drain_transfers()
    fail_over_expired_leases()
    reconcile_assignments()
    mark_local_drained()
    state
  end

  defp rejoin_local_node(%{boot_rejoin_pending: false} = state), do: state

  defp rejoin_local_node(%{boot_rejoin_pending: true} = state) do
    case Store.node_status(Node.self()) do
      :ready ->
        %{state | boot_rejoin_pending: false}

      status when status in [:draining, :drained] ->
        case Store.mark_node_ready(Node.self(), :startup) do
          :ok -> %{state | boot_rejoin_pending: false}
          {:error, _reason} -> state
        end

      _other ->
        state
    end
  end

  defp reconcile_assignments do
    case Store.all_assignments(:consistency) do
      {:ok, assignments} ->
        :ok = progress_transfers(assignments)
        prune_stale_logs(assignments)

      {:error, _reason} ->
        :ok
    end
  end

  defp process_assignment(
         {session_id,
          %{status: :moving, owner: owner, target_owner: target_owner, transfer_id: transfer_id}}
       )
       when is_binary(session_id) and session_id != "" and is_atom(owner) and
              is_atom(target_owner) and is_binary(transfer_id) and transfer_id != "" do
    if owner == Node.self() and target_owner != Node.self() do
      _ = handoff_session(session_id, target_owner, transfer_id)
    end

    :ok
  end

  defp process_assignment(_assignment), do: :ok

  defp progress_transfers(assignments) when is_map(assignments) do
    Enum.each(assignments, &process_assignment/1)
  end

  defp start_drain_transfers do
    if Store.node_status(Node.self()) == :draining do
      _ = Store.start_drain_transfers(Node.self())
    end

    :ok
  end

  defp renew_local_lease do
    _ = Store.renew_local_lease()
    :ok
  end

  defp fail_over_expired_leases do
    now_ms = System.system_time(:millisecond)

    Store.expired_nodes(now_ms)
    |> Enum.reject(&(&1 == Node.self()))
    |> Enum.each(fn expired_node ->
      _ = Store.reassign_sessions_from(expired_node)
    end)

    :ok
  end

  defp mark_local_drained do
    with :draining <- Store.node_status(Node.self()),
         {:ok, %{active_owned_sessions: 0, moving_sessions: 0}} <- Store.drain_status(Node.self()) do
      _ = Store.mark_node_drained(Node.self(), :watcher)
      :ok
    else
      _other -> :ok
    end
  end

  defp handoff_session(session_id, target_owner, transfer_id)
       when is_binary(session_id) and session_id != "" and is_atom(target_owner) and
              is_binary(transfer_id) and transfer_id != "" do
    with {:ok, %{session: session, events: events}} <- SessionQuorum.handoff_snapshot(session_id),
         :ok <- send_handoff(target_owner, session, events),
         {:ok, _assignment} <- Store.commit_transfer(session_id, transfer_id) do
      :ok = SessionQuorum.stop_session(session_id)
      :ok
    end
  end

  defp send_handoff(target_owner, session, events)
       when is_atom(target_owner) and is_list(events) do
    case :rpc.call(
           target_owner,
           SessionQuorum,
           :receive_handoff,
           [session, events],
           @handoff_rpc_timeout_ms
         ) do
      :ok -> :ok
      {:badrpc, reason} -> {:error, {:handoff_rpc_failed, target_owner, reason}}
      other -> {:error, {:invalid_handoff_reply, other}}
    end
  end

  defp prune_stale_logs(assignments) when is_map(assignments) do
    desired_ids = desired_local_session_ids(assignments)

    SessionQuorum.local_session_ids()
    |> Enum.each(fn session_id ->
      if MapSet.member?(desired_ids, session_id) do
        :ok
      else
        :ok = SessionQuorum.stop_session(session_id)
      end
    end)
  end

  defp desired_local_session_ids(assignments) when is_map(assignments) do
    Enum.reduce(assignments, MapSet.new(), fn
      {session_id, %{owner: owner, replicas: replicas, status: :active}}, acc
      when is_binary(session_id) and is_atom(owner) and is_list(replicas) ->
        if owner == Node.self() or Node.self() in replicas do
          MapSet.put(acc, session_id)
        else
          acc
        end

      {session_id,
       %{owner: owner, replicas: replicas, status: :moving, target_owner: target_owner}},
      acc
      when is_binary(session_id) and is_atom(owner) and is_list(replicas) and
             is_atom(target_owner) ->
        if owner == Node.self() or target_owner == Node.self() or Node.self() in replicas do
          MapSet.put(acc, session_id)
        else
          acc
        end

      _other, acc ->
        acc
    end)
  end

  defp schedule_tick(state) when is_map(state) do
    Process.send_after(self(), :tick, @poll_interval_ms)
    state
  end

  defp call_if_running(message, timeout, fallback)
       when is_integer(timeout) and timeout > 0 do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        try do
          GenServer.call(__MODULE__, message, timeout)
        catch
          :exit, _reason -> fallback
        end

      nil ->
        fallback
    end
  end

  defp ensure_suspect_table do
    _ =
      :ets.new(@suspect_table, [
        :named_table,
        :public,
        :set,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    true = :ets.delete_all_objects(@suspect_table)
    :ok
  rescue
    ArgumentError ->
      true = :ets.delete_all_objects(@suspect_table)
      :ok
  end
end
