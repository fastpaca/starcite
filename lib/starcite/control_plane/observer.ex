defmodule Starcite.ControlPlane.Observer do
  @moduledoc """
  Liveness observer used by request-path routing.

  Tracks node visibility and status transitions without mutating Raft membership.
  """

  use GenServer

  alias Starcite.ControlPlane.ObserverState
  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.RaftBootstrap

  @status_call_timeout_ms 1_000
  @suspect_to_lost_ms 120_000
  @maintenance_interval_ms 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns all currently visible distributed nodes."
  def all_nodes do
    [Node.self() | Node.list()] |> Enum.uniq() |> Enum.sort()
  end

  @doc "Returns write nodes currently eligible for routing."
  def ready_nodes do
    fallback = fallback_ready_nodes()

    case Process.whereis(__MODULE__) do
      nil -> fallback
      pid -> safe_call(pid, :ready_nodes, fallback)
    end
  end

  @doc false
  def route_candidates(replicas) when is_list(replicas) do
    fallback = fallback_route_candidates(replicas)

    case Process.whereis(__MODULE__) do
      nil -> fallback
      pid -> safe_call(pid, {:route_candidates, replicas}, fallback)
    end
  end

  @doc "Returns observer diagnostics status."
  def status do
    fallback = %{
      status: :down,
      visible_nodes: all_nodes(),
      ready_nodes: fallback_ready_nodes(),
      node_statuses: %{},
      raft_ready_nodes: []
    }

    case Process.whereis(__MODULE__) do
      nil ->
        fallback

      pid ->
        safe_call(pid, :status, fallback)
    end
  end

  @doc "Marks a node as draining (excluded from ready routing set)."
  def mark_node_draining(node \\ Node.self()) when is_atom(node) do
    propagate_node_status(node, :draining)
  end

  @doc "Marks a node as ready (eligible for routing)."
  def mark_node_ready(node \\ Node.self()) when is_atom(node) do
    propagate_node_status(node, :ready)
  end

  @doc false
  def apply_node_status(node, status)
      when is_atom(node) and status in [:ready, :draining] do
    GenServer.cast(__MODULE__, {:set_node_status, node, status})
    :ok
  end

  @impl true
  def init(_opts) do
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)

    now_ms = System.monotonic_time(:millisecond)

    observer =
      all_nodes()
      |> ObserverState.new(now_ms)
      |> recover_draining_nodes(now_ms)

    schedule_maintenance()

    raft_ready_nodes = refresh_raft_ready_nodes(all_nodes())

    {:ok, %{observer: observer, raft_ready_nodes: raft_ready_nodes}}
  end

  @impl true
  def handle_call(
        :ready_nodes,
        _from,
        %{observer: observer, raft_ready_nodes: raft_ready_nodes} = state
      ) do
    {:reply, ready_nodes_from_observer(observer, raft_ready_nodes), state}
  end

  @impl true
  def handle_call(
        {:route_candidates, replicas},
        _from,
        %{observer: observer, raft_ready_nodes: raft_ready_nodes} = state
      )
      when is_list(replicas) do
    {:reply, route_candidates_from_observer(observer, raft_ready_nodes, replicas), state}
  end

  @impl true
  def handle_call(
        :status,
        _from,
        %{observer: observer, raft_ready_nodes: raft_ready_nodes} = state
      ) do
    visible_nodes = all_nodes()

    node_statuses =
      observer.nodes
      |> Enum.into(%{}, fn {node, %{status: status, changed_at_ms: changed_at_ms}} ->
        {node, %{status: status, changed_at_ms: changed_at_ms}}
      end)

    reply = %{
      status: :ok,
      visible_nodes: visible_nodes,
      ready_nodes: ready_nodes_from_observer(observer, raft_ready_nodes),
      node_statuses: node_statuses,
      raft_ready_nodes: raft_ready_nodes |> MapSet.to_list() |> Enum.sort()
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_cast({:mark_node_draining, node}, state) do
    handle_cast({:set_node_status, node, :draining}, state)
  end

  @impl true
  def handle_cast({:mark_node_ready, node}, state) do
    handle_cast({:set_node_status, node, :ready}, state)
  end

  @impl true
  def handle_cast({:set_node_status, node, :draining}, %{observer: observer} = state) do
    now_ms = System.monotonic_time(:millisecond)
    next = ObserverState.mark_draining(observer, node, now_ms)
    {:noreply, %{state | observer: next}}
  end

  @impl true
  def handle_cast({:set_node_status, node, :ready}, %{observer: observer} = state) do
    now_ms = System.monotonic_time(:millisecond)
    next = ObserverState.mark_ready(observer, node, now_ms)
    {:noreply, %{state | observer: next}}
  end

  @impl true
  def handle_cast(_message, state), do: {:noreply, state}

  @impl true
  def handle_info({:nodeup, node, _info}, %{observer: observer} = state) do
    now_ms = System.monotonic_time(:millisecond)

    next =
      case observer.nodes[node] do
        %{status: :draining} ->
          observer

        _ ->
          ObserverState.mark_ready(observer, node, now_ms)
      end

    {:noreply, %{state | observer: next}}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, %{observer: observer} = state) do
    now_ms = System.monotonic_time(:millisecond)
    next = ObserverState.mark_suspect(observer, node, now_ms)
    {:noreply, %{state | observer: next}}
  end

  @impl true
  def handle_info(:maintenance, %{observer: observer} = state) do
    now_ms = System.monotonic_time(:millisecond)
    visible_nodes = all_nodes()

    next =
      observer
      |> ObserverState.refresh(visible_nodes, now_ms)
      |> ObserverState.advance_timeouts(now_ms, @suspect_to_lost_ms)

    raft_ready_nodes = refresh_raft_ready_nodes(visible_nodes)

    schedule_maintenance()

    {:noreply, %{state | observer: next, raft_ready_nodes: raft_ready_nodes}}
  end

  defp schedule_maintenance do
    Process.send_after(self(), :maintenance, @maintenance_interval_ms)
  end

  defp propagate_node_status(node, status)
       when is_atom(node) and status in [:ready, :draining] do
    :ok = apply_node_status(node, status)

    Enum.each(Node.list(), fn remote ->
      :rpc.cast(remote, __MODULE__, :apply_node_status, [node, status])
    end)

    :ok
  end

  defp recover_draining_nodes(observer, now_ms)
       when is_struct(observer, ObserverState) and is_integer(now_ms) do
    Enum.reduce(Node.list(), observer, fn remote, acc ->
      case :rpc.call(remote, __MODULE__, :status, [], @status_call_timeout_ms) do
        %{status: :ok, node_statuses: node_statuses} when is_map(node_statuses) ->
          Enum.reduce(node_statuses, acc, fn
            {node, %{status: :draining}}, next when is_atom(node) ->
              ObserverState.mark_draining(next, node, now_ms)

            {_node, _meta}, next ->
              next
          end)

        _other ->
          acc
      end
    end)
  end

  defp safe_call(pid, message, fallback) do
    GenServer.call(pid, message, @status_call_timeout_ms)
  catch
    :exit, _reason -> fallback
  end

  defp ready_nodes_from_observer(%ObserverState{} = observer, raft_ready_nodes)
       when is_map(raft_ready_nodes) do
    ready =
      observer.nodes
      |> Enum.filter(fn {_node, %{status: status}} -> status == :ready end)
      |> Enum.map(fn {node, _meta} -> node end)
      |> Enum.filter(&MapSet.member?(raft_ready_nodes, &1))
      |> Enum.sort()

    write_nodes = WriteNodes.nodes()
    Enum.filter(ready, &(&1 in write_nodes))
  end

  defp route_candidates_from_observer(%ObserverState{} = observer, raft_ready_nodes, replicas)
       when is_map(raft_ready_nodes) and is_list(replicas) do
    write_replicas = Enum.filter(replicas, &WriteNodes.write_node?/1)

    ready =
      write_replicas
      |> Enum.filter(fn node ->
        MapSet.member?(raft_ready_nodes, node) and node_status(observer, node) == :ready
      end)

    fallbacks =
      write_replicas
      |> Enum.filter(fn node ->
        status = node_status(observer, node)
        status != :draining
      end)
      |> Enum.reject(&(&1 in ready))

    %{ready: Enum.uniq(ready), fallbacks: Enum.uniq(fallbacks)}
  end

  defp node_status(%ObserverState{nodes: nodes}, node) when is_map(nodes) and is_atom(node) do
    case nodes do
      %{^node => %{status: status}} -> status
      _ -> nil
    end
  end

  defp refresh_raft_ready_nodes(visible_nodes) when is_list(visible_nodes) do
    visible = MapSet.new(visible_nodes)

    WriteNodes.nodes()
    |> Enum.filter(&MapSet.member?(visible, &1))
    |> Enum.reduce(MapSet.new(), fn node, acc ->
      if raft_node_ready?(node) do
        MapSet.put(acc, node)
      else
        acc
      end
    end)
  end

  defp raft_node_ready?(node) when is_atom(node) do
    if node == Node.self() do
      if Node.self() == :nonode@nohost do
        true
      else
        RaftBootstrap.ready?()
      end
    else
      :rpc.call(node, RaftBootstrap, :ready?, [], @status_call_timeout_ms) == true
    end
  end

  defp fallback_ready_nodes do
    visible_nodes = all_nodes()
    now_ms = System.monotonic_time(:millisecond)
    observer = ObserverState.new(visible_nodes, now_ms)
    raft_ready_nodes = refresh_raft_ready_nodes(visible_nodes)
    ready_nodes_from_observer(observer, raft_ready_nodes)
  end

  defp fallback_route_candidates(replicas) when is_list(replicas) do
    visible_nodes = all_nodes()
    now_ms = System.monotonic_time(:millisecond)
    observer = ObserverState.new(visible_nodes, now_ms)
    raft_ready_nodes = refresh_raft_ready_nodes(visible_nodes)
    route_candidates_from_observer(observer, raft_ready_nodes, replicas)
  end
end
