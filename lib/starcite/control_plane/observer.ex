defmodule Starcite.ControlPlane.Observer do
  @moduledoc """
  Liveness observer used by request-path routing.

  Tracks node visibility and status transitions without mutating Raft membership.
  """

  use GenServer

  alias Starcite.ControlPlane.ObserverState
  alias Starcite.ControlPlane.WriteNodes

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
    fallback =
      WriteNodes.nodes()
      |> Enum.filter(&(&1 in all_nodes()))

    case Process.whereis(__MODULE__) do
      nil -> fallback
      pid -> safe_call(pid, :ready_nodes, fallback)
    end
  end

  @doc "Returns observer diagnostics status."
  def status do
    fallback = %{status: :down, visible_nodes: all_nodes(), ready_nodes: [], node_statuses: %{}}

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

    {:ok, %{observer: observer}}
  end

  @impl true
  def handle_call(:ready_nodes, _from, %{observer: observer} = state) do
    {:reply, ready_nodes_from_observer(observer), state}
  end

  @impl true
  def handle_call(:status, _from, %{observer: observer} = state) do
    visible_nodes = all_nodes()

    node_statuses =
      observer.nodes
      |> Enum.into(%{}, fn {node, %{status: status, changed_at_ms: changed_at_ms}} ->
        {node, %{status: status, changed_at_ms: changed_at_ms}}
      end)

    reply = %{
      status: :ok,
      visible_nodes: visible_nodes,
      ready_nodes: ready_nodes_from_observer(observer),
      node_statuses: node_statuses
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

    next =
      observer
      |> ObserverState.refresh(all_nodes(), now_ms)
      |> ObserverState.advance_timeouts(now_ms, @suspect_to_lost_ms)

    schedule_maintenance()

    {:noreply, %{state | observer: next}}
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

  defp ready_nodes_from_observer(%ObserverState{} = observer) do
    ready =
      observer.nodes
      |> Enum.filter(fn {_node, %{status: status}} -> status == :ready end)
      |> Enum.map(fn {node, _meta} -> node end)
      |> Enum.sort()

    write_nodes = WriteNodes.nodes()
    Enum.filter(ready, &(&1 in write_nodes))
  end
end
