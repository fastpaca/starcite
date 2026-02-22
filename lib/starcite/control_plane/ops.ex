defmodule Starcite.ControlPlane.Ops do
  @moduledoc """
  Operator-facing helpers for static write-node control and status.

  These helpers affect routing eligibility only. They do not mutate Raft
  membership.
  """

  alias Starcite.ControlPlane.Observer
  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.WritePath.{RaftManager, RaftTopology}

  @default_wait_interval_ms 200

  @spec status() :: map()
  def status do
    local_mode = local_mode()

    %{
      node: Node.self(),
      local_write_node: WriteNodes.write_node?(Node.self()),
      local_mode: local_mode,
      local_ready: local_ready(),
      local_drained: local_drained(),
      write_nodes: WriteNodes.nodes(),
      write_replication_factor: WriteNodes.replication_factor(),
      num_groups: WriteNodes.num_groups(),
      local_groups: local_write_groups(),
      observer: Observer.status()
    }
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes do
    Observer.ready_nodes()
  end

  @spec local_mode() :: :write_node | :router_node
  def local_mode do
    if WriteNodes.write_node?(Node.self()) do
      :write_node
    else
      :router_node
    end
  end

  @spec local_ready() :: boolean()
  def local_ready do
    topology_ready = RaftTopology.ready?()

    case local_mode() do
      :write_node ->
        topology_ready and Node.self() in Observer.ready_nodes()

      :router_node ->
        topology_ready
    end
  end

  @spec local_drained() :: boolean()
  def local_drained do
    local_mode() == :write_node and Node.self() not in Observer.ready_nodes()
  end

  @spec wait_local_ready(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_ready(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(&local_ready/0, timeout_ms)
  end

  @spec wait_local_drained(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_drained(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    wait_until(&local_drained/0, timeout_ms)
  end

  @spec drain_node(node()) :: :ok
  def drain_node(node \\ Node.self()) when is_atom(node) do
    Observer.mark_node_draining(node)
    :ok
  end

  @spec undrain_node(node()) :: :ok
  def undrain_node(node \\ Node.self()) when is_atom(node) do
    Observer.mark_node_ready(node)
    :ok
  end

  @spec local_write_groups() :: [non_neg_integer()]
  def local_write_groups do
    for group_id <- 0..(WriteNodes.num_groups() - 1),
        RaftManager.should_participate?(group_id),
        do: group_id
  end

  @spec group_replicas(non_neg_integer()) :: [node()]
  def group_replicas(group_id) when is_integer(group_id) and group_id >= 0 do
    RaftManager.replicas_for_group(group_id)
  end

  defp wait_until(predicate, timeout_ms)
       when is_function(predicate, 0) and is_integer(timeout_ms) and timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(predicate, deadline_ms)
  end

  defp do_wait_until(predicate, deadline_ms)
       when is_function(predicate, 0) and is_integer(deadline_ms) do
    if predicate.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline_ms do
        {:error, :timeout}
      else
        Process.sleep(@default_wait_interval_ms)
        do_wait_until(predicate, deadline_ms)
      end
    end
  end
end
