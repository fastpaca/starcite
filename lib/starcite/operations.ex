defmodule Starcite.Operations do
  @moduledoc """
  Operator-facing helpers for routing and lease administration.

  This module is a thin facade over focused operations modules so callers keep a
  stable entrypoint while the operational surface grows.
  """

  alias Starcite.Routing.{Observer, Topology}
  alias Starcite.Operations.{Leadership, Maintenance, Readiness}
  alias Starcite.Routing.LeaseManager

  @spec status() :: map()
  def status do
    %{
      node: Node.self(),
      local_routing_node: Topology.routing_node?(Node.self()),
      local_mode: local_mode(),
      local_ready: local_ready(),
      local_drained: local_drained(),
      routing_nodes: Topology.nodes(),
      routing_replication_factor: Topology.replication_factor(),
      num_groups: Topology.num_groups(),
      local_routing_groups: local_routing_groups(),
      local_leader_groups: local_leader_groups(),
      lease_role_counts: lease_role_counts(),
      lease_storage: lease_storage_status(),
      observer: Observer.status()
    }
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes do
    Observer.ready_nodes()
  end

  @spec local_mode() :: :routing_node | :ingress_node
  def local_mode do
    if Topology.routing_node?(Node.self()) do
      :routing_node
    else
      :ingress_node
    end
  end

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts) do
    Readiness.local_ready(opts)
  end

  @spec local_readiness(keyword()) :: map()
  def local_readiness(opts \\ []) when is_list(opts) do
    Readiness.local_readiness(opts)
  end

  @spec local_drained() :: boolean()
  def local_drained do
    Readiness.local_drained()
  end

  @spec wait_local_ready(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_ready(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    Readiness.wait_local_ready(timeout_ms)
  end

  @spec wait_local_drained(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_drained(timeout_ms \\ 30_000)
      when is_integer(timeout_ms) and timeout_ms > 0 do
    Readiness.wait_local_drained(timeout_ms)
  end

  @spec drain_node(node()) :: :ok | {:error, :invalid_routing_node}
  def drain_node(node \\ Node.self()) when is_atom(node) do
    Maintenance.drain_node(node)
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_routing_node}
  def undrain_node(node \\ Node.self()) when is_atom(node) do
    Maintenance.undrain_node(node)
  end

  @spec local_routing_groups() :: [non_neg_integer()]
  def local_routing_groups do
    for group_id <- 0..(Topology.num_groups() - 1),
        LeaseManager.should_participate?(group_id),
        do: group_id
  end

  @spec group_replicas(non_neg_integer()) :: [node()]
  def group_replicas(group_id) when is_integer(group_id) and group_id >= 0 do
    case parse_group_id(group_id) do
      {:ok, valid_group_id} ->
        LeaseManager.replicas_for_group(valid_group_id)

      {:error, :invalid_group_id} ->
        raise ArgumentError, "invalid group_id: #{inspect(group_id)}"
    end
  end

  @spec known_nodes() :: [node()]
  def known_nodes do
    Topology.nodes()
  end

  @spec parse_known_node(term()) :: {:ok, node()} | {:error, :invalid_routing_node}
  def parse_known_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)

    case Enum.find(known_nodes(), fn node -> Atom.to_string(node) == node_name end) do
      nil -> {:error, :invalid_routing_node}
      node -> {:ok, node}
    end
  end

  def parse_known_node(_raw_node), do: {:error, :invalid_routing_node}

  @spec parse_group_id(term()) :: {:ok, non_neg_integer()} | {:error, :invalid_group_id}
  def parse_group_id(raw_group_id) when is_binary(raw_group_id) do
    case Integer.parse(String.trim(raw_group_id)) do
      {group_id, ""} when group_id >= 0 ->
        parse_group_id(group_id)

      _ ->
        {:error, :invalid_group_id}
    end
  end

  def parse_group_id(raw_group_id) do
    if is_integer(raw_group_id) and raw_group_id >= 0 and raw_group_id < Topology.num_groups() do
      {:ok, raw_group_id}
    else
      {:error, :invalid_group_id}
    end
  end

  @spec local_lease_group_states() :: [map()]
  def local_lease_group_states do
    Leadership.local_lease_group_states()
  end

  @spec lease_role_counts() :: %{
          leader: non_neg_integer(),
          follower: non_neg_integer(),
          candidate: non_neg_integer(),
          other: non_neg_integer(),
          down: non_neg_integer()
        }
  def lease_role_counts do
    Leadership.lease_role_counts()
  end

  @spec local_leader_groups() :: [non_neg_integer()]
  def local_leader_groups do
    Leadership.local_leader_groups()
  end

  @spec group_leader(non_neg_integer()) :: node() | nil
  def group_leader(group_id) when is_integer(group_id) and group_id >= 0 do
    Leadership.group_leader(group_id)
  end

  @spec transfer_group_leadership(non_neg_integer(), node()) ::
          :ok | :already_leader | {:error, term()} | {:timeout, term()}
  def transfer_group_leadership(group_id, target_node)
      when is_integer(group_id) and group_id >= 0 and is_atom(target_node) do
    Leadership.transfer_group_leadership(group_id, target_node)
  end

  def transfer_group_leadership(_group_id, _target_node), do: {:error, :invalid_args}

  @spec wait_group_leader(non_neg_integer(), node(), pos_integer()) :: :ok | {:error, term()}
  def wait_group_leader(group_id, target_node, timeout_ms)
      when is_integer(group_id) and group_id >= 0 and is_atom(target_node) and
             is_integer(timeout_ms) and timeout_ms > 0 do
    Leadership.wait_group_leader(group_id, target_node, timeout_ms)
  end

  def wait_group_leader(_group_id, _target_node, _timeout_ms), do: {:error, :invalid_args}

  defp lease_storage_status do
    %{
      starcite_data_dir: LeaseManager.raft_data_dir_root(),
      ra_data_dir: normalize_raft_path(Application.get_env(:ra, :data_dir)),
      ra_wal_data_dir: normalize_raft_path(Application.get_env(:ra, :wal_data_dir))
    }
  end

  defp normalize_raft_path(value) when is_binary(value), do: value

  defp normalize_raft_path(value) when is_list(value) do
    if List.ascii_printable?(value), do: List.to_string(value), else: inspect(value)
  end

  defp normalize_raft_path(nil), do: nil
  defp normalize_raft_path(value), do: inspect(value)
end
