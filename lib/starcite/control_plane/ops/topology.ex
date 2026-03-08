defmodule Starcite.ControlPlane.Ops.Topology do
  @moduledoc false

  alias Starcite.ControlPlane.{Observer, WriteNodes}
  alias Starcite.DataPlane.RaftManager

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

  @spec local_write_groups() :: [non_neg_integer()]
  def local_write_groups do
    for group_id <- 0..(WriteNodes.num_groups() - 1),
        RaftManager.should_participate?(group_id),
        do: group_id
  end

  @spec group_replicas(non_neg_integer()) :: [node()]
  def group_replicas(group_id) when is_integer(group_id) and group_id >= 0 do
    case parse_group_id(group_id) do
      {:ok, valid_group_id} ->
        RaftManager.replicas_for_group(valid_group_id)

      {:error, :invalid_group_id} ->
        raise ArgumentError, "invalid group_id: #{inspect(group_id)}"
    end
  end

  @spec known_nodes() :: [node()]
  def known_nodes do
    WriteNodes.nodes()
  end

  @spec parse_known_node(term()) :: {:ok, node()} | {:error, :invalid_write_node}
  def parse_known_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)

    case Enum.find(known_nodes(), fn node -> Atom.to_string(node) == node_name end) do
      nil -> {:error, :invalid_write_node}
      node -> {:ok, node}
    end
  end

  def parse_known_node(_raw_node), do: {:error, :invalid_write_node}

  @spec parse_group_id(term()) :: {:ok, non_neg_integer()} | {:error, :invalid_group_id}
  def parse_group_id(raw_group_id) when is_binary(raw_group_id) do
    case Integer.parse(String.trim(raw_group_id)) do
      {group_id, ""} when group_id >= 0 ->
        parse_group_id(group_id)

      _ ->
        {:error, :invalid_group_id}
    end
  end

  def parse_group_id(group_id) when is_integer(group_id) and group_id >= 0 do
    if group_id < WriteNodes.num_groups() do
      {:ok, group_id}
    else
      {:error, :invalid_group_id}
    end
  end

  def parse_group_id(_raw_group_id), do: {:error, :invalid_group_id}
end
