defmodule Starcite.ControlPlane.WriteNodes do
  @moduledoc """
  Static write-node configuration and validation for the control plane.

  This module is the single source of truth for:

  - write node ids
  - write replication factor
  - number of write groups
  """

  @default_num_groups 256
  @default_replication_factor 3
  @default_write_nodes [:nonode@nohost]

  @type config :: %{
          num_groups: pos_integer(),
          replication_factor: pos_integer(),
          nodes: [node()]
        }

  @spec validate!() :: :ok
  def validate! do
    _ = config!()
    :ok
  end

  @spec validate() :: {:ok, config()} | {:error, String.t()}
  def validate do
    {:ok, config!()}
  rescue
    error in [ArgumentError] ->
      {:error, Exception.message(error)}
  end

  @spec config!() :: config()
  def config! do
    num_groups = read_num_groups!()
    replication_factor = read_replication_factor!()
    nodes = read_nodes!()

    if replication_factor > length(nodes) do
      raise ArgumentError,
            "invalid write-node config: write_replication_factor=#{replication_factor} exceeds write_node_ids=#{length(nodes)}"
    end

    if Node.self() != :nonode@nohost and nodes == @default_write_nodes do
      raise ArgumentError,
            "invalid write-node config: STARCITE_WRITE_NODE_IDS must be configured for distributed nodes"
    end

    %{
      num_groups: num_groups,
      replication_factor: replication_factor,
      nodes: nodes
    }
  end

  @spec num_groups() :: pos_integer()
  def num_groups do
    %{num_groups: num_groups} = config!()
    num_groups
  end

  @spec replication_factor() :: pos_integer()
  def replication_factor do
    %{replication_factor: replication_factor} = config!()
    replication_factor
  end

  @spec nodes() :: [node()]
  def nodes do
    %{nodes: nodes} = config!()
    nodes
  end

  @spec write_node?(node()) :: boolean()
  def write_node?(node) when is_atom(node) do
    node in nodes()
  end

  defp read_num_groups! do
    case Application.get_env(:starcite, :num_groups, @default_num_groups) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :num_groups: #{inspect(value)} (expected positive integer)"
    end
  end

  defp read_replication_factor! do
    case Application.get_env(:starcite, :write_replication_factor, @default_replication_factor) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :write_replication_factor: #{inspect(value)} (expected positive integer)"
    end
  end

  defp read_nodes! do
    case Application.get_env(:starcite, :write_node_ids, @default_write_nodes) do
      nodes when is_list(nodes) and nodes != [] ->
        if Enum.all?(nodes, &is_atom/1) do
          Enum.uniq(nodes)
        else
          raise ArgumentError,
                "invalid value for :write_node_ids: #{inspect(nodes)} (expected list of node atoms)"
        end

      value ->
        raise ArgumentError,
              "invalid value for :write_node_ids: #{inspect(value)} (expected non-empty list of node atoms)"
    end
  end
end
