defmodule Starcite.ControlPlane.WriteNodes do
  @moduledoc """
  Static write-node configuration and validation for the control plane.

  This module is the single source of truth for:

  - write node ids
  - write replication factor
  - number of write groups
  """

  @default_num_groups 32
  @default_replication_factor 3
  @default_write_nodes [:nonode@nohost]
  @config_cache_key {__MODULE__, :config}

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
    raw_num_groups = Application.get_env(:starcite, :num_groups, @default_num_groups)

    raw_replication_factor =
      Application.get_env(:starcite, :write_replication_factor, @default_replication_factor)

    raw_nodes = Application.get_env(:starcite, :write_node_ids, @default_write_nodes)
    node_self = Node.self()
    cache_key = {raw_num_groups, raw_replication_factor, raw_nodes, node_self}

    case :persistent_term.get(@config_cache_key, :undefined) do
      {^cache_key, config} ->
        config

      _ ->
        config =
          build_config!(
            raw_num_groups,
            raw_replication_factor,
            raw_nodes,
            node_self
          )

        :persistent_term.put(@config_cache_key, {cache_key, config})
        config
    end
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

  defp build_config!(raw_num_groups, raw_replication_factor, raw_nodes, node_self) do
    num_groups = validate_num_groups!(raw_num_groups)
    replication_factor = validate_replication_factor!(raw_replication_factor)
    nodes = validate_nodes!(raw_nodes)

    if replication_factor > length(nodes) do
      raise ArgumentError,
            "invalid write-node config: write_replication_factor=#{replication_factor} exceeds write_node_ids=#{length(nodes)}"
    end

    if node_self != :nonode@nohost and nodes == @default_write_nodes do
      raise ArgumentError,
            "invalid write-node config: STARCITE_WRITE_NODE_IDS must be configured for distributed nodes"
    end

    %{
      num_groups: num_groups,
      replication_factor: replication_factor,
      nodes: nodes
    }
  end

  defp validate_num_groups!(value) when is_integer(value) and value > 0, do: value

  defp validate_num_groups!(value) do
    raise ArgumentError,
          "invalid value for :num_groups: #{inspect(value)} (expected positive integer)"
  end

  defp validate_replication_factor!(value) when is_integer(value) and value > 0, do: value

  defp validate_replication_factor!(value) do
    raise ArgumentError,
          "invalid value for :write_replication_factor: #{inspect(value)} (expected positive integer)"
  end

  defp validate_nodes!(nodes) when is_list(nodes) and nodes != [] do
    if Enum.all?(nodes, &is_atom/1) do
      Enum.uniq(nodes)
    else
      raise ArgumentError,
            "invalid value for :write_node_ids: #{inspect(nodes)} (expected list of node atoms)"
    end
  end

  defp validate_nodes!(value) do
    raise ArgumentError,
          "invalid value for :write_node_ids: #{inspect(value)} (expected non-empty list of node atoms)"
  end
end
