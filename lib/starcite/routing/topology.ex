defmodule Starcite.Routing.Topology do
  @moduledoc """
  Static cluster-node inventory and replication-factor config.
  """

  @default_replication_factor 3
  @default_cluster_nodes [:nonode@nohost]
  @config_cache_key {__MODULE__, :config}

  @type config :: %{
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
    error in [ArgumentError] -> {:error, Exception.message(error)}
  end

  @spec config!() :: config()
  def config! do
    raw_replication_factor =
      Application.get_env(:starcite, :routing_replication_factor, @default_replication_factor)

    raw_nodes = Application.get_env(:starcite, :cluster_node_ids, @default_cluster_nodes)
    node_self = Node.self()
    cache_key = {raw_replication_factor, raw_nodes, node_self}

    case :persistent_term.get(@config_cache_key, :undefined) do
      {^cache_key, config} ->
        config

      _other ->
        config = build_config!(raw_replication_factor, raw_nodes, node_self)
        :persistent_term.put(@config_cache_key, {cache_key, config})
        config
    end
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

  defp build_config!(raw_replication_factor, raw_nodes, node_self) do
    replication_factor = validate_replication_factor!(raw_replication_factor)
    nodes = validate_nodes!(raw_nodes)

    if replication_factor > length(nodes) do
      raise ArgumentError,
            "invalid cluster-node config: routing_replication_factor=#{replication_factor} exceeds cluster_node_ids=#{length(nodes)}"
    end

    if node_self != :nonode@nohost and nodes == @default_cluster_nodes do
      raise ArgumentError,
            "invalid cluster-node config: CLUSTER_NODES must be configured for distributed nodes"
    end

    if node_self not in nodes do
      raise ArgumentError,
            "invalid cluster-node config: local node #{inspect(node_self)} is missing from cluster_node_ids=#{inspect(nodes)}"
    end

    %{
      replication_factor: replication_factor,
      nodes: nodes
    }
  end

  defp validate_replication_factor!(value) when is_integer(value) and value > 0, do: value

  defp validate_replication_factor!(value) do
    raise ArgumentError,
          "invalid value for :routing_replication_factor: #{inspect(value)} (expected positive integer)"
  end

  defp validate_nodes!(nodes) when is_list(nodes) and nodes != [] do
    if Enum.all?(nodes, &is_atom/1) do
      Enum.uniq(nodes)
    else
      raise ArgumentError,
            "invalid value for :cluster_node_ids: #{inspect(nodes)} (expected list of node atoms)"
    end
  end

  defp validate_nodes!(value) do
    raise ArgumentError,
          "invalid value for :cluster_node_ids: #{inspect(value)} (expected non-empty list of node atoms)"
  end
end
