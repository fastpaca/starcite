defmodule Starcite.Operations do
  @moduledoc """
  Operator-facing helpers for routing administration.
  """

  alias Starcite.Operations.{Maintenance, Readiness}
  alias Starcite.Routing.{Store, Topology}

  @spec status() :: map()
  def status do
    %{
      node: Node.self(),
      local_cluster_node: true,
      local_mode: local_mode(),
      local_ready: local_ready(),
      local_drained: local_drained(),
      cluster_nodes: Topology.nodes(),
      routing_replication_factor: Topology.replication_factor(),
      routing_store: %{
        running?: Store.running?(),
        store_id: Store.store_id(),
        store_dir: Store.local_store_dir()
      },
      ready_nodes: ready_nodes()
    }
  end

  @spec ready_nodes() :: [node()]
  def ready_nodes, do: Store.ready_nodes()

  @spec node_status(node()) :: :ready | :draining | :drained | :unknown
  def node_status(node) when is_atom(node), do: Store.node_status(node)

  @spec drain_status(node()) ::
          {:ok, %{active_owned_sessions: non_neg_integer(), moving_sessions: non_neg_integer()}}
          | {:error, term()}
  def drain_status(node) when is_atom(node), do: Store.drain_status(node)

  @spec local_mode() :: :cluster_node
  def local_mode, do: :cluster_node

  @spec local_ready(keyword()) :: boolean()
  def local_ready(opts \\ []) when is_list(opts), do: Readiness.local_ready(opts)

  @spec local_readiness(keyword()) :: map()
  def local_readiness(opts \\ []) when is_list(opts), do: Readiness.local_readiness(opts)

  @spec local_drained() :: boolean()
  def local_drained, do: Readiness.local_drained()

  @spec wait_local_ready(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_ready(timeout_ms \\ 30_000) when is_integer(timeout_ms) and timeout_ms > 0 do
    Readiness.wait_local_ready(timeout_ms)
  end

  @spec wait_local_drained(pos_integer()) :: :ok | {:error, :timeout}
  def wait_local_drained(timeout_ms \\ 30_000) when is_integer(timeout_ms) and timeout_ms > 0 do
    Readiness.wait_local_drained(timeout_ms)
  end

  @spec drain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def drain_node(node \\ Node.self()) when is_atom(node), do: Maintenance.drain_node(node)

  @spec drain_node(node(), atom()) :: :ok | {:error, :invalid_cluster_node | term()}
  def drain_node(node, source) when is_atom(node) and is_atom(source),
    do: Maintenance.drain_node(node, source)

  @spec undrain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def undrain_node(node \\ Node.self()) when is_atom(node), do: Maintenance.undrain_node(node)

  @spec undrain_node(node(), atom()) :: :ok | {:error, :invalid_cluster_node | term()}
  def undrain_node(node, source) when is_atom(node) and is_atom(source),
    do: Maintenance.undrain_node(node, source)

  @spec known_nodes() :: [node()]
  def known_nodes, do: Topology.nodes()

  @spec parse_known_node(term()) :: {:ok, node()} | {:error, :invalid_cluster_node}
  def parse_known_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)

    case Enum.find(known_nodes(), fn node -> Atom.to_string(node) == node_name end) do
      nil -> {:error, :invalid_cluster_node}
      node -> {:ok, node}
    end
  end

  def parse_known_node(_raw_node), do: {:error, :invalid_cluster_node}
end
