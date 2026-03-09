defmodule Starcite.Operations.Maintenance do
  @moduledoc false

  alias Starcite.Routing.{Store, Topology, Watcher}

  @spec drain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def drain_node(node) when is_atom(node) do
    with :ok <- ensure_cluster_node(node),
         :ok <- Store.mark_node_draining(node) do
      _ = advance_local_drain(node)
      :ok
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def undrain_node(node) when is_atom(node) do
    with :ok <- ensure_cluster_node(node),
         :ok <- Store.mark_node_ready(node) do
      :ok
    end
  end

  defp ensure_cluster_node(node) when is_atom(node) do
    if(node in Topology.nodes(), do: :ok, else: {:error, :invalid_cluster_node})
  end

  defp advance_local_drain(node) when is_atom(node) do
    if node == Node.self() do
      _ = Watcher.run_once()
      Watcher.progress_local_transfers()
    else
      :ok
    end
  end
end
