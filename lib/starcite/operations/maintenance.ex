defmodule Starcite.Operations.Maintenance do
  @moduledoc false

  alias Starcite.Routing.{Store, Topology, Watcher}

  @spec drain_node(node()) :: :ok | {:error, :invalid_routing_node | term()}
  def drain_node(node) when is_atom(node) do
    with :ok <- ensure_routing_node(node),
         :ok <- Store.mark_node_draining(node) do
      _ = maybe_kick_local_watcher(node)
      _ = maybe_progress_local_transfers(node)
      :ok
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_routing_node | term()}
  def undrain_node(node) when is_atom(node) do
    with :ok <- ensure_routing_node(node),
         :ok <- Store.mark_node_ready(node) do
      :ok
    end
  end

  defp ensure_routing_node(node) when is_atom(node) do
    if node in Topology.nodes() do
      :ok
    else
      {:error, :invalid_routing_node}
    end
  end

  defp maybe_kick_local_watcher(node) do
    if node == Node.self() do
      Watcher.run_once()
    else
      :ok
    end
  end

  defp maybe_progress_local_transfers(node) do
    if node == Node.self() do
      Watcher.progress_local_transfers()
    else
      :ok
    end
  end
end
