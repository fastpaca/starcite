defmodule Starcite.Operations.Maintenance do
  @moduledoc false

  alias Starcite.Routing.{Observer, Topology}

  @spec drain_node(node()) :: :ok | {:error, :invalid_routing_node}
  def drain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_routing_node(node) do
      Observer.mark_node_draining(node)
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_routing_node}
  def undrain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_routing_node(node) do
      Observer.mark_node_ready(node)
    end
  end

  defp ensure_routing_node(node) when is_atom(node) do
    if Topology.routing_node?(node) do
      :ok
    else
      {:error, :invalid_routing_node}
    end
  end
end
