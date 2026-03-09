defmodule Starcite.Operations.Maintenance do
  @moduledoc false

  alias Starcite.Routing.{Store, Topology}

  @spec drain_node(node()) :: :ok | {:error, :invalid_routing_node | term()}
  def drain_node(node) when is_atom(node) do
    with :ok <- ensure_routing_node(node),
         :ok <- Store.mark_node_draining(node),
         {:ok, _moved} <- Store.reassign_sessions_from(node) do
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
    if Topology.routing_node?(node) do
      :ok
    else
      {:error, :invalid_routing_node}
    end
  end
end
