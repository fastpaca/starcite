defmodule Starcite.ControlPlane.Ops.Maintenance do
  @moduledoc false

  alias Starcite.ControlPlane.{Observer, WriteNodes}

  @spec drain_node(node()) :: :ok | {:error, :invalid_write_node}
  def drain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_write_node(node) do
      Observer.mark_node_draining(node)
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_write_node}
  def undrain_node(node \\ Node.self()) when is_atom(node) do
    with :ok <- ensure_write_node(node) do
      Observer.mark_node_ready(node)
    end
  end

  defp ensure_write_node(node) when is_atom(node) do
    if WriteNodes.write_node?(node) do
      :ok
    else
      {:error, :invalid_write_node}
    end
  end
end
