defmodule Starcite.Operations.Maintenance do
  @moduledoc false

  alias Starcite.Routing.{Store, Topology, Watcher}

  @spec drain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def drain_node(node) when is_atom(node) do
    drain_node(node, :maintenance)
  end

  @spec drain_node(node(), atom()) :: :ok | {:error, :invalid_cluster_node | term()}
  def drain_node(node, source) when is_atom(node) and is_atom(source) do
    with :ok <- ensure_cluster_node(node),
         :ok <- Store.mark_node_draining(node, source) do
      _ = advance_local_drain(node)
      :ok
    end
  end

  @spec undrain_node(node()) :: :ok | {:error, :invalid_cluster_node | term()}
  def undrain_node(node) when is_atom(node) do
    undrain_node(node, :maintenance)
  end

  @spec undrain_node(node(), atom()) ::
          :ok | {:error, :invalid_cluster_node | :node_still_draining | term()}
  def undrain_node(node, source) when is_atom(node) and is_atom(source) do
    with :ok <- ensure_cluster_node(node),
         :ok <- ensure_drained(node),
         :ok <- Store.mark_node_ready(node, source) do
      :ok
    end
  end

  defp ensure_drained(node) when is_atom(node) do
    case Store.drain_status(node) do
      {:ok, %{active_owned_sessions: 0, moving_sessions: 0}} -> :ok
      {:ok, _status} -> {:error, :node_still_draining}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_cluster_node(node) when is_atom(node) do
    if(node in Topology.nodes(), do: :ok, else: {:error, :invalid_cluster_node})
  end

  defp advance_local_drain(node) when is_atom(node) do
    if node == Node.self() do
      Watcher.run_once()
    else
      :ok
    end
  end
end
