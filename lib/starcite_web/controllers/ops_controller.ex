defmodule StarciteWeb.OpsController do
  @moduledoc """
  Operator API for control-plane write-node status and drain controls.
  """

  use StarciteWeb, :controller

  alias Starcite.ControlPlane.{Ops, WriteNodes}

  action_fallback StarciteWeb.FallbackController

  def status(conn, _params) do
    %{
      node: node,
      local_write_node: local_write_node,
      local_mode: local_mode,
      local_ready: local_ready,
      local_drained: local_drained,
      write_nodes: write_nodes,
      write_replication_factor: write_replication_factor,
      num_groups: num_groups,
      local_groups: local_groups,
      observer: observer
    } = Ops.status()

    json(conn, %{
      node: node_to_string(node),
      local_write_node: local_write_node,
      local_mode: Atom.to_string(local_mode),
      local_ready: local_ready,
      local_drained: local_drained,
      write_nodes: render_nodes(write_nodes),
      write_replication_factor: write_replication_factor,
      num_groups: num_groups,
      local_groups: local_groups,
      observer: render_observer(observer)
    })
  end

  def ready_nodes(conn, _params) do
    json(conn, %{ready_nodes: render_nodes(Ops.ready_nodes())})
  end

  def drain(conn, params) when is_map(params) do
    with {:ok, node} <- parse_node_param(params),
         :ok <- Ops.drain_node(node) do
      json(conn, %{status: "ok", node: node_to_string(node)})
    end
  end

  def undrain(conn, params) when is_map(params) do
    with {:ok, node} <- parse_node_param(params),
         :ok <- Ops.undrain_node(node) do
      json(conn, %{status: "ok", node: node_to_string(node)})
    end
  end

  def group_replicas(conn, %{"group_id" => raw_group_id}) do
    with {:ok, group_id} <- parse_group_id(raw_group_id) do
      json(conn, %{group_id: group_id, replicas: render_nodes(Ops.group_replicas(group_id))})
    end
  end

  def group_replicas(_conn, _params), do: {:error, :invalid_group_id}

  defp parse_node_param(%{"node" => raw_node}), do: parse_known_node(raw_node)
  defp parse_node_param(_params), do: {:ok, Node.self()}

  defp parse_known_node(raw_node) when is_binary(raw_node) do
    node_name = String.trim(raw_node)

    case Enum.find(known_nodes(), fn node -> Atom.to_string(node) == node_name end) do
      nil -> {:error, :invalid_write_node}
      node -> {:ok, node}
    end
  end

  defp parse_known_node(_raw_node), do: {:error, :invalid_write_node}

  defp known_nodes do
    ([Node.self()] ++ Node.list() ++ WriteNodes.nodes())
    |> Enum.filter(&is_atom/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp parse_group_id(raw_group_id) when is_binary(raw_group_id) do
    case Integer.parse(String.trim(raw_group_id)) do
      {group_id, ""} when group_id >= 0 ->
        if group_id < WriteNodes.num_groups() do
          {:ok, group_id}
        else
          {:error, :invalid_group_id}
        end

      _ ->
        {:error, :invalid_group_id}
    end
  end

  defp parse_group_id(_raw_group_id), do: {:error, :invalid_group_id}

  defp render_observer(%{
         status: status,
         visible_nodes: visible_nodes,
         ready_nodes: ready_nodes,
         node_statuses: node_statuses
       }) do
    %{
      status: Atom.to_string(status),
      visible_nodes: render_nodes(visible_nodes),
      ready_nodes: render_nodes(ready_nodes),
      node_statuses: render_node_statuses(node_statuses)
    }
  end

  defp render_node_statuses(node_statuses) when is_map(node_statuses) do
    Enum.into(node_statuses, %{}, fn
      {node, %{status: status, changed_at_ms: changed_at_ms}}
      when is_atom(node) and is_atom(status) and is_integer(changed_at_ms) ->
        {
          node_to_string(node),
          %{status: Atom.to_string(status), changed_at_ms: changed_at_ms}
        }
    end)
  end

  defp render_nodes(nodes) when is_list(nodes) do
    Enum.map(nodes, &node_to_string/1)
  end

  defp node_to_string(node) when is_atom(node), do: Atom.to_string(node)
end
