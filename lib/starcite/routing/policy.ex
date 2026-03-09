defmodule Starcite.Routing.Policy do
  @moduledoc false

  @spec choose_claim_nodes(map(), [node()], pos_integer()) ::
          {:ok, [node()]} | {:error, :no_ready_cluster_nodes}
  def choose_claim_nodes(assignments, ready_nodes, desired)
      when is_map(assignments) and is_list(ready_nodes) and is_integer(desired) and desired > 0 do
    counts = session_counts(assignments)

    case least_loaded_nodes(ready_nodes, counts, desired) do
      [] -> {:error, :no_ready_cluster_nodes}
      nodes when length(nodes) < desired -> {:error, :no_ready_cluster_nodes}
      nodes -> {:ok, nodes}
    end
  end

  @spec session_counts(map()) :: %{optional(node()) => non_neg_integer()}
  def session_counts(assignments) when is_map(assignments) do
    Enum.reduce(assignments, %{}, fn
      {_session_id, %{owner: owner, status: :moving, target_owner: target_owner}}, acc
      when is_atom(owner) and is_atom(target_owner) ->
        acc
        |> Map.update(owner, 1, &(&1 + 1))
        |> Map.update(target_owner, 1, &(&1 + 1))

      {_session_id, %{owner: owner}}, acc when is_atom(owner) ->
        Map.update(acc, owner, 1, &(&1 + 1))

      _other, acc ->
        acc
    end)
  end

  @spec next_owner(map(), node(), [node()], map()) :: node() | nil
  def next_owner(%{replicas: replicas}, owner_node, ready_nodes, counts)
      when is_list(replicas) and is_atom(owner_node) and is_list(ready_nodes) and is_map(counts) do
    case least_loaded_node(
           Enum.filter(replicas, &(&1 != owner_node and &1 in ready_nodes)),
           counts
         ) do
      nil ->
        ready_nodes
        |> Enum.reject(&(&1 == owner_node))
        |> least_loaded_node(counts)

      target_node ->
        target_node
    end
  end

  @spec failover_target(map(), node(), [node()], map()) :: node() | nil
  def failover_target(
        %{status: :moving, target_owner: target_owner},
        owner_node,
        ready_nodes,
        counts
      )
      when is_atom(target_owner) and is_atom(owner_node) and is_list(ready_nodes) and
             is_map(counts) do
    if target_owner != owner_node and target_owner in ready_nodes do
      target_owner
    else
      ready_nodes
      |> Enum.reject(&(&1 == owner_node))
      |> least_loaded_node(counts)
    end
  end

  def failover_target(assignment, owner_node, ready_nodes, counts)
      when is_map(assignment) and is_atom(owner_node) and is_list(ready_nodes) and
             is_map(counts) do
    next_owner(assignment, owner_node, ready_nodes, counts)
  end

  @spec rebalance_replicas([node()], node(), [node()], pos_integer()) :: [node()]
  def rebalance_replicas(current_replicas, target_owner, ready_nodes, desired)
      when is_list(current_replicas) and is_atom(target_owner) and is_list(ready_nodes) and
             is_integer(desired) and desired > 0 do
    replicas =
      [target_owner | Enum.filter(current_replicas, &(&1 != target_owner and &1 in ready_nodes))]
      |> Enum.uniq()

    if length(replicas) >= desired do
      Enum.take(replicas, desired)
    else
      additional = Enum.reject(ready_nodes, &(&1 in replicas))

      (replicas ++ additional)
      |> Enum.uniq()
      |> Enum.take(desired)
    end
  end

  defp least_loaded_nodes(nodes, counts, desired)
       when is_list(nodes) and is_map(counts) and is_integer(desired) and desired > 0 do
    nodes
    |> Enum.sort_by(fn node -> {Map.get(counts, node, 0), Atom.to_string(node)} end)
    |> Enum.take(desired)
  end

  defp least_loaded_node(nodes, counts) when is_list(nodes) and is_map(counts) do
    nodes
    |> least_loaded_nodes(counts, 1)
    |> List.first()
  end
end
