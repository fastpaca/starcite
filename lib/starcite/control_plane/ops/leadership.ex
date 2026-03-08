defmodule Starcite.ControlPlane.Ops.Leadership do
  @moduledoc false

  alias Starcite.ControlPlane.Observer
  alias Starcite.ControlPlane.Ops.Topology
  alias Starcite.DataPlane.RaftManager
  alias Starcite.Observability.Telemetry

  @default_wait_interval_ms 200
  @target_probe_timeout_ms 1_000
  @group_roles [:leader, :follower, :candidate, :other, :down]

  @type group_role :: :leader | :follower | :candidate | :other | :down

  @spec group_roles() :: [group_role()]
  def group_roles, do: @group_roles

  @spec local_raft_group_states() :: [
          %{
            group_id: non_neg_integer(),
            leader_node: node() | nil,
            replicas: [node()],
            role: group_role()
          }
        ]
  def local_raft_group_states do
    Enum.map(Topology.local_write_groups(), &local_group_state/1)
  end

  @spec raft_role_counts() :: %{
          leader: non_neg_integer(),
          follower: non_neg_integer(),
          candidate: non_neg_integer(),
          other: non_neg_integer(),
          down: non_neg_integer()
        }
  def raft_role_counts do
    Enum.reduce(local_raft_group_states(), empty_role_counts(), fn %{role: role}, acc ->
      Map.update!(acc, role, &(&1 + 1))
    end)
  end

  @spec local_leader_groups() :: [non_neg_integer()]
  def local_leader_groups do
    local_raft_group_states()
    |> Enum.reduce([], fn
      %{group_id: group_id, role: :leader}, acc -> [group_id | acc]
      _state, acc -> acc
    end)
    |> Enum.reverse()
  end

  @spec group_leader(non_neg_integer()) :: node() | nil
  def group_leader(group_id) when is_integer(group_id) and group_id >= 0 do
    case Topology.parse_group_id(group_id) do
      {:ok, valid_group_id} ->
        lookup_group_leader(valid_group_id)

      {:error, :invalid_group_id} ->
        raise ArgumentError, "invalid group_id: #{inspect(group_id)}"
    end
  end

  @doc false
  @spec local_group_state(non_neg_integer()) :: %{
          group_id: non_neg_integer(),
          leader_node: node() | nil,
          replicas: [node()],
          role: group_role()
        }
  def local_group_state(group_id) when is_integer(group_id) and group_id >= 0 do
    %{
      group_id: group_id,
      role: local_group_role(group_id),
      leader_node: lookup_group_leader(group_id),
      replicas: RaftManager.replicas_for_group(group_id)
    }
  end

  @doc false
  @spec local_group_probe(non_neg_integer()) :: %{role: group_role(), running?: boolean()}
  def local_group_probe(group_id) when is_integer(group_id) and group_id >= 0 do
    %{
      role: local_group_role(group_id),
      running?: group_running?(group_id)
    }
  end

  @doc false
  @spec emit_local_raft_role_telemetry() :: :ok
  def emit_local_raft_role_telemetry do
    node_name = Atom.to_string(Node.self())
    states = local_raft_group_states()
    counts = count_states(states)

    Enum.each(counts, fn {role, groups} ->
      :ok = Telemetry.raft_group_role_count(node_name, role, groups)
    end)

    Enum.each(states, fn %{group_id: group_id, role: current_role} ->
      Enum.each(@group_roles, fn role ->
        present = if role == current_role, do: 1, else: 0
        :ok = Telemetry.raft_group_role_presence(node_name, group_id, role, present)
      end)
    end)

    :ok
  end

  @spec transfer_group_leadership(non_neg_integer(), node()) ::
          :ok | :already_leader | {:error, term()} | {:timeout, term()}
  def transfer_group_leadership(group_id, target_node)
      when is_integer(group_id) and group_id >= 0 and is_atom(target_node) do
    source_node = lookup_group_leader_safe(group_id)

    result =
      with {:ok, valid_group_id} <- Topology.parse_group_id(group_id),
           :ok <- validate_target_replica(valid_group_id, target_node),
           {:ok, current_leader} <- current_leader(valid_group_id),
           :ok <- validate_target_leader_change(current_leader, target_node),
           :ok <- validate_target_ready(target_node),
           :ok <- validate_target_probe(valid_group_id, target_node),
           transfer_result <- do_transfer(valid_group_id, current_leader, target_node) do
        transfer_result
      end

    maybe_emit_transfer_telemetry(group_id, source_node, target_node, result)
    result
  end

  @spec wait_group_leader(non_neg_integer(), node(), pos_integer()) :: :ok | {:error, term()}
  def wait_group_leader(group_id, target_node, timeout_ms)
      when is_integer(group_id) and group_id >= 0 and is_atom(target_node) and
             is_integer(timeout_ms) and timeout_ms > 0 do
    with {:ok, valid_group_id} <- Topology.parse_group_id(group_id),
         :ok <- validate_target_replica(valid_group_id, target_node) do
      deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
      do_wait_group_leader(valid_group_id, target_node, deadline_ms)
    end
  end

  defp do_wait_group_leader(group_id, target_node, deadline_ms)
       when is_integer(group_id) and is_atom(target_node) and is_integer(deadline_ms) do
    if lookup_group_leader(group_id) == target_node do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline_ms do
        {:error, :timeout}
      else
        Process.sleep(@default_wait_interval_ms)
        do_wait_group_leader(group_id, target_node, deadline_ms)
      end
    end
  end

  defp validate_target_replica(group_id, target_node)
       when is_integer(group_id) and is_atom(target_node) do
    if target_node in RaftManager.replicas_for_group(group_id) do
      :ok
    else
      {:error, :target_not_replica}
    end
  end

  defp current_leader(group_id) when is_integer(group_id) and group_id >= 0 do
    case lookup_group_leader(group_id) do
      leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
        {:ok, leader_node}

      nil ->
        {:error, :leader_unknown}
    end
  end

  defp validate_target_leader_change(current_leader, target_node)
       when is_atom(current_leader) and is_atom(target_node) do
    if current_leader == target_node do
      :already_leader
    else
      :ok
    end
  end

  defp validate_target_ready(target_node) when is_atom(target_node) do
    if target_node in Observer.ready_nodes() do
      :ok
    else
      {:error, :target_not_ready}
    end
  end

  defp validate_target_probe(group_id, target_node)
       when is_integer(group_id) and is_atom(target_node) do
    case target_group_probe(target_node, group_id) do
      {:ok, %{role: :follower, running?: true}} ->
        :ok

      {:ok, %{role: :leader}} ->
        :already_leader

      {:ok, %{role: role, running?: true}} ->
        {:error, {:target_not_follower, role}}

      {:ok, %{role: role, running?: false}} ->
        {:error, {:target_group_not_running, role}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp target_group_probe(target_node, group_id)
       when is_atom(target_node) and is_integer(group_id) and group_id >= 0 do
    if target_node == Node.self() do
      {:ok, local_group_probe(group_id)}
    else
      case :rpc.call(
             target_node,
             __MODULE__,
             :local_group_probe,
             [group_id],
             @target_probe_timeout_ms
           ) do
        %{role: role, running?: running?}
        when role in @group_roles and is_boolean(running?) ->
          {:ok, %{role: role, running?: running?}}

        {:badrpc, reason} ->
          {:error, {:target_probe_failed, reason}}

        other ->
          {:error, {:target_probe_invalid_response, other}}
      end
    end
  end

  defp do_transfer(group_id, current_leader, target_node)
       when is_integer(group_id) and is_atom(current_leader) and is_atom(target_node) do
    server_id = RaftManager.server_id(group_id)

    try do
      :ra.transfer_leadership({server_id, current_leader}, {server_id, target_node})
    catch
      :exit, reason ->
        {:error, {:transfer_failed, reason}}
    end
  end

  defp maybe_emit_transfer_telemetry(group_id, source_node, target_node, result)
       when is_integer(group_id) and group_id >= 0 and is_atom(target_node) do
    source_node_name = normalize_node_name(source_node)
    target_node_name = normalize_node_name(target_node)

    {outcome, reason} = normalize_transfer_result(result)

    :ok =
      Telemetry.raft_leadership_transfer(
        group_id,
        source_node_name,
        target_node_name,
        outcome,
        reason
      )
  end

  defp maybe_emit_transfer_telemetry(_group_id, _source_node, _target_node, _result), do: :ok

  defp normalize_transfer_result(:ok), do: {:ok, :none}
  defp normalize_transfer_result(:already_leader), do: {:already_leader, :none}
  defp normalize_transfer_result({:timeout, _leader}), do: {:timeout, :timeout}
  defp normalize_transfer_result({:error, :leader_unknown}), do: {:error, :leader_unknown}
  defp normalize_transfer_result({:error, :target_not_ready}), do: {:error, :target_not_ready}
  defp normalize_transfer_result({:error, :target_not_replica}), do: {:error, :target_not_replica}

  defp normalize_transfer_result({:error, {:target_not_follower, role}})
       when role in @group_roles do
    {:error, role}
  end

  defp normalize_transfer_result({:error, {:target_group_not_running, role}})
       when role in @group_roles do
    case role do
      :leader -> {:error, :leader_not_running}
      :follower -> {:error, :follower_not_running}
      :candidate -> {:error, :candidate_not_running}
      :other -> {:error, :other_not_running}
      :down -> {:error, :down_not_running}
    end
  end

  defp normalize_transfer_result({:error, {:target_probe_failed, _reason}}),
    do: {:error, :target_probe_failed}

  defp normalize_transfer_result({:error, {:target_probe_invalid_response, _response}}),
    do: {:error, :target_probe_invalid_response}

  defp normalize_transfer_result({:error, {:transfer_failed, _reason}}),
    do: {:error, :transfer_failed}

  defp normalize_transfer_result({:error, :invalid_group_id}), do: {:error, :invalid_group_id}
  defp normalize_transfer_result({:error, other}) when is_atom(other), do: {:error, other}
  defp normalize_transfer_result({:error, _other}), do: {:error, :error}

  defp lookup_group_leader(group_id) when is_integer(group_id) and group_id >= 0 do
    server_id = RaftManager.server_id(group_id)
    cluster_name = RaftManager.cluster_name(group_id)

    case :ra_leaderboard.lookup_leader(cluster_name) do
      {^server_id, leader_node} when is_atom(leader_node) and not is_nil(leader_node) ->
        leader_node

      _other ->
        nil
    end
  end

  defp lookup_group_leader_safe(group_id) when is_integer(group_id) and group_id >= 0 do
    case Topology.parse_group_id(group_id) do
      {:ok, valid_group_id} -> lookup_group_leader(valid_group_id)
      {:error, :invalid_group_id} -> nil
    end
  end

  defp local_group_role(group_id) when is_integer(group_id) and group_id >= 0 do
    if group_running?(group_id) do
      server_ref = {RaftManager.server_id(group_id), Node.self()}

      try do
        case :ra.key_metrics(server_ref) do
          %{state: :leader} -> :leader
          %{state: :follower} -> :follower
          %{state: :candidate} -> :candidate
          _metrics -> :other
        end
      catch
        :exit, _reason ->
          :down
      end
    else
      :down
    end
  end

  defp group_running?(group_id) when is_integer(group_id) and group_id >= 0 do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp count_states(states) when is_list(states) do
    Enum.reduce(states, empty_role_counts(), fn %{role: role}, acc ->
      Map.update!(acc, role, &(&1 + 1))
    end)
  end

  defp empty_role_counts do
    %{leader: 0, follower: 0, candidate: 0, other: 0, down: 0}
  end

  defp normalize_node_name(node) when is_atom(node) and not is_nil(node), do: Atom.to_string(node)
  defp normalize_node_name(nil), do: "unknown"
end
