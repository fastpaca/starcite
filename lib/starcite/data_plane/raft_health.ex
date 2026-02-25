defmodule Starcite.DataPlane.RaftHealth do
  @moduledoc """
  Intrinsic Raft readiness checks used by health and routing gates.

  This module owns the cached consensus readiness state and the on-demand
  refresh path used by `/health/ready`.
  """

  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.DataPlane.RaftManager

  @consensus_probe_ttl_ms 3_000

  @spec maybe_refresh(map(), boolean()) :: map()
  def maybe_refresh(state, refresh?) when is_map(state) and is_boolean(refresh?) do
    cond do
      not state.startup_complete? ->
        clear(state)

      refresh? and not consensus_probe_fresh?(state.consensus_last_probe_at_ms) ->
        refresh_consensus_state(state)

      true ->
        state
    end
  end

  @spec clear(map()) :: map()
  def clear(state) when is_map(state) do
    %{
      state
      | consensus_ready?: false,
        consensus_last_probe_at_ms: nil,
        consensus_probe_detail: %{
          checked_groups: 0,
          failing_group_id: nil,
          probe_result: "startup_sync"
        }
    }
  end

  @spec readiness_status(map()) :: map()
  def readiness_status(state) when is_map(state) do
    ready? = state.startup_complete? and consensus_gate_satisfied?(state)
    probe_fresh? = consensus_probe_fresh?(state.consensus_last_probe_at_ms)

    probe_detail =
      Map.merge(readiness_detail_base(state, probe_fresh?), state.consensus_probe_detail)

    {reason, detail} =
      cond do
        ready? ->
          {:ok, probe_detail}

        not state.startup_complete? ->
          {:startup_sync, probe_detail}

        true ->
          {:raft_sync, probe_detail}
      end

    %{
      ready?: ready?,
      reason: reason,
      detail: detail
    }
  end

  @spec readiness_status_fallback() :: map()
  def readiness_status_fallback do
    %{
      ready?: false,
      reason: :bootstrap_down,
      detail: %{
        startup_mode: nil,
        consensus_ready?: false,
        consensus_probe_fresh?: false,
        consensus_probe_age_ms: nil,
        checked_groups: 0,
        failing_group_id: nil,
        probe_result: "bootstrap_down"
      }
    }
  end

  @spec record_write_outcome(
          map(),
          :local_ok
          | :local_error
          | :local_timeout
          | :leader_retry_ok
          | :leader_retry_error
          | :leader_retry_timeout
        ) :: map()
  def record_write_outcome(state, outcome)
      when is_map(state) and
             outcome in [:local_ok, :local_error, :leader_retry_ok, :leader_retry_error] do
    state
  end

  def record_write_outcome(state, outcome)
      when is_map(state) and outcome in [:local_timeout, :leader_retry_timeout] do
    now_ms = System.monotonic_time(:millisecond)

    state
    |> Map.put(:consensus_ready?, false)
    |> Map.put(:consensus_last_probe_at_ms, now_ms)
    |> Map.put(:consensus_probe_detail, %{
      checked_groups: 0,
      failing_group_id: nil,
      probe_result: "write_timeout",
      outcome: Atom.to_string(outcome)
    })
  end

  @spec note_peer_down(map(), node()) :: map()
  def note_peer_down(state, down_node) when is_map(state) and is_atom(down_node) do
    if state.startup_complete? and WriteNodes.write_node?(Node.self()) and
         down_node in WriteNodes.nodes() and not local_quorum_available?() do
      now_ms = System.monotonic_time(:millisecond)

      state
      |> Map.put(:consensus_ready?, false)
      |> Map.put(:consensus_last_probe_at_ms, now_ms)
      |> Map.put(:consensus_probe_detail, %{
        checked_groups: 0,
        failing_group_id: nil,
        probe_result: "quorum_unreachable",
        node: Atom.to_string(down_node)
      })
    else
      state
    end
  end

  @spec note_peer_up(map(), node()) :: map()
  def note_peer_up(state, up_node) when is_map(state) and is_atom(up_node) do
    if state.startup_complete? and WriteNodes.write_node?(Node.self()) and
         up_node in WriteNodes.nodes() do
      Map.put(state, :consensus_last_probe_at_ms, nil)
    else
      state
    end
  end

  defp refresh_consensus_state(state) when is_map(state) do
    now_ms = System.monotonic_time(:millisecond)

    case consensus_probe() do
      {:ok, detail} ->
        %{
          state
          | consensus_ready?: true,
            consensus_last_probe_at_ms: now_ms,
            consensus_probe_detail: detail
        }

      {:error, detail} ->
        %{
          state
          | consensus_ready?: false,
            consensus_last_probe_at_ms: now_ms,
            consensus_probe_detail: detail
        }
    end
  end

  defp consensus_probe do
    connected_nodes = connected_nodes_set()

    case compute_my_groups() do
      [] ->
        {:ok, %{checked_groups: 0, failing_group_id: nil, probe_result: "ok"}}

      my_groups ->
        Enum.reduce_while(my_groups, {:ok, 0}, fn group_id, {:ok, checked_groups} ->
          case group_consensus_ready?(group_id, connected_nodes) do
            :ok ->
              {:cont, {:ok, checked_groups + 1}}

            {:error, detail} ->
              {:halt, {:error, Map.put(detail, :checked_groups, checked_groups + 1)}}
          end
        end)
        |> case do
          {:ok, checked_groups} ->
            {:ok, %{checked_groups: checked_groups, failing_group_id: nil, probe_result: "ok"}}

          {:error, detail} ->
            {:error, detail}
        end
    end
  end

  defp consensus_gate_satisfied?(%{
         consensus_ready?: consensus_ready,
         consensus_last_probe_at_ms: consensus_last_probe_at_ms
       }) do
    consensus_ready and consensus_probe_fresh?(consensus_last_probe_at_ms)
  end

  defp readiness_detail_base(state, probe_fresh?)
       when is_map(state) and is_boolean(probe_fresh?) do
    %{
      startup_mode: state.startup_mode,
      consensus_ready?: state.consensus_ready?,
      consensus_probe_fresh?: probe_fresh?,
      consensus_probe_age_ms: consensus_probe_age_ms(state.consensus_last_probe_at_ms)
    }
  end

  defp consensus_probe_fresh?(consensus_last_probe_at_ms)
       when is_integer(consensus_last_probe_at_ms) do
    System.monotonic_time(:millisecond) - consensus_last_probe_at_ms <= @consensus_probe_ttl_ms
  end

  defp consensus_probe_fresh?(_consensus_last_probe_at_ms), do: false

  defp consensus_probe_age_ms(consensus_last_probe_at_ms)
       when is_integer(consensus_last_probe_at_ms) do
    max(System.monotonic_time(:millisecond) - consensus_last_probe_at_ms, 0)
  end

  defp consensus_probe_age_ms(_consensus_last_probe_at_ms), do: nil

  defp group_consensus_ready?(group_id, connected_nodes)
       when is_integer(group_id) and group_id >= 0 and is_struct(connected_nodes, MapSet) do
    server_id = RaftManager.server_id(group_id)

    if group_running?(group_id) do
      with :ok <- validate_group_quorum(group_id, connected_nodes),
           metrics <- :ra.key_metrics({server_id, Node.self()}),
           {:ok, metrics} <- validate_group_metrics(group_id, metrics),
           :ok <- validate_group_leader(group_id, server_id),
           :ok <- validate_group_membership(group_id, metrics) do
        :ok
      else
        {:error, detail} ->
          {:error, detail}
      end
    else
      {:error, %{failing_group_id: group_id, probe_result: "local_group_down"}}
    end
  end

  defp validate_group_quorum(group_id, connected_nodes)
       when is_integer(group_id) and group_id >= 0 and is_struct(connected_nodes, MapSet) do
    replicas = RaftManager.replicas_for_group(group_id)
    required_quorum = quorum_size(length(replicas))

    reachable_replicas =
      Enum.count(replicas, fn replica -> MapSet.member?(connected_nodes, replica) end)

    if reachable_replicas >= required_quorum do
      :ok
    else
      {:error,
       %{
         failing_group_id: group_id,
         probe_result: "quorum_unreachable",
         reachable_replicas: reachable_replicas,
         required_quorum: required_quorum
       }}
    end
  end

  defp validate_group_metrics(group_id, metrics)
       when is_integer(group_id) and group_id >= 0 and is_map(metrics) do
    state = Map.get(metrics, :state)

    case state do
      :leader ->
        {:ok, metrics}

      :follower ->
        {:ok, metrics}

      _ ->
        {:error,
         %{
           failing_group_id: group_id,
           probe_result: "raft_state",
           state: inspect(state)
         }}
    end
  end

  defp validate_group_metrics(group_id, _metrics) when is_integer(group_id) and group_id >= 0 do
    {:error, %{failing_group_id: group_id, probe_result: "metrics_unavailable"}}
  end

  defp validate_group_leader(group_id, server_id)
       when is_integer(group_id) and group_id >= 0 and is_atom(server_id) do
    cluster_name = RaftManager.cluster_name(group_id)

    case :ra_leaderboard.lookup_leader(cluster_name) do
      {^server_id, leader_node} when is_atom(leader_node) and not is_nil(leader_node) ->
        :ok

      :undefined ->
        {:error, %{failing_group_id: group_id, probe_result: "leader_unknown"}}

      leader ->
        {:error,
         %{
           failing_group_id: group_id,
           probe_result: "leader_mismatch",
           leader: normalize_leader_hint(leader)
         }}
    end
  end

  defp validate_group_membership(group_id, metrics)
       when is_integer(group_id) and group_id >= 0 and is_map(metrics) do
    case Map.get(metrics, :membership) do
      :voter ->
        :ok

      membership ->
        {:error,
         %{
           failing_group_id: group_id,
           probe_result: "membership",
           membership: inspect(membership)
         }}
    end
  end

  defp local_quorum_available? do
    connected_nodes = connected_nodes_set()

    compute_my_groups()
    |> Enum.all?(fn group_id ->
      replicas = RaftManager.replicas_for_group(group_id)
      required_quorum = quorum_size(length(replicas))
      reachable_replicas = Enum.count(replicas, &MapSet.member?(connected_nodes, &1))
      reachable_replicas >= required_quorum
    end)
  end

  defp connected_nodes_set do
    [Node.self() | Node.list(:connected)]
    |> MapSet.new()
  end

  defp compute_my_groups do
    for group_id <- 0..(WriteNodes.num_groups() - 1),
        RaftManager.should_participate?(group_id),
        do: group_id
  end

  defp group_running?(group_id) do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp quorum_size(replica_count) when is_integer(replica_count) and replica_count >= 0 do
    div(replica_count, 2) + 1
  end

  defp normalize_leader_hint({server_id, leader_node})
       when is_atom(server_id) and not is_nil(server_id) and is_atom(leader_node) and
              not is_nil(leader_node) do
    %{
      server_id: Atom.to_string(server_id),
      node: Atom.to_string(leader_node)
    }
  end

  defp normalize_leader_hint(other), do: inspect(other)
end
