defmodule Starcite.ControlPlane.SessionReplicator do
  @moduledoc """
  In-memory cross-node replication for session-owner state.

  This module lives in the control plane and is used by the data plane to
  synchronously replicate session snapshots to standby nodes before acking
  writes to callers.
  """

  alias Starcite.DataPlane.{RaftManager, SessionOwners}
  alias Starcite.Observability.Telemetry
  alias Starcite.Session

  @rpc_timeout_ms Application.compile_env(:starcite, :session_replication_rpc_timeout_ms, 1_000)
  @max_concurrency Application.compile_env(:starcite, :session_replication_max_concurrency, 16)

  @type replication_failure :: {node(), term()}
  @type replication_error ::
          {:replication_quorum_not_met,
           %{
             required_remote_acks: non_neg_integer(),
             successful_remote_acks: non_neg_integer(),
             failures: [replication_failure()]
           }}

  @spec replicate_state(Session.t(), [map()]) :: :ok | {:error, replication_error()}
  def replicate_state(%Session{id: session_id, tenant_id: tenant_id} = session, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    started_ms = System.monotonic_time(:millisecond)
    replicas = RaftManager.replicas_for_group(RaftManager.group_for_session(session_id))
    local_node = Node.self()
    quorum_size = quorum_size(length(replicas))
    local_acks = if local_node in replicas, do: 1, else: 0
    required_remote_acks = max(quorum_size - local_acks, 0)
    standby_nodes = Enum.reject(replicas, &(&1 == local_node))
    standby_count = length(standby_nodes)

    {result, successful_remote_acks, failures} =
      case required_remote_acks do
        0 ->
          {:ok, 0, []}

        _other ->
          {successful_remote_acks, failures} =
            collect_remote_results_parallel(
              standby_nodes,
              session,
              events
            )

          finalize_quorum_result(required_remote_acks, successful_remote_acks, failures)
      end

    duration_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)
    outcome = if result == :ok, do: :ok, else: :quorum_not_met
    failure_reason = primary_failure_reason(failures)

    :ok =
      Telemetry.session_replication(
        session_id,
        tenant_id,
        outcome,
        duration_ms,
        standby_count,
        required_remote_acks,
        successful_remote_acks,
        length(failures),
        failure_reason
      )

    result
  end

  defp collect_remote_results_parallel(
         standby_nodes,
         session,
         events
       )
       when is_list(standby_nodes) and is_struct(session, Session) and is_list(events) do
    standby_nodes
    |> Task.async_stream(
      fn standby_node ->
        {standby_node, replicate_to_standby(standby_node, session, events)}
      end,
      ordered: false,
      timeout: @rpc_timeout_ms,
      on_timeout: :kill_task,
      max_concurrency: max_concurrency(length(standby_nodes))
    )
    |> Enum.reduce({0, []}, fn result, {acks, failures} ->
      case result_to_ack_result(result) do
        {:ack, _node} ->
          {acks + 1, failures}

        {:fail, failure} ->
          {acks, [failure | failures]}
      end
    end)
  end

  defp replicate_to_standby(standby_node, session, events)
       when is_atom(standby_node) and is_struct(session, Session) and is_list(events) do
    case :rpc.call(
           standby_node,
           SessionOwners,
           :replicate_state,
           [session, events],
           @rpc_timeout_ms
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, reason}

      {:timeout, reason} ->
        {:error, {:timeout, reason}}

      {:badrpc, reason} ->
        {:error, {:badrpc, reason}}

      other ->
        {:error, {:unexpected_response, other}}
    end
  end

  defp result_to_ack_result({:ok, {standby_node, :ok}})
       when is_atom(standby_node) do
    {:ack, standby_node}
  end

  defp result_to_ack_result({:ok, {standby_node, {:error, reason}}})
       when is_atom(standby_node) do
    {:fail, {standby_node, {:error, reason}}}
  end

  defp result_to_ack_result({:exit, reason}) do
    {:fail, {:unknown, {:error, {:task_exit, reason}}}}
  end

  defp result_to_ack_result(other) do
    {:fail, {:unknown, {:error, {:task_error, other}}}}
  end

  defp quorum_size(replica_count) when is_integer(replica_count) and replica_count > 0 do
    div(replica_count, 2) + 1
  end

  defp quorum_size(_replica_count), do: 1

  defp finalize_quorum_result(required_remote_acks, successful_remote_acks, failures)
       when is_integer(required_remote_acks) and required_remote_acks >= 0 and
              is_integer(successful_remote_acks) and successful_remote_acks >= 0 and
              is_list(failures) do
    if successful_remote_acks >= required_remote_acks do
      {:ok, successful_remote_acks, failures}
    else
      error =
        {:error,
         {:replication_quorum_not_met,
          %{
            required_remote_acks: required_remote_acks,
            successful_remote_acks: successful_remote_acks,
            failures: Enum.reverse(failures)
          }}}

      {error, successful_remote_acks, failures}
    end
  end

  defp max_concurrency(standby_count)
       when is_integer(standby_count) and standby_count >= 0 do
    configured =
      case @max_concurrency do
        value when is_integer(value) and value > 0 -> value
        _ -> 1
      end

    max(1, min(configured, max(standby_count, 1)))
  end

  defp primary_failure_reason([]), do: :none

  defp primary_failure_reason([{_node, {:error, {:badrpc, _reason}}} | _]), do: :badrpc
  defp primary_failure_reason([{_node, {:error, {:timeout, _reason}}} | _]), do: :timeout

  defp primary_failure_reason([{_node, {:error, {:unexpected_response, _response}}} | _]),
    do: :unexpected_response

  defp primary_failure_reason([{_node, {:error, {:task_exit, _reason}}} | _]), do: :task_exit
  defp primary_failure_reason([{_node, {:error, {:task_error, _reason}}} | _]), do: :task_error
  defp primary_failure_reason([{_node, {:error, reason}} | _]) when is_atom(reason), do: reason
  defp primary_failure_reason([{_node, {:error, _reason}} | _]), do: :error
  defp primary_failure_reason([{_node, {:badrpc, _reason}} | _]), do: :badrpc
  defp primary_failure_reason([{_node, reason} | _]) when is_atom(reason), do: reason
  defp primary_failure_reason([_ | _]), do: :error
end
