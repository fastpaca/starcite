defmodule Starcite.WritePath.CommandRouter do
  @moduledoc """
  Shared Raft command dispatch for write-path orchestration.

  This module owns routing to local or remote replicas, leader retry handling,
  and the pipeline fallback used by create, append, and archive-ack flows.
  """

  alias Starcite.DataPlane.{
    RaftBootstrap,
    RaftManager,
    RaftPipelineClient,
    ReplicaRouter
  }

  alias Starcite.Observability.Telemetry

  @timeout Application.compile_env(:starcite, :raft_command_timeout_ms, 2_000)

  @type result :: {:ok, term()} | {:error, term()} | {:timeout, term()}

  @doc """
  Route a session-scoped command to the correct local or remote Raft group.
  """
  @spec dispatch_session(String.t(), (atom() -> result()), module(), atom(), [term()]) :: result()
  def dispatch_session(session_id, local_dispatch, remote_mod, remote_fun, remote_args)
      when is_binary(session_id) and session_id != "" and is_function(local_dispatch, 1) and
             is_atom(remote_mod) and is_atom(remote_fun) and is_list(remote_args) do
    dispatch_group(
      group_for_session(session_id),
      local_dispatch,
      remote_mod,
      remote_fun,
      remote_args
    )
  end

  @doc """
  Dispatch a session-scoped command against the local group's server process.
  """
  @spec dispatch_local_session(String.t(), term()) :: result()
  def dispatch_local_session(session_id, command)
      when is_binary(session_id) and session_id != "" do
    with {:ok, server_id, _group} <- locate_and_ensure_started(session_id) do
      dispatch_server(server_id, command)
    end
  end

  @doc """
  Route a pre-grouped command to the correct local or remote Raft group.
  """
  @spec dispatch_group(non_neg_integer(), (atom() -> result()), module(), atom(), [term()]) ::
          result()
  def dispatch_group(group_id, local_dispatch, remote_mod, remote_fun, remote_args)
      when is_integer(group_id) and group_id >= 0 and is_function(local_dispatch, 1) and
             is_atom(remote_mod) and is_atom(remote_fun) and is_list(remote_args) do
    routing_operation = Telemetry.write_path_routing_operation(remote_mod, remote_fun)

    route_started_at =
      if routing_operation in [:append_event, :append_events] do
        System.monotonic_time()
      end

    case local_server_for_group(group_id) do
      {:ok, server_id} ->
        if routing_operation do
          replica_count = length(RaftManager.replicas_for_group(group_id))

          :ok =
            Telemetry.routing_decision(group_id, %{
              target: :local,
              prefer_leader: false,
              leader_hint: :disabled,
              replica_count: replica_count,
              ready_count: 1
            })
        end

        result = local_dispatch.(server_id)

        if routing_operation do
          :ok =
            Telemetry.routing_result(group_id, :local, result, %{attempts: 0, leader_redirects: 0})
        end

        if routing_operation in [:append_event, :append_events] and is_integer(route_started_at) do
          :ok =
            Telemetry.request_result(
              routing_operation,
              :route,
              result,
              elapsed_ms_since(route_started_at)
            )
        end

        result

      :error ->
        result = dispatch_remote(group_id, remote_mod, remote_fun, remote_args, routing_operation)

        if routing_operation in [:append_event, :append_events] and is_integer(route_started_at) do
          :ok =
            Telemetry.request_result(
              routing_operation,
              :route,
              result,
              elapsed_ms_since(route_started_at)
            )
        end

        result
    end
  end

  @spec locate_and_ensure_started(String.t()) ::
          {:ok, atom(), non_neg_integer()} | {:error, term()}
  def locate_and_ensure_started(session_id) when is_binary(session_id) and session_id != "" do
    group_id = group_for_session(session_id)
    server_id = RaftManager.server_id(group_id)

    with :ok <- ensure_group_started(server_id, group_id) do
      {:ok, server_id, group_id}
    end
  end

  @spec local_server_for_group(non_neg_integer()) :: {:ok, atom()} | :error
  def local_server_for_group(group_id) when is_integer(group_id) and group_id >= 0 do
    server_id = RaftManager.server_id(group_id)

    if Process.whereis(server_id) != nil do
      {:ok, server_id}
    else
      :error
    end
  end

  @spec group_for_session(String.t()) :: non_neg_integer()
  def group_for_session(session_id) when is_binary(session_id) and session_id != "" do
    RaftManager.group_for_session(session_id)
  end

  @doc """
  Dispatch a command directly to a specific local Raft server, retrying on the
  reported leader when the initial call times out.
  """
  @spec dispatch_server(atom(), term()) :: result()
  def dispatch_server(server_id, command) when is_atom(server_id) do
    self_node = Node.self()
    command_name = Telemetry.write_path_command(command)
    request_operation = Telemetry.write_path_request_operation(command)

    {local_result, local_duration_ms} =
      timed_result(fn -> dispatch_on_node(server_id, self_node, command) end)

    local_outcome = classify_local_outcome(local_result)

    :ok =
      Telemetry.raft_command_attempt(
        command_name,
        local_outcome,
        Atom.to_string(self_node),
        request_operation,
        local_result,
        local_duration_ms
      )

    {final_result, outcome} =
      case local_result do
        {:timeout, {^server_id, leader_node}}
        when is_atom(leader_node) and not is_nil(leader_node) ->
          if leader_node == self_node do
            {local_result, :local_timeout}
          else
            {retry_result, retry_duration_ms} =
              timed_result(fn -> dispatch_on_node(server_id, leader_node, command) end)

            retry_outcome = classify_leader_retry_outcome(retry_result)

            :ok =
              Telemetry.raft_command_attempt(
                command_name,
                retry_outcome,
                Atom.to_string(leader_node),
                request_operation,
                retry_result,
                retry_duration_ms
              )

            {retry_result, retry_outcome}
          end

        _ ->
          {local_result, local_outcome}
      end

    :ok = RaftBootstrap.record_write_outcome(outcome)
    final_result
  end

  defp dispatch_remote(group_id, remote_mod, remote_fun, remote_args, routing_operation)
       when is_integer(group_id) and group_id >= 0 and is_atom(remote_mod) and is_atom(remote_fun) and
              is_list(remote_args) do
    ReplicaRouter.call_on_replica(
      group_id,
      remote_mod,
      remote_fun,
      remote_args,
      remote_mod,
      remote_fun,
      remote_args,
      prefer_leader: true,
      telemetry_operation: routing_operation
    )
  end

  defp dispatch_on_node(server_id, node, command)
       when is_atom(server_id) and is_atom(node) do
    case RaftPipelineClient.command(server_id, node, command, @timeout) do
      {:ok, _reply} = ok -> ok
      {:error, _reason} = error -> error
      {:timeout, _leader} -> dispatch_on_node_fallback(server_id, node, command)
    end
  end

  defp ensure_group_started(server_id, group_id)
       when is_atom(server_id) and is_integer(group_id) and group_id >= 0 do
    if Process.whereis(server_id) != nil do
      :ok
    else
      ensure_group_started_slow(group_id)
    end
  end

  defp ensure_group_started_slow(group_id) when is_integer(group_id) and group_id >= 0 do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, {:shutdown, {:failed_to_start_child, _child, {:already_started, _pid}}}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp dispatch_on_node_fallback(server_id, node, command)
       when is_atom(server_id) and is_atom(node) do
    case :ra.process_command({server_id, node}, command, @timeout) do
      {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
      {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
      {:timeout, leader} -> {:timeout, leader}
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify_local_outcome({:ok, _reply}), do: :local_ok
  defp classify_local_outcome({:error, _reason}), do: :local_error
  defp classify_local_outcome({:timeout, _leader}), do: :local_timeout

  defp classify_leader_retry_outcome({:ok, _reply}), do: :leader_retry_ok
  defp classify_leader_retry_outcome({:error, _reason}), do: :leader_retry_error
  defp classify_leader_retry_outcome({:timeout, _leader}), do: :leader_retry_timeout

  defp timed_result(fun) when is_function(fun, 0) do
    started_at = System.monotonic_time()
    {fun.(), elapsed_ms_since(started_at)}
  end

  defp elapsed_ms_since(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end
end
