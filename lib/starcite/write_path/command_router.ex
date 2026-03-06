defmodule Starcite.WritePath.CommandRouter do
  @moduledoc """
  Shared Raft command dispatch for write-path orchestration.

  This module owns routing to local or remote replicas, leader retry handling,
  and the pipeline fallback used by create, append, and archive-ack flows.
  """

  alias Starcite.DataPlane.{RaftAccess, RaftBootstrap, RaftPipelineClient, ReplicaRouter}

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
      RaftAccess.group_for_session(session_id),
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
    with {:ok, server_id, _group} <- RaftAccess.locate_and_ensure_started(session_id) do
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
    case RaftAccess.local_server_for_group(group_id) do
      {:ok, server_id} -> local_dispatch.(server_id)
      :error -> dispatch_remote(group_id, remote_mod, remote_fun, remote_args)
    end
  end

  @doc """
  Dispatch a command directly to a specific local Raft server, retrying on the
  reported leader when the initial call times out.
  """
  @spec dispatch_server(atom(), term()) :: result()
  def dispatch_server(server_id, command) when is_atom(server_id) do
    self_node = Node.self()
    local_result = dispatch_on_node(server_id, self_node, command)

    {final_result, outcome} =
      case local_result do
        {:timeout, {^server_id, leader_node}}
        when is_atom(leader_node) and not is_nil(leader_node) ->
          if leader_node == self_node do
            {local_result, :local_timeout}
          else
            retry_result = dispatch_on_node(server_id, leader_node, command)
            {retry_result, classify_leader_retry_outcome(retry_result)}
          end

        _ ->
          {local_result, classify_local_outcome(local_result)}
      end

    :ok = RaftBootstrap.record_write_outcome(outcome)
    final_result
  end

  defp dispatch_remote(group_id, remote_mod, remote_fun, remote_args)
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
      prefer_leader: false
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
end
