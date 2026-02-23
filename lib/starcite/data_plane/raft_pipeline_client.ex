defmodule Starcite.DataPlane.RaftPipelineClient do
  @moduledoc """
  Thin RA command client using `:ra.pipeline_command/4` directly from the
  caller process.

  This avoids the extra per-command hop through intermediate worker processes.
  """

  @type command_result ::
          {:ok, term()}
          | {:error, term()}
          | {:timeout, {atom(), node()}}

  @spec command(atom(), node(), term(), pos_integer()) :: command_result
  def command(server_id, node, command, timeout_ms)
      when is_atom(server_id) and is_atom(node) and is_integer(timeout_ms) and timeout_ms > 0 do
    correlation = make_ref()

    :ok = :ra.pipeline_command({server_id, node}, command, correlation, command_priority(command))

    await_result(correlation, timeout_ms, server_id, node)
  end

  defp await_result(correlation, timeout_ms, server_id, node)
       when is_reference(correlation) and is_integer(timeout_ms) and timeout_ms > 0 and
              is_atom(server_id) and is_atom(node) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_result(correlation, deadline, server_id, node)
  end

  defp do_await_result(correlation, deadline, server_id, node)
       when is_reference(correlation) and is_integer(deadline) and is_atom(server_id) and
              is_atom(node) do
    remaining_ms = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:ra_event, _from, {:applied, applied}} when is_list(applied) ->
        case find_applied_result(applied, correlation) do
          {:ok, reply} ->
            reply

          :not_found ->
            do_await_result(correlation, deadline, server_id, node)
        end

      {:ra_event, _from, {:rejected, {:not_leader, leader, ^correlation}}} ->
        timeout_result(server_id, node, leader)

      {:ra_event, _from, {:rejected, _reason}} ->
        do_await_result(correlation, deadline, server_id, node)
    after
      remaining_ms ->
        {:timeout, {server_id, node}}
    end
  end

  defp find_applied_result(applied, correlation)
       when is_list(applied) and is_reference(correlation) do
    Enum.reduce_while(applied, :not_found, fn
      {^correlation, {:reply, {:ok, reply}}}, _acc ->
        {:halt, {:ok, {:ok, reply}}}

      {^correlation, {:reply, {:error, reason}}}, _acc ->
        {:halt, {:ok, {:error, reason}}}

      {_other_correlation, _other_reply}, _acc ->
        {:cont, :not_found}
    end)
  end

  defp timeout_result(_server_id, _node, {leader_id, leader_node})
       when is_atom(leader_id) and is_atom(leader_node) do
    {:timeout, {leader_id, leader_node}}
  end

  defp timeout_result(server_id, node, _leader)
       when is_atom(server_id) and is_atom(node) do
    {:timeout, {server_id, node}}
  end

  # Let the Ra leader coalesce hot append traffic into fewer log appends.
  defp command_priority({:append_event, _id, _event, _expected_seq}), do: :low
  defp command_priority({:append_events, _id, _events, _opts}), do: :low
  defp command_priority({:ack_archived, _id, _upto_seq}), do: :low
  defp command_priority(_command), do: :normal
end
