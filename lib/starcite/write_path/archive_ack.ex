defmodule Starcite.WritePath.ArchiveAck do
  @moduledoc """
  Archive acknowledgement orchestration for the write path.

  This module accepts flat archived session progress entries, deduplicates them,
  shards them by Raft group, and applies best-effort archive acknowledgements
  back into the FSM.
  """

  alias Starcite.WritePath
  alias Starcite.WritePath.CommandRouter

  @type ack_archived_entry :: {String.t(), non_neg_integer()}
  @type ack_archived_applied :: %{
          session_id: String.t(),
          archived_seq: non_neg_integer(),
          trimmed: non_neg_integer()
        }
  @type ack_archived_failed :: %{session_id: String.t(), reason: term()}
  @type ack_archived_result :: %{
          applied: [ack_archived_applied()],
          failed: [ack_archived_failed()]
        }

  @doc """
  Acknowledge archived progress for one or more sessions.
  """
  @spec run([ack_archived_entry()]) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def run(entries) when is_list(entries) and entries != [] do
    with {:ok, grouped_entries} <- group_entries(entries) do
      result =
        Enum.reduce(grouped_entries, %{applied: [], failed: []}, fn {group, group_entries}, acc ->
          command = {:ack_archived, group_entries}

          dispatch_result =
            CommandRouter.dispatch_group(
              group,
              &CommandRouter.dispatch_server(&1, command),
              WritePath,
              :ack_archived_local,
              [group_entries]
            )

          apply_group_result(acc, group_entries, dispatch_result)
        end)

      {:ok, %{applied: Enum.reverse(result.applied), failed: Enum.reverse(result.failed)}}
    end
  end

  def run(_entries), do: {:error, :invalid_archive_ack}

  @doc """
  Acknowledge archived progress for a single session.
  """
  @spec run(String.t(), non_neg_integer()) ::
          {:ok, ack_archived_result()} | {:error, term()} | {:timeout, term()}
  def run(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    run([{id, upto_seq}])
  end

  def run(_id, _upto_seq), do: {:error, :invalid_archive_ack}

  @doc false
  def run_local(entries) when is_list(entries) and entries != [] do
    with [{session_id, _upto_seq} | _rest] <- entries,
         {:ok, server_id, _group} <- CommandRouter.locate_and_ensure_started(session_id) do
      CommandRouter.dispatch_server(server_id, {:ack_archived, entries})
    else
      _ -> {:error, :invalid_archive_ack}
    end
  end

  @doc false
  def run_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    run_local([{id, upto_seq}])
  end

  def run_local(_id, _upto_seq), do: {:error, :invalid_archive_ack}

  defp group_entries(entries) when is_list(entries) do
    case Enum.reduce_while(entries, %{}, fn
           {session_id, upto_seq}, acc
           when is_binary(session_id) and session_id != "" and is_integer(upto_seq) and
                  upto_seq >= 0 ->
             {:cont, Map.update(acc, session_id, upto_seq, &max(&1, upto_seq))}

           _entry, _acc ->
             {:halt, :error}
         end) do
      deduped when is_map(deduped) and map_size(deduped) > 0 ->
        {:ok,
         Enum.reduce(deduped, %{}, fn {session_id, upto_seq}, acc ->
           Map.update(
             acc,
             CommandRouter.group_for_session(session_id),
             [{session_id, upto_seq}],
             &[{session_id, upto_seq} | &1]
           )
         end)}

      _ ->
        {:error, :invalid_archive_ack}
    end
  end

  defp apply_group_result(
         %{applied: applied_acc, failed: failed_acc},
         group_entries,
         {:ok, %{applied: applied, failed: failed}}
       )
       when is_list(group_entries) and is_list(applied) and is_list(failed) do
    %{applied: Enum.reverse(applied, applied_acc), failed: Enum.reverse(failed, failed_acc)}
  end

  defp apply_group_result(
         %{applied: applied_acc, failed: failed_acc},
         group_entries,
         error
       )
       when is_list(group_entries) do
    reason =
      case error do
        {:error, failure} -> failure
        {:timeout, failure} -> {:timeout, failure}
        other -> {:invalid_archive_ack_reply, other}
      end

    failed =
      Enum.map(group_entries, fn {session_id, _upto_seq} ->
        %{session_id: session_id, reason: reason}
      end)

    %{applied: applied_acc, failed: Enum.reverse(failed, failed_acc)}
  end
end
