defmodule Starcite.ReadPath do
  @moduledoc """
  Read path for session and event tail operations.
  """

  alias Starcite.DataPlane.EventStore
  alias Starcite.Session
  alias Starcite.WritePath.RaftFSM
  alias Starcite.WritePath.RaftManager
  alias Starcite.WritePath.ReplicaRouter

  @default_tail_batch_size 1_000

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(id) when is_binary(id) and id != "" do
    group = RaftManager.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :get_session_local,
      [id],
      __MODULE__,
      :get_session_local,
      [id],
      prefer_leader: false
    )
  end

  def get_session(_id), do: {:error, :invalid_session_id}

  @doc false
  def get_session_local(id) when is_binary(id) and id != "" do
    with {:ok, server_id, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.consistent_query({server_id, Node.self()}, fn state ->
             RaftFSM.query_session(state, id)
           end) do
        {:ok, {:ok, session}, _leader} ->
          {:ok, session}

        {:ok, {:error, reason}, _leader} ->
          {:error, reason}

        {:ok, {{_term, _index}, {:ok, session}}, _leader} ->
          {:ok, session}

        {:ok, {{_term, _index}, {:error, reason}}, _leader} ->
          {:error, reason}

        {:timeout, leader} ->
          {:error, {:timeout, leader}}

        other ->
          other
      end
    end
  end

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftManager.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :get_events_from_cursor_local,
      [id, cursor, limit],
      __MODULE__,
      :get_events_from_cursor_local,
      [id, cursor, limit],
      prefer_leader: false
    )
  end

  def get_events_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @doc false
  def get_events_from_cursor_local(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    with {:ok, server_id, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.consistent_query({server_id, Node.self()}, fn state ->
             RaftFSM.query_session(state, id)
           end) do
        {:ok, {:ok, session}, _leader} ->
          read_events_across_tiers(id, cursor, limit, session)

        {:ok, {{_term, _index}, {:ok, session}}, _leader} ->
          read_events_across_tiers(id, cursor, limit, session)

        {:ok, {:error, reason}, _leader} ->
          {:error, reason}

        {:ok, {{_term, _index}, {:error, reason}}, _leader} ->
          {:error, reason}

        {:timeout, leader} ->
          {:error, {:timeout, leader}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp locate(id) do
    group = RaftManager.group_for_session(id)
    server_id = RaftManager.server_id(group)
    {:ok, server_id, group}
  end

  defp ensure_group_started(group_id) do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, {:shutdown, {:failed_to_start_child, _child, {:already_started, _pid}}}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp read_events_across_tiers(id, cursor, limit, %Session{} = session) do
    with {:ok, cold_events} <- read_cold_events(id, cursor, limit, session),
         {:ok, hot_events} <- read_hot_events(id, cursor, limit, session, cold_events),
         {:ok, merged} <- merge_events(cold_events, hot_events, limit) do
      {:ok, merged}
    end
  end

  defp read_cold_events(id, cursor, limit, %Session{archived_seq: archived_seq}) do
    if archived_seq > cursor do
      from_seq = cursor + 1
      to_seq = min(archived_seq, cursor + limit)
      EventStore.read_archived_events(id, from_seq, to_seq)
    else
      {:ok, []}
    end
  end

  defp read_hot_events(id, cursor, limit, %Session{archived_seq: archived_seq}, cold_events) do
    remaining = max(limit - length(cold_events), 0)

    if remaining == 0 do
      {:ok, []}
    else
      hot_cursor = max(cursor, archived_seq)
      {:ok, EventStore.from_cursor(id, hot_cursor, remaining)}
    end
  end

  defp merge_events(cold_events, hot_events, limit) do
    merged =
      (cold_events ++ hot_events)
      |> Enum.uniq_by(& &1.seq)
      |> Enum.sort_by(& &1.seq)
      |> Enum.take(limit)

    ensure_gap_free(merged)
  end

  defp ensure_gap_free([]), do: {:ok, []}

  defp ensure_gap_free([_single] = events), do: {:ok, events}

  defp ensure_gap_free(events) do
    gap? =
      events
      |> Enum.reduce_while(nil, fn event, previous_seq ->
        cond do
          previous_seq == nil ->
            {:cont, event.seq}

          event.seq == previous_seq + 1 ->
            {:cont, event.seq}

          true ->
            {:halt, :gap}
        end
      end)
      |> Kernel.==(:gap)

    if gap? do
      {:error, :event_gap_detected}
    else
      {:ok, events}
    end
  end
end
