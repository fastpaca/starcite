defmodule Starcite.ReadPath do
  @moduledoc """
  Read path for session and event tail operations.
  """

  alias Starcite.DataPlane.{EventStore, RaftAccess, ReplicaRouter, SessionStore}
  alias Starcite.Session

  @default_tail_batch_size 1_000

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(id), do: SessionStore.get_session(id)

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftAccess.group_for_session(id)

    ReplicaRouter.call_on_replica(
      group,
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      prefer_leader: false
    )
  end

  def get_events_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @doc false
  def rpc_get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    do_get_events_from_cursor(id, cursor, limit)
  end

  defp do_get_events_from_cursor(id, cursor, limit) do
    with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
         {:ok, cold_events} <- maybe_read_cold_events(id, cursor, limit, hot_events),
         {:ok, merged} <- merge_events(cold_events, hot_events, limit) do
      {:ok, merged}
    else
      {:error, :archive_read_unavailable} ->
        with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
             {:ok, merged} <- merge_events([], hot_events, limit) do
          {:ok, merged}
        end
    end
  end

  defp maybe_read_cold_events(id, cursor, limit, []),
    do: read_cold_events(id, cursor, limit)

  defp maybe_read_cold_events(_id, cursor, _limit, [%{seq: seq} | _rest])
       when is_integer(cursor) and is_integer(seq) and seq == cursor + 1 do
    {:ok, []}
  end

  defp maybe_read_cold_events(id, cursor, limit, _hot_events) do
    read_cold_events(id, cursor, limit)
  end

  defp read_cold_events(id, cursor, limit) do
    from_seq = cursor + 1
    to_seq = cursor + limit

    try do
      EventStore.read_archived_events(id, from_seq, to_seq)
    rescue
      DBConnection.OwnershipError -> {:error, :archive_read_unavailable}
    end
  end

  defp read_hot_events(id, cursor, limit) do
    {:ok, EventStore.from_cursor(id, cursor, limit)}
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
