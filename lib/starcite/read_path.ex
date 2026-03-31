defmodule Starcite.ReadPath do
  @moduledoc """
  Read path for session and event tail operations.
  """

  alias Starcite.Routing.SessionRouter
  alias Starcite.Cursor
  alias Starcite.DataPlane.{EventStore, SessionQuorum, SessionStore}
  alias Starcite.Session

  @default_tail_batch_size 1_000

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(id) when is_binary(id) and id != "" do
    # Cold misses fall back to archive header + archived-head lookup, so this
    # should not be treated as a cheap hot-path read for cold sessions.
    SessionStore.get_session(id)
  end

  def get_session(_id), do: {:error, :invalid_session_id}

  @spec get_session_routed(String.t(), boolean()) ::
          {:ok, Session.t()} | {:error, term()} | {:timeout, term()}
  def get_session_routed(id, prefer_leader \\ true)

  def get_session_routed(id, prefer_leader)
      when is_binary(id) and id != "" and is_boolean(prefer_leader) do
    SessionRouter.call(
      id,
      __MODULE__,
      :rpc_get_session,
      [id],
      __MODULE__,
      :rpc_get_session,
      [id],
      prefer_leader: prefer_leader,
      defer_local_when_unconfirmed: prefer_leader
    )
  end

  def get_session_routed(_id, _prefer_leader), do: {:error, :invalid_session_id}

  @type gap_reason :: :cursor_expired | :epoch_stale | :rollback
  @type gap_signal :: %{
          required(:reason) => gap_reason(),
          required(:from_cursor) => Cursor.t(),
          required(:next_cursor) => Cursor.t(),
          required(:committed_cursor) => Cursor.t(),
          required(:earliest_available_cursor) => Cursor.t()
        }

  @spec replay_from_cursor(String.t(), term(), pos_integer()) ::
          {:ok, [map()]} | {:gap, gap_signal()} | {:error, term()}
  def replay_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def replay_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(limit) and limit > 0 do
    with {:ok, normalized_cursor} <- Cursor.normalize(cursor),
         {:ok, snapshot} <- fetch_cursor_snapshot_routed(id),
         {:ok, earliest_available_seq} <- earliest_available_seq(id, snapshot.committed_seq) do
      case replay_gap_reason(normalized_cursor, snapshot, earliest_available_seq) do
        nil ->
          do_replay_from_cursor(id, normalized_cursor, limit, snapshot, earliest_available_seq)

        reason ->
          {:gap, gap_signal(reason, normalized_cursor, snapshot, earliest_available_seq)}
      end
    end
  end

  def replay_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    get_events_from_cursor(id, cursor, limit, true)
  end

  def get_events_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer(), boolean()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit, prefer_leader)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 and is_boolean(prefer_leader) do
    route_events_read(id, cursor, limit, prefer_leader)
  end

  def get_events_from_cursor(_id, _cursor, _limit, _prefer_leader), do: {:error, :invalid_cursor}

  @doc false
  def rpc_get_session(id) when is_binary(id) and id != "" do
    SessionQuorum.get_session(id)
  end

  def rpc_get_session(_id), do: {:error, :invalid_session_id}

  @doc false
  def rpc_get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    do_get_events_from_cursor(id, cursor, limit)
  end

  defp do_get_events_from_cursor(id, cursor, limit) do
    with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
         {:ok, cold_events} <- maybe_read_cold_events(id, cursor, limit, hot_events),
         {:ok, merged} <- merge_events(cold_events, hot_events, cursor, limit) do
      {:ok, merged}
    else
      {:error, :archive_read_unavailable} ->
        with {:ok, hot_events} <- read_hot_events(id, cursor, limit),
             {:ok, merged} <- merge_events([], hot_events, cursor, limit) do
          {:ok, merged}
        end

      {:error, reason} ->
        {:error, reason}
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

  defp merge_events(cold_events, hot_events, cursor, limit)
       when is_list(cold_events) and is_list(hot_events) and is_integer(cursor) and cursor >= 0 and
              is_integer(limit) and limit > 0 do
    merged =
      (cold_events ++ hot_events)
      |> Enum.uniq_by(& &1.seq)
      |> Enum.sort_by(& &1.seq)
      |> Enum.take(limit)

    ensure_gap_free(cursor, merged)
  end

  defp ensure_gap_free(_cursor, []), do: {:ok, []}

  defp ensure_gap_free(cursor, [%{seq: seq} | _events]) when seq != cursor + 1,
    do: {:error, :event_gap_detected}

  defp ensure_gap_free(_cursor, [_single] = events), do: {:ok, events}

  defp ensure_gap_free(_cursor, events) do
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

  defp do_replay_from_cursor(id, cursor, limit, snapshot, earliest_available_seq)
       when is_binary(id) and is_map(cursor) and is_integer(limit) and limit > 0 and
              is_map(snapshot) do
    case get_events_from_cursor(id, cursor.seq, limit) do
      {:ok, events} ->
        {:ok, attach_epoch(events, snapshot.epoch)}

      {:error, reason} when reason in [:event_gap_detected, :archive_read_unavailable] ->
        {:gap, gap_signal(:cursor_expired, cursor, snapshot, earliest_available_seq)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp replay_gap_reason(
         %{epoch: cursor_epoch, seq: cursor_seq},
         snapshot,
         earliest_available_seq
       )
       when is_map(snapshot) and is_integer(cursor_seq) and cursor_seq >= 0 do
    cond do
      cursor_seq > snapshot.last_seq ->
        :rollback

      cursor_epoch_mismatch?(cursor_epoch, snapshot.epoch) ->
        :epoch_stale

      is_integer(earliest_available_seq) and earliest_available_seq > 0 and
          cursor_seq < earliest_available_seq - 1 ->
        :cursor_expired

      true ->
        nil
    end
  end

  defp cursor_epoch_mismatch?(nil, _active_epoch), do: false

  defp cursor_epoch_mismatch?(cursor_epoch, active_epoch)
       when is_integer(cursor_epoch) and is_integer(active_epoch) and cursor_epoch >= 0 and
              active_epoch >= 0 do
    cursor_epoch != active_epoch
  end

  defp cursor_epoch_mismatch?(_cursor_epoch, _active_epoch), do: true

  defp gap_signal(reason, from_cursor, snapshot, earliest_available_seq)
       when reason in [:cursor_expired, :epoch_stale, :rollback] and is_map(from_cursor) and
              is_map(snapshot) do
    next_cursor = next_cursor_for_gap(reason, from_cursor, snapshot, earliest_available_seq)
    committed_cursor = Cursor.new(snapshot.epoch, snapshot.committed_seq)

    earliest_cursor =
      Cursor.new(
        snapshot.epoch,
        earliest_available_seq || snapshot.last_seq + 1
      )

    %{
      reason: reason,
      from_cursor: from_cursor,
      next_cursor: next_cursor,
      committed_cursor: committed_cursor,
      earliest_available_cursor: earliest_cursor
    }
  end

  defp next_cursor_for_gap(:cursor_expired, _from_cursor, snapshot, earliest_available_seq)
       when is_map(snapshot) do
    next_seq =
      cond do
        is_integer(earliest_available_seq) and earliest_available_seq > 0 ->
          earliest_available_seq - 1

        true ->
          snapshot.committed_seq
      end

    Cursor.new(snapshot.epoch, max(next_seq, 0))
  end

  defp next_cursor_for_gap(:epoch_stale, from_cursor, snapshot, _earliest_available_seq)
       when is_map(from_cursor) and is_map(snapshot) do
    next_seq = min(from_cursor.seq, snapshot.last_seq)
    Cursor.new(snapshot.epoch, max(next_seq, snapshot.committed_seq))
  end

  defp next_cursor_for_gap(:rollback, _from_cursor, snapshot, _earliest_available_seq)
       when is_map(snapshot) do
    Cursor.new(snapshot.epoch, max(snapshot.last_seq, snapshot.committed_seq))
  end

  defp attach_epoch(events, epoch)
       when is_list(events) and is_integer(epoch) and epoch >= 0 do
    Enum.map(events, fn event ->
      Map.put_new(event, :epoch, epoch)
    end)
  end

  defp earliest_available_seq(id, committed_seq)
       when is_binary(id) and id != "" and is_integer(committed_seq) and committed_seq >= 0 do
    hot_first_seq =
      case EventStore.from_cursor(id, 0, 1) do
        [%{seq: seq} | _rest] when is_integer(seq) and seq > 0 -> seq
        _ -> nil
      end

    cond do
      hot_first_seq == 1 ->
        {:ok, 1}

      is_integer(hot_first_seq) and hot_first_seq > 1 ->
        {:ok, archive_floor_or_hot_floor(id, hot_first_seq)}

      committed_seq > 0 ->
        {:ok, archive_floor_or_hot_floor(id, committed_seq + 1)}

      true ->
        {:ok, nil}
    end
  end

  defp archive_floor_or_hot_floor(id, hot_floor_seq)
       when is_binary(id) and id != "" and is_integer(hot_floor_seq) and hot_floor_seq > 0 do
    case read_archived_floor_event(id) do
      {:ok, %{seq: seq}} when is_integer(seq) and seq > 0 -> seq
      _ -> hot_floor_seq
    end
  end

  defp read_archived_floor_event(id) when is_binary(id) and id != "" do
    try do
      case EventStore.read_archived_events(id, 1, 1) do
        {:ok, [event | _rest]} -> {:ok, event}
        _ -> :error
      end
    rescue
      DBConnection.OwnershipError -> :error
    end
  end

  defp fetch_cursor_snapshot_routed(id) when is_binary(id) and id != "" do
    SessionRouter.call(
      id,
      SessionQuorum,
      :fetch_cursor_snapshot,
      [id],
      SessionQuorum,
      :fetch_cursor_snapshot,
      [id],
      prefer_leader: true,
      defer_local_when_unconfirmed: true
    )
  end

  defp route_events_read(id, cursor, limit, prefer_leader)
       when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and
              is_integer(limit) and limit > 0 and is_boolean(prefer_leader) do
    SessionRouter.call(
      id,
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      __MODULE__,
      :rpc_get_events_from_cursor,
      [id, cursor, limit],
      prefer_leader: prefer_leader,
      defer_local_when_unconfirmed: prefer_leader
    )
  end
end
