defmodule Starcite.Runtime.EventStore.EventQueue do
  @moduledoc """
  In-memory write queue for `Starcite.Runtime.EventStore`.

  This module owns the unarchived session tail and exposes queue operations
  used by append, replay, and archive acknowledgement flows.
  """

  @table :starcite_event_store_pending_events
  @index_table :starcite_event_store_pending_session_max_seq

  @spec ensure_tables() :: :ok
  @doc """
  Ensure pending tier storage is ready.
  """
  def ensure_tables do
    _ = ensure_named_table(@table)
    _ = ensure_named_table(@index_table)
    :ok
  end

  @spec put_event(String.t(), pos_integer(), map()) :: :ok
  @doc """
  Persist one committed event into pending state and advance the per-session max
  sequence index.
  """
  def put_event(session_id, seq, event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
             is_map(event) do
    table = ensure_named_table(@table)
    index_table = ensure_named_table(@index_table)

    true = :ets.insert(table, {{session_id, seq}, event})
    true = :ets.insert(index_table, {session_id, seq})
    :ok
  end

  @spec get_event(String.t(), pos_integer()) :: {:ok, map()} | :error
  @doc """
  Fetch one pending event by session and sequence.
  """
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    table = ensure_named_table(@table)

    case :ets.lookup(table, {session_id, seq}) do
      [{{^session_id, ^seq}, event}] -> {:ok, event}
      [] -> :error
    end
  end

  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [map()]
  @doc """
  Return pending events strictly after `cursor`, ordered by sequence, up to
  `limit`.
  """
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    table = ensure_named_table(@table)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:>, :"$1", cursor}],
        [{{:"$1", :"$2"}}]
      }
    ]

    table
    |> select_take(ms, limit)
    |> Enum.map(&elem(&1, 1))
  end

  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  @doc """
  Delete pending events with sequence lower than `floor_seq` for one session.
  """
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    table = ensure_named_table(@table)
    index_table = ensure_named_table(@index_table)
    max_seq_before_delete = max_seq_value(index_table, session_id)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:<, :"$1", floor_seq}],
        [true]
      }
    ]

    deleted = :ets.select_delete(table, ms)

    if should_drop_index?(max_seq_before_delete, floor_seq) do
      :ets.delete(index_table, session_id)
    end

    deleted
  end

  @spec size() :: non_neg_integer()
  @doc """
  Return total pending event count.
  """
  def size do
    table = ensure_named_table(@table)
    :ets.info(table, :size) || 0
  end

  @spec session_size(String.t()) :: non_neg_integer()
  @doc """
  Return pending event count for one session.
  """
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    table = ensure_named_table(@table)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(table, ms)
  end

  @spec session_ids() :: [String.t()]
  @doc """
  Return session IDs currently represented in pending state.
  """
  def session_ids do
    index_table = ensure_named_table(@index_table)

    ms = [
      {
        {:"$1", :"$2"},
        [],
        [:"$1"]
      }
    ]

    :ets.select(index_table, ms)
  end

  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  @doc """
  Return the highest pending sequence for one session.
  """
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    index_table = ensure_named_table(@index_table)

    case :ets.lookup(index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> {:ok, seq}
      [] -> :error
    end
  end

  @spec clear() :: :ok
  @doc """
  Clear all pending state.
  """
  def clear do
    clear_table(@table)
    clear_table(@index_table)
    :ok
  end

  @spec memory_bytes() :: non_neg_integer()
  @doc """
  Return approximate memory consumed by pending state.
  """
  def memory_bytes do
    table = ensure_named_table(@table)
    words = :ets.info(table, :memory) || 0
    words * :erlang.system_info(:wordsize)
  end

  defp ensure_named_table(table_name) when is_atom(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [
          :ordered_set,
          :named_table,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      table ->
        table
    end
  end

  defp clear_table(table_name) when is_atom(table_name) do
    case :ets.whereis(table_name) do
      :undefined -> :ok
      table -> :ets.delete_all_objects(table)
    end
  end

  defp select_take(table, ms, limit) when limit > 0 do
    case :ets.select(table, ms, limit) do
      {rows, _continuation} when is_list(rows) -> rows
      :"$end_of_table" -> []
    end
  end

  defp max_seq_value(index_table, session_id) do
    case :ets.lookup(index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> seq
      _ -> nil
    end
  end

  defp should_drop_index?(nil, _floor_seq), do: true
  defp should_drop_index?(max_seq, floor_seq) when is_integer(max_seq), do: max_seq < floor_seq
end
