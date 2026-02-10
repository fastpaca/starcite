defmodule Starcite.Runtime.EventStore do
  @moduledoc """
  Node-local ETS event store keyed by `{session_id, seq}`.

  This store is intentionally independent from Raft FSM state. Raft `apply/3`
  may mirror committed events into this table so local consumers can read
  events without traversing FSM event-log structures.
  """

  use GenServer

  alias Starcite.Observability.Telemetry
  alias Starcite.Session.EventLog

  @table :starcite_event_store
  @session_index_table :starcite_event_store_sessions

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    _table = ensure_table()
    _session_index_table = ensure_session_index_table()
    {:ok, %{}}
  end

  @doc """
  Insert one committed event for a session.
  """
  @spec put_event(String.t(), EventLog.event()) :: :ok
  def put_event(session_id, %{seq: seq} = event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    table = ensure_table()
    session_index_table = ensure_session_index_table()
    true = :ets.insert(table, {{session_id, seq}, event})
    :ok = update_session_max_seq(session_index_table, session_id, seq)

    Telemetry.event_store_write(
      session_id,
      seq,
      byte_size(Jason.encode!(event.payload)),
      size()
    )

    :ok
  end

  @doc """
  Fetch one event by exact `{session_id, seq}` key.
  """
  @spec get_event(String.t(), pos_integer()) :: {:ok, EventLog.event()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    table = ensure_table()

    case :ets.lookup(table, {session_id, seq}) do
      [{{^session_id, ^seq}, event}] -> {:ok, event}
      [] -> :error
    end
  end

  @doc """
  Return events for `seq > cursor`, ordered ascending, up to `limit`.
  """
  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [EventLog.event()]
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    table = ensure_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:>, :"$1", cursor}],
        [{{:"$1", :"$2"}}]
      }
    ]

    table
    |> select_take(ms, limit)
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(&elem(&1, 1))
  end

  @doc """
  Delete entries where `seq < floor_seq` for one session.
  """
  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    table = ensure_table()
    session_index_table = ensure_session_index_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:<, :"$1", floor_seq}],
        [true]
      }
    ]

    deleted = :ets.select_delete(table, ms)

    if session_size(session_id) == 0 do
      :ets.delete(session_index_table, session_id)
    end

    deleted
  end

  @doc """
  Total event entries currently in ETS.
  """
  @spec size() :: non_neg_integer()
  def size do
    table = ensure_table()
    :ets.info(table, :size) || 0
  end

  @doc """
  Number of event entries for one session.
  """
  @spec session_size(String.t()) :: non_neg_integer()
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    table = ensure_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(table, ms)
  end

  @doc """
  Return all session IDs currently represented in the event store index.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    session_index_table = ensure_session_index_table()

    session_index_table
    |> :ets.tab2list()
    |> Enum.map(fn {session_id, _max_seq} -> session_id end)
  end

  @doc """
  Return the maximum mirrored sequence for one session.
  """
  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    session_index_table = ensure_session_index_table()

    case :ets.lookup(session_index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> {:ok, seq}
      [] -> :error
    end
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    case :ets.whereis(@table) do
      :undefined ->
        :ok

      table ->
        :ets.delete_all_objects(table)
    end

    case :ets.whereis(@session_index_table) do
      :undefined ->
        :ok

      table ->
        :ets.delete_all_objects(table)
    end

    :ok
  end

  defp ensure_table do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [
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

  defp ensure_session_index_table do
    case :ets.whereis(@session_index_table) do
      :undefined ->
        :ets.new(@session_index_table, [
          :set,
          :named_table,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])

      table ->
        table
    end
  end

  defp update_session_max_seq(session_index_table, session_id, seq) do
    case :ets.lookup(session_index_table, session_id) do
      [] ->
        true = :ets.insert(session_index_table, {session_id, seq})
        :ok

      [{^session_id, max_seq}] when is_integer(max_seq) and max_seq >= seq ->
        :ok

      [{^session_id, _max_seq}] ->
        true = :ets.insert(session_index_table, {session_id, seq})
        :ok
    end
  end

  defp select_take(table, ms, limit) when limit > 0 do
    case :ets.select(table, ms, limit) do
      {rows, _continuation} when is_list(rows) ->
        rows

      :"$end_of_table" ->
        []
    end
  end
end
