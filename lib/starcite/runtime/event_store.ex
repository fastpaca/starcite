defmodule Starcite.Runtime.EventStore do
  @moduledoc """
  Node-local ETS event store backed by dedicated event and index tables.

  This is used to store in-flight events such that they can be read and
  referenced without interfering the write-path in Raft.

  This store is intentionally independent from Raft FSM state. Raft `apply/3`
  may mirror committed events into this table so local consumers can read
  events without traversing FSM event-log structures.
  """

  use GenServer

  alias Starcite.Observability.Telemetry
  alias Starcite.Session.Event

  # Event entries are keyed by `{session_id, seq}` and session max-sequence
  # index entries are keyed by `session_id`. This allows us to do quick
  # max_seq lookups without scanning the entire event table.
  @event_table :starcite_event_store_events
  @index_table :starcite_event_store_session_max_seq
  @default_max_memory_bytes 2_147_483_648
  @max_memory_limit_cache_key {__MODULE__, :max_memory_bytes_limit}
  @capacity_check_cache_key {__MODULE__, :capacity_check_enabled}

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    _event_table = ensure_event_table()
    _index_table = ensure_index_table()
    {:ok, %{}}
  end

  @doc """
  Insert one committed event for a session.
  """
  @spec put_event(String.t(), Event.t()) :: :ok | {:error, :event_store_backpressure}
  def put_event(session_id, %{seq: seq} = event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    case ensure_capacity(session_id, event) do
      {:ok, _current_memory_bytes} ->
        event_table = ensure_event_table()
        index_table = ensure_index_table()
        true = :ets.insert(event_table, {{session_id, seq}, event})
        :ok = update_session_max_seq(index_table, session_id, seq)

        :ok

      {:error, :event_store_backpressure, metadata} ->
        Telemetry.event_store_backpressure(
          session_id,
          metadata.current_memory_bytes,
          metadata.max_memory_bytes,
          metadata.reason
        )

        {:error, :event_store_backpressure}
    end
  end

  @doc """
  Fetch one event by exact `{session_id, seq}` key.
  """
  @spec get_event(String.t(), pos_integer()) :: {:ok, Event.t()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    event_table = ensure_event_table()

    case :ets.lookup(event_table, {session_id, seq}) do
      [{{^session_id, ^seq}, event}] -> {:ok, event}
      [] -> :error
    end
  end

  @doc """
  Return events for `seq > cursor`, ordered ascending, up to `limit`.
  """
  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [Event.t()]
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    event_table = ensure_event_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:>, :"$1", cursor}],
        [{{:"$1", :"$2"}}]
      }
    ]

    event_table
    |> select_take(ms, limit)
    |> Enum.map(&elem(&1, 1))
  end

  @doc """
  Delete entries where `seq < floor_seq` for one session.
  """
  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    event_table = ensure_event_table()
    index_table = ensure_index_table()
    max_seq_before_delete = max_seq_value(index_table, session_id)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:<, :"$1", floor_seq}],
        [true]
      }
    ]

    deleted = :ets.select_delete(event_table, ms)

    if should_drop_index?(max_seq_before_delete, floor_seq) do
      :ets.delete(index_table, session_id)
    end

    deleted
  end

  @doc """
  Total event entries currently in ETS.
  """
  @spec size() :: non_neg_integer()
  def size do
    event_table = ensure_event_table()
    table_size(event_table)
  end

  @doc """
  Number of event entries for one session.
  """
  @spec session_size(String.t()) :: non_neg_integer()
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    event_table = ensure_event_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(event_table, ms)
  end

  @doc """
  Approximate ETS memory usage for the event store table in bytes.
  """
  @spec memory_bytes() :: non_neg_integer()
  def memory_bytes do
    table = ensure_event_table()
    words = :ets.info(table, :memory) || 0
    words * :erlang.system_info(:wordsize)
  end

  @doc """
  Return all session IDs currently represented in the event store index.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    index_table = ensure_index_table()

    ms = [
      {
        {:"$1", :"$2"},
        [],
        [:"$1"]
      }
    ]

    :ets.select(index_table, ms)
  end

  @doc """
  Return the maximum mirrored sequence for one session.
  """
  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    index_table = ensure_index_table()

    case :ets.lookup(index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> {:ok, seq}
      [] -> :error
    end
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    clear_table(@event_table)
    clear_table(@index_table)

    :ok
  end

  defp ensure_event_table do
    ensure_named_table(@event_table)
  end

  defp ensure_index_table do
    ensure_named_table(@index_table)
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

  defp update_session_max_seq(index_table, session_id, seq) do
    true = :ets.insert(index_table, {session_id, seq})
    :ok
  end

  defp select_take(table, ms, limit) when limit > 0 do
    case :ets.select(table, ms, limit) do
      {rows, _continuation} when is_list(rows) ->
        rows

      :"$end_of_table" ->
        []
    end
  end

  defp ensure_capacity(session_id, event)
       when is_binary(session_id) and session_id != "" and is_map(event) do
    if capacity_check_enabled?() do
      max_memory_bytes = max_memory_bytes_limit()
      current_memory_bytes = memory_bytes()

      if current_memory_bytes >= max_memory_bytes do
        {:error, :event_store_backpressure,
         %{
           reason: :memory_limit,
           current_memory_bytes: current_memory_bytes,
           max_memory_bytes: max_memory_bytes
         }}
      else
        {:ok, current_memory_bytes}
      end
    else
      {:ok, 0}
    end
  end

  defp capacity_check_enabled? do
    raw = Application.get_env(:starcite, :event_store_capacity_check, true)

    case :persistent_term.get(@capacity_check_cache_key, :undefined) do
      {^raw, enabled} when is_boolean(enabled) ->
        enabled

      _ ->
        enabled = normalize_capacity_check!(raw)
        :persistent_term.put(@capacity_check_cache_key, {raw, enabled})
        enabled
    end
  end

  defp max_memory_bytes_limit do
    raw = Application.get_env(:starcite, :event_store_max_bytes, @default_max_memory_bytes)

    case :persistent_term.get(@max_memory_limit_cache_key, :undefined) do
      {^raw, bytes} when is_integer(bytes) and bytes > 0 ->
        bytes

      _ ->
        bytes = normalize_max_memory_bytes!(raw)
        :persistent_term.put(@max_memory_limit_cache_key, {raw, bytes})
        bytes
    end
  end

  defp table_size(table) do
    :ets.info(table, :size) || 0
  end

  defp max_seq_value(index_table, session_id) do
    case :ets.lookup(index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> seq
      _ -> nil
    end
  end

  defp should_drop_index?(nil, _floor_seq), do: true
  defp should_drop_index?(max_seq, floor_seq) when is_integer(max_seq), do: max_seq < floor_seq

  defp normalize_capacity_check!(value) when is_boolean(value), do: value

  defp normalize_capacity_check!(value) do
    raise ArgumentError,
          "invalid value for event_store_capacity_check: #{inspect(value)} (expected true/false)"
  end

  defp normalize_max_memory_bytes!(value) when is_integer(value) and value > 0, do: value

  defp normalize_max_memory_bytes!(value) do
    raise ArgumentError,
          "invalid value for event_store_max_bytes: #{inspect(value)} (expected positive integer bytes)"
  end
end
