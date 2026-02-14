defmodule Starcite.Runtime.EventStore do
  @moduledoc """
  Unified local event store abstraction for reads and flush coordination.

  This module intentionally owns both hot read tiers used by the runtime:

  - pending events in ETS (events not yet acknowledged as archived)
  - archived events in Cachex (events already flushed to durable storage)

  ## Write / Flush Flow

  1. Raft `append_event` mirrors committed events into pending ETS (`put_event/2`).
  2. `Starcite.Archive` reads pending ETS batches and persists them.
  3. Successful flushes are promoted into Cachex (`cache_archived_events/2`).
  4. `ack_archived` deletes the corresponding pending ETS prefix.

  ## Read Flow

  1. For archived ranges, `read_archived_events/3` serves from Cachex first.
  2. Any missing ranges are fetched from persistence and written through Cachex.
  3. Unarchived tail reads use pending ETS (`from_cursor/3`).

  Keeping this behavior in one module makes memory pressure policy and cache
  semantics explicit in a single place.

  ## Why This Module Is On The Write Hot Path

  `put_event/2` runs inside Raft FSM apply for every committed append. That
  means this module must stay strict about local-only operations and bounded
  work:

  - No persistence calls from `put_event/2`.
  - No unbounded scans in the append path.
  - Backpressure decisions are based on local memory only.

  ## Data Model

  - Pending ETS table (`@pending_table`) stores `{session_id, seq} => event`.
  - Pending index table (`@pending_index_table`) stores `session_id => max_seq`.
  - Cachex stores archived events as chunk maps keyed by
    `{:event_chunk, session_id, chunk_start}`.

  ## Memory Policy

  Memory pressure is evaluated against **combined** usage:

  - pending ETS bytes (must preserve write correctness)
  - Cachex bytes (best-effort hot cache, reclaimable)

  When usage crosses `event_store_max_bytes`, the module evicts cache chunks
  first (LRU-ish by `touched`) before rejecting appends with
  `:event_store_backpressure`.
  """

  use GenServer

  alias Starcite.Archive.Store, as: ArchiveStore
  alias Starcite.Observability.Telemetry
  alias Starcite.Session.Event

  # Pending entries are keyed by `{session_id, seq}` and the index table tracks
  # pending max-seq per session so we can iterate archival candidates cheaply.
  @pending_table :starcite_event_store_pending_events
  @pending_index_table :starcite_event_store_pending_session_max_seq
  @cache :starcite_archive_read_cache
  @default_max_memory_bytes 2_147_483_648
  @default_cache_chunk_size 256
  @default_cache_reclaim_fraction 0.25
  @max_memory_limit_cache_key {__MODULE__, :max_memory_bytes_limit}

  @doc """
  Start the event store owner process.

  The GenServer itself is intentionally lightweight; ETS and Cachex hold data.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  @doc false
  def init(_opts) do
    _pending_table = ensure_pending_table()
    _pending_index_table = ensure_pending_index_table()
    {:ok, %{}}
  end

  @doc """
  Insert one committed event into the pending ETS tier.

  This function is called from the Raft write path and must remain local-only.
  It performs:

  1. capacity check against combined memory budget
  2. pending ETS insert
  3. pending index update
  4. append-path telemetry emission

  Returns `{:error, :event_store_backpressure}` when memory cannot be reclaimed
  enough to safely accept additional writes.
  """
  @spec put_event(String.t(), Event.t()) :: :ok | {:error, :event_store_backpressure}
  def put_event(session_id, %{seq: seq} = event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    case ensure_capacity(session_id, event) do
      {:ok, _current_memory_bytes} ->
        pending_table = ensure_pending_table()
        pending_index_table = ensure_pending_index_table()
        true = :ets.insert(pending_table, {{session_id, seq}, event})
        :ok = update_session_max_seq(pending_index_table, session_id, seq)

        total_entries = size()
        payload_bytes = payload_bytes(event)
        total_memory_bytes = memory_bytes()

        :ok =
          Telemetry.event_store_write(
            session_id,
            seq,
            payload_bytes,
            total_entries,
            total_memory_bytes
          )

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
  Fetch one event by `{session_id, seq}` from pending ETS, then archived cache.

  Preference order intentionally preserves fresh, unflushed visibility:

  1. pending ETS
  2. archived cache
  """
  @spec get_event(String.t(), pos_integer()) :: {:ok, Event.t()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    pending_table = ensure_pending_table()

    case :ets.lookup(pending_table, {session_id, seq}) do
      [{{^session_id, ^seq}, event}] ->
        {:ok, event}

      [] ->
        case get_cached_event(session_id, seq) do
          {:ok, event} -> {:ok, event}
          :error -> :error
        end
    end
  end

  @doc """
  Return pending events for `seq > cursor`, ordered ascending, up to `limit`.

  This is used by hot-tail reads and by the archiver when preparing flush
  batches.
  """
  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [Event.t()]
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    pending_table = ensure_pending_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:>, :"$1", cursor}],
        [{{:"$1", :"$2"}}]
      }
    ]

    pending_table
    |> select_take(ms, limit)
    |> Enum.map(&elem(&1, 1))
  end

  @doc """
  Read an archived range (`from_seq..to_seq`) from cache with persistence
  fallback and write-through cache population.

  The flow is:

  1. assemble any already-cached events in range
  2. compute exact missing ranges
  3. fetch missing ranges from persistence
  4. populate cache with fetched events
  5. return a single ordered list

  Persistence failures are returned as-is from the configured archive adapter.
  """
  @spec read_archived_events(String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def read_archived_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, cached_by_seq, missing_ranges} <-
           cached_events_and_missing_ranges(session_id, from_seq, to_seq),
         {:ok, fetched_events} <- fetch_missing_ranges(session_id, missing_ranges) do
      :ok = cache_archived_events(session_id, fetched_events)

      merged =
        cached_by_seq
        |> Map.merge(Map.new(fetched_events, fn %{seq: seq} = event -> {seq, event} end))
        |> Map.values()
        |> Enum.sort_by(& &1.seq)

      {:ok, merged}
    end
  end

  @doc """
  Promote archived events into the Cachex tier for one session.

  Events are stored as fixed-size sequence chunks (default 256 events/chunk) to
  avoid range-key explosion and to keep eviction coarse but predictable.
  """
  @spec cache_archived_events(String.t(), [map()]) :: :ok
  def cache_archived_events(session_id, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    chunk_size = cache_chunk_size()

    events_by_chunk =
      Enum.reduce(events, %{}, fn event, acc ->
        put_event_in_chunk_map(acc, event, chunk_size)
      end)

    Enum.each(events_by_chunk, fn {chunk_start, additions} ->
      existing = get_cached_chunk(session_id, chunk_start)
      merged = Map.merge(existing, additions)
      :ok = put_cached_chunk(session_id, chunk_start, merged)
    end)

    :ok = maybe_enforce_capacity()
    :ok
  end

  @doc """
  Delete pending entries where `seq < floor_seq` for one session.

  This is called after archive acknowledgements to trim the pending queue. If
  the trim removes the whole pending prefix for a session, the pending index
  entry is also dropped.
  """
  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    pending_table = ensure_pending_table()
    pending_index_table = ensure_pending_index_table()
    max_seq_before_delete = max_seq_value(pending_index_table, session_id)

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [{:<, :"$1", floor_seq}],
        [true]
      }
    ]

    deleted = :ets.select_delete(pending_table, ms)

    if should_drop_index?(max_seq_before_delete, floor_seq) do
      :ets.delete(pending_index_table, session_id)
    end

    deleted
  end

  @doc """
  Total pending event entries currently in ETS.
  """
  @spec size() :: non_neg_integer()
  def size do
    pending_table = ensure_pending_table()
    table_size(pending_table)
  end

  @doc """
  Number of pending event entries for one session.
  """
  @spec session_size(String.t()) :: non_neg_integer()
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    pending_table = ensure_pending_table()

    ms = [
      {
        {{session_id, :"$1"}, :"$2"},
        [],
        [true]
      }
    ]

    :ets.select_count(pending_table, ms)
  end

  @doc """
  Approximate combined memory usage (pending ETS + archived Cachex) in bytes.

  This value drives write-path backpressure and cache reclamation decisions.
  """
  @spec memory_bytes() :: non_neg_integer()
  def memory_bytes do
    pending_memory_bytes() + cache_memory_bytes_or_zero()
  end

  @doc """
  Return all session IDs currently represented in the pending index.

  This is the archiver's session discovery primitive.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    pending_index_table = ensure_pending_index_table()

    ms = [
      {
        {:"$1", :"$2"},
        [],
        [:"$1"]
      }
    ]

    :ets.select(pending_index_table, ms)
  end

  @doc """
  Return the maximum pending sequence for one session.
  """
  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    pending_index_table = ensure_pending_index_table()

    case :ets.lookup(pending_index_table, session_id) do
      [{^session_id, seq}] when is_integer(seq) and seq > 0 -> {:ok, seq}
      [] -> :error
    end
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    clear_table(@pending_table)
    clear_table(@pending_index_table)
    _ = cache_clear()

    :ok
  end

  defp ensure_pending_table do
    ensure_named_table(@pending_table)
  end

  defp ensure_pending_index_table do
    ensure_named_table(@pending_index_table)
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
    max_memory_bytes = max_memory_bytes_limit()
    current_memory_bytes = memory_bytes()

    if current_memory_bytes >= max_memory_bytes do
      :ok = maybe_reclaim_cache(max_memory_bytes)
      memory_after_reclaim = memory_bytes()

      if memory_after_reclaim >= max_memory_bytes do
        {:error, :event_store_backpressure,
         %{
           reason: :memory_limit,
           current_memory_bytes: memory_after_reclaim,
           max_memory_bytes: max_memory_bytes
         }}
      else
        {:ok, memory_after_reclaim}
      end
    else
      {:ok, current_memory_bytes}
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

  # Cache capacity enforcement runs outside append for cache write-through paths.
  defp maybe_enforce_capacity do
    max_memory_bytes = max_memory_bytes_limit()

    if memory_bytes() > max_memory_bytes do
      :ok = maybe_reclaim_cache(max_memory_bytes)
    end

    :ok
  end

  # Reclaim target keeps headroom so we do not oscillate at the hard limit.
  defp maybe_reclaim_cache(max_memory_bytes)
       when is_integer(max_memory_bytes) and max_memory_bytes > 0 do
    pending_bytes = pending_memory_bytes()
    target_total_bytes = reclaim_target_total_bytes(max_memory_bytes)
    target_cache_bytes = max(target_total_bytes - pending_bytes, 0)
    evict_cache_to_target_memory(target_cache_bytes)
  end

  defp reclaim_target_total_bytes(max_memory_bytes)
       when is_integer(max_memory_bytes) and max_memory_bytes > 0 do
    reclaim_fraction = cache_reclaim_fraction()
    keep_fraction = 1.0 - reclaim_fraction
    trunc(max_memory_bytes * keep_fraction)
  end

  defp evict_cache_to_target_memory(target_cache_bytes)
       when is_integer(target_cache_bytes) and target_cache_bytes >= 0 do
    cache_entries =
      cache_stream()
      |> Enum.filter(fn
        {:entry, {:event_chunk, _session_id, _chunk_start}, _touched, _ttl, _value} -> true
        _other -> false
      end)
      |> Enum.sort_by(fn {:entry, _key, touched, _ttl, _value} -> touched end)

    _bytes_after =
      Enum.reduce_while(cache_entries, cache_memory_bytes_or_zero(), fn
        {:entry, key, _touched, _ttl, _value}, current_bytes ->
          if current_bytes <= target_cache_bytes do
            {:halt, current_bytes}
          else
            _ = cache_del(key)
            {:cont, cache_memory_bytes_or_zero()}
          end
      end)

    :ok
  end

  defp pending_memory_bytes do
    table = ensure_pending_table()
    words = :ets.info(table, :memory) || 0
    words * :erlang.system_info(:wordsize)
  end

  defp cache_memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 ->
        {:ok, bytes}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp cache_memory_bytes_or_zero do
    case cache_memory_bytes() do
      {:ok, bytes} -> bytes
      :error -> 0
    end
  end

  defp cache_reclaim_fraction do
    case Application.get_env(
           :starcite,
           :archive_read_cache_reclaim_fraction,
           @default_cache_reclaim_fraction
         ) do
      fraction when is_number(fraction) and fraction > 0.0 and fraction < 1.0 ->
        fraction

      _ ->
        @default_cache_reclaim_fraction
    end
  end

  defp cache_chunk_size do
    case Application.get_env(:starcite, :event_store_cache_chunk_size, @default_cache_chunk_size) do
      size when is_integer(size) and size > 0 ->
        size

      _ ->
        @default_cache_chunk_size
    end
  end

  defp get_cached_event(session_id, seq)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    chunk_start = chunk_start_for(seq, cache_chunk_size())

    case Map.fetch(get_cached_chunk(session_id, chunk_start), seq) do
      {:ok, event} -> {:ok, event}
      :error -> :error
    end
  end

  defp cached_events_and_missing_ranges(session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    chunk_size = cache_chunk_size()

    cached_by_seq =
      chunk_starts_for_range(from_seq, to_seq, chunk_size)
      |> Enum.reduce(%{}, fn chunk_start, acc ->
        chunk = get_cached_chunk(session_id, chunk_start)

        Enum.reduce(chunk, acc, fn
          {seq, event}, inner when is_integer(seq) and seq >= from_seq and seq <= to_seq ->
            Map.put(inner, seq, event)

          _, inner ->
            inner
        end)
      end)

    present_seqs =
      cached_by_seq
      |> Map.keys()
      |> Enum.sort()

    {:ok, cached_by_seq, missing_ranges(from_seq, to_seq, present_seqs)}
  end

  defp fetch_missing_ranges(_session_id, []), do: {:ok, []}

  defp fetch_missing_ranges(session_id, [{from_seq, to_seq} | rest])
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, events} <- ArchiveStore.read_events(session_id, from_seq, to_seq),
         {:ok, tail} <- fetch_missing_ranges(session_id, rest) do
      {:ok, events ++ tail}
    end
  end

  defp put_event_in_chunk_map(acc, %{seq: seq} = event, chunk_size)
       when is_map(acc) and is_integer(seq) and seq > 0 and is_integer(chunk_size) and
              chunk_size > 0 do
    chunk_start = chunk_start_for(seq, chunk_size)
    normalized_event = normalize_cached_event(event)

    Map.update(
      acc,
      chunk_start,
      %{seq => normalized_event},
      &Map.put(&1, seq, normalized_event)
    )
  end

  defp put_event_in_chunk_map(_acc, event, _chunk_size) do
    raise ArgumentError,
          "invalid archived cache event shape: #{inspect(event)} (expected map with positive integer :seq)"
  end

  defp normalize_cached_event(%{} = event) do
    Map.delete(event, :session_id)
  end

  defp missing_ranges(from_seq, to_seq, present_seqs)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_list(present_seqs) do
    {ranges, next_expected} =
      Enum.reduce(present_seqs, {[], from_seq}, fn
        seq, {acc, expected} when seq < expected ->
          {acc, expected}

        seq, {acc, expected} when seq == expected ->
          {acc, expected + 1}

        seq, {acc, expected} ->
          {[{expected, seq - 1} | acc], seq + 1}
      end)

    ranges =
      if next_expected <= to_seq do
        [{next_expected, to_seq} | ranges]
      else
        ranges
      end

    Enum.reverse(ranges)
  end

  defp chunk_starts_for_range(from_seq, to_seq, chunk_size)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_integer(chunk_size) and chunk_size > 0 do
    first = chunk_start_for(from_seq, chunk_size)
    last = chunk_start_for(to_seq, chunk_size)

    first
    |> Stream.iterate(&(&1 + chunk_size))
    |> Enum.take_while(&(&1 <= last))
  end

  defp chunk_start_for(seq, chunk_size)
       when is_integer(seq) and seq > 0 and is_integer(chunk_size) and chunk_size > 0 do
    div(seq - 1, chunk_size) * chunk_size + 1
  end

  defp chunk_key(session_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    {:event_chunk, session_id, chunk_start}
  end

  defp get_cached_chunk(session_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    key = chunk_key(session_id, chunk_start)

    case cache_get(key) do
      {:ok, chunk} when is_map(chunk) ->
        chunk

      _ ->
        %{}
    end
  end

  defp put_cached_chunk(session_id, chunk_start, chunk)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 and
              is_map(chunk) do
    key = chunk_key(session_id, chunk_start)
    _ = cache_put(key, chunk)
    :ok
  end

  defp cache_get(key) do
    case Cachex.get(@cache, key) do
      {:ok, value} -> {:ok, value}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_put(key, value) do
    case Cachex.put(@cache, key, value) do
      {:ok, true} -> :ok
      {:ok, _other} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_del(key) do
    case Cachex.del(@cache, key) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_clear do
    case Cachex.clear(@cache) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_stream do
    Cachex.stream!(@cache)
  rescue
    _ -> []
  end

  defp payload_bytes(%{payload: payload}), do: :erlang.external_size(payload)
  defp payload_bytes(_event), do: 0

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

  defp normalize_max_memory_bytes!(value) when is_integer(value) and value > 0, do: value

  defp normalize_max_memory_bytes!(value) do
    raise ArgumentError,
          "invalid value for event_store_max_bytes: #{inspect(value)} (expected positive integer bytes)"
  end
end
