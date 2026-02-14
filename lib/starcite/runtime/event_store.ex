defmodule Starcite.Runtime.EventStore do
  @moduledoc """
  Unified local event store abstraction for write-path state, flush
  coordination, and pressure-aware reads.

  ## Write / Flush Flow

  1. Raft `append_event` mirrors committed events into pending local state (`put_event/2`).
  2. `Starcite.Archive` persists pending batches.
  3. Successful flushes are promoted into local archived-read cache (`cache_archived_events/2`).
  4. `ack_archived` advances archival progress and trims pending state.

  ## Read Flow

  1. `get_event/2` prefers local state.
  2. `read_archived_events/3` serves archived ranges from local cache first, then fills misses from persistence.
  3. `from_cursor/3` serves the pending tail.

  ## Write Hot Path Constraints

  `put_event/2` runs inside Raft FSM apply for every committed append and must
  remain local-only and bounded.

  ## Memory Policy

  Memory pressure is evaluated against combined local usage. When usage crosses
  `event_store_max_bytes`, the module reclaims archived-read cache first before
  rejecting appends with `:event_store_backpressure`.
  """

  use GenServer

  alias Starcite.Observability.Telemetry
  alias Starcite.Runtime.EventStore.{ArchiveCache, Pending}
  alias Starcite.Session.Event

  @default_max_memory_bytes 2_147_483_648
  @default_cache_reclaim_fraction 0.25
  @max_memory_limit_cache_key {__MODULE__, :max_memory_bytes_limit}

  @doc """
  Start the event store owner process.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  @doc false
  def init(_opts) do
    :ok = Pending.ensure_tables()
    {:ok, %{}}
  end

  @doc """
  Insert one committed event into pending local state.

  Returns `{:error, :event_store_backpressure}` when memory cannot be reclaimed
  enough to safely accept additional writes.
  """
  @spec put_event(String.t(), Event.t()) :: :ok | {:error, :event_store_backpressure}
  def put_event(session_id, %{seq: seq} = event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    case ensure_capacity(session_id, event) do
      {:ok, _current_memory_bytes} ->
        :ok = Pending.put_event(session_id, seq, event)

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
  Fetch one event by `{session_id, seq}` from pending state, then archived cache.
  """
  @spec get_event(String.t(), pos_integer()) :: {:ok, Event.t()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    case Pending.get_event(session_id, seq) do
      {:ok, event} -> {:ok, event}
      :error -> ArchiveCache.get_event(session_id, seq)
    end
  end

  @doc """
  Return pending events for `seq > cursor`, ordered ascending, up to `limit`.
  """
  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [Event.t()]
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    Pending.from_cursor(session_id, cursor, limit)
  end

  @doc """
  Read an archived range (`from_seq..to_seq`) from local cache with persistence
  fallback and write-through cache population.

  Persistence failures are returned as-is from the configured archive adapter.
  """
  @spec read_archived_events(String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def read_archived_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, cached_by_seq, missing_ranges} <-
           ArchiveCache.cached_events_and_missing_ranges(session_id, from_seq, to_seq),
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
  Promote archived events into local archived-read cache for one session.
  """
  @spec cache_archived_events(String.t(), [map()]) :: :ok
  def cache_archived_events(session_id, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    :ok = ArchiveCache.cache_events(session_id, events)
    :ok = maybe_enforce_capacity()
    :ok
  end

  @doc """
  Delete pending entries where `seq < floor_seq` for one session.
  """
  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    Pending.delete_below(session_id, floor_seq)
  end

  @doc """
  Total pending event entries currently in local state.
  """
  @spec size() :: non_neg_integer()
  def size do
    Pending.size()
  end

  @doc """
  Number of pending event entries for one session.
  """
  @spec session_size(String.t()) :: non_neg_integer()
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    Pending.session_size(session_id)
  end

  @doc """
  Approximate combined local memory usage in bytes.
  """
  @spec memory_bytes() :: non_neg_integer()
  def memory_bytes do
    Pending.memory_bytes() + ArchiveCache.memory_bytes_or_zero()
  end

  @doc """
  Return all session IDs currently represented in pending local state.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    Pending.session_ids()
  end

  @doc """
  Return the maximum pending sequence for one session.
  """
  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    Pending.max_seq(session_id)
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    Pending.clear()
    :ok = ArchiveCache.clear()
    :ok
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

  defp maybe_enforce_capacity do
    max_memory_bytes = max_memory_bytes_limit()

    if memory_bytes() > max_memory_bytes do
      :ok = maybe_reclaim_cache(max_memory_bytes)
    end

    :ok
  end

  defp maybe_reclaim_cache(max_memory_bytes)
       when is_integer(max_memory_bytes) and max_memory_bytes > 0 do
    pending_bytes = Pending.memory_bytes()
    target_total_bytes = reclaim_target_total_bytes(max_memory_bytes)
    target_cache_bytes = max(target_total_bytes - pending_bytes, 0)
    ArchiveCache.evict_to_target_memory(target_cache_bytes)
  end

  defp reclaim_target_total_bytes(max_memory_bytes)
       when is_integer(max_memory_bytes) and max_memory_bytes > 0 do
    reclaim_fraction = cache_reclaim_fraction()
    keep_fraction = 1.0 - reclaim_fraction
    trunc(max_memory_bytes * keep_fraction)
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

  defp fetch_missing_ranges(_session_id, []), do: {:ok, []}

  defp fetch_missing_ranges(session_id, [{from_seq, to_seq} | rest])
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, events} <- Starcite.Archive.adapter().read_events(session_id, from_seq, to_seq),
         {:ok, tail} <- fetch_missing_ranges(session_id, rest) do
      {:ok, events ++ tail}
    end
  end

  defp payload_bytes(%{payload: payload}), do: :erlang.external_size(payload)
  defp payload_bytes(_event), do: 0

  defp normalize_max_memory_bytes!(value) when is_integer(value) and value > 0, do: value

  defp normalize_max_memory_bytes!(value) do
    raise ArgumentError,
          "invalid value for event_store_max_bytes: #{inspect(value)} (expected positive integer bytes)"
  end
end
