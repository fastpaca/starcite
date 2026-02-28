defmodule Starcite.DataPlane.EventStore do
  @moduledoc """
  Top-level local event store for hot-path writes and tiered reads.

  `EventStore` composes two focused submodules:

  - `EventQueue` for unarchived in-memory events
  - `Starcite.Archive.Store` for archived reads and archived-read cache

  Memory pressure is enforced across both tiers. When local memory exceeds
  `event_store_max_bytes`, cached archived reads are reclaimed and pressure is
  emitted via telemetry.
  """

  use GenServer

  alias Starcite.Archive.Store
  alias Starcite.Observability.Telemetry
  alias Starcite.Observability.Tenancy
  alias Starcite.DataPlane.EventStore.EventQueue
  alias Starcite.Session.Event

  @default_max_memory_bytes 2_147_483_648
  @default_capacity_check_interval 4
  @default_cache_reclaim_fraction 0.25
  @max_memory_limit_cache_key {__MODULE__, :max_memory_bytes_limit}
  @capacity_check_interval_cache_key {__MODULE__, :capacity_check_interval}
  @capacity_check_tick_key {__MODULE__, :capacity_check_tick}
  @archive_cache_memory_bytes_cache_key {__MODULE__, :archive_cache_memory_bytes}

  @doc """
  Start the event store owner process.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  @doc false
  def init(_opts) do
    :ok = EventQueue.ensure_tables()
    :ok = cache_archive_memory_bytes(Store.cache_memory_bytes_or_zero())
    {:ok, %{}}
  end

  @doc """
  Insert one committed event into pending local state.

  This path does not reject committed writes under pressure; it emits
  backpressure telemetry when capacity cannot be reclaimed.
  """
  @spec put_event(String.t(), Event.t()) :: :ok
  def put_event(session_id, %{seq: seq} = event)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    tenant_id = Tenancy.label_from_event(event)

    maybe_emit_backpressure(
      ensure_capacity_for_put_event(session_id, event),
      session_id,
      tenant_id
    )

    :ok = EventQueue.put_event(session_id, seq, event)
    :ok = emit_event_store_write_telemetry(session_id, tenant_id, event)
    :ok
  end

  @doc """
  Insert many committed events into pending local state.

  This path does not reject committed writes under pressure; it emits
  backpressure telemetry when capacity cannot be reclaimed.
  """
  @spec put_events(String.t(), [Event.t()]) :: :ok
  def put_events(session_id, events)
      when is_binary(session_id) and session_id != "" and is_list(events) and events != [] do
    tenant_id = tenant_label_from_events(events)
    maybe_emit_backpressure(ensure_capacity_for_puts(session_id, events), session_id, tenant_id)

    :ok = EventQueue.put_events(session_id, events)
    :ok = emit_event_store_write_telemetry(session_id, tenant_id, events)
    :ok
  end

  @doc """
  Fetch one event by `{session_id, seq}` from local tiers.
  """
  @spec get_event(String.t(), pos_integer()) :: {:ok, Event.t()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    case EventQueue.get_event(session_id, seq) do
      {:ok, event} -> {:ok, event}
      :error -> Store.get_cached_event(session_id, seq)
    end
  end

  @doc """
  Return pending events for `seq > cursor`, ordered ascending, up to `limit`.
  """
  @spec from_cursor(String.t(), non_neg_integer(), pos_integer()) :: [Event.t()]
  def from_cursor(session_id, cursor, limit)
      when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
             is_integer(limit) and limit > 0 do
    EventQueue.from_cursor(session_id, cursor, limit)
  end

  @doc """
  Read archived events for `from_seq..to_seq`.
  """
  @spec read_archived_events(String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def read_archived_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    case Store.read_events(session_id, from_seq, to_seq) do
      {:ok, _events} = ok ->
        :ok = refresh_archive_cache_memory_bytes()
        ok

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Promote archived events into local archived-read cache for one session.
  """
  @spec cache_archived_events(String.t(), [map()]) :: :ok
  def cache_archived_events(session_id, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    :ok = Store.cache_events(session_id, events)
    :ok = maybe_enforce_capacity()
    :ok = refresh_archive_cache_memory_bytes()
    :ok
  end

  @doc """
  Delete pending entries where `seq < floor_seq` for one session.
  """
  @spec delete_below(String.t(), pos_integer()) :: non_neg_integer()
  def delete_below(session_id, floor_seq)
      when is_binary(session_id) and session_id != "" and is_integer(floor_seq) and floor_seq > 0 do
    EventQueue.delete_below(session_id, floor_seq)
  end

  @doc """
  Total pending event entries currently in local state.
  """
  @spec size() :: non_neg_integer()
  def size do
    EventQueue.size()
  end

  @doc """
  Number of pending event entries for one session.
  """
  @spec session_size(String.t()) :: non_neg_integer()
  def session_size(session_id) when is_binary(session_id) and session_id != "" do
    EventQueue.session_size(session_id)
  end

  @doc """
  Approximate combined local memory usage in bytes.
  """
  @spec memory_bytes() :: non_neg_integer()
  def memory_bytes do
    EventQueue.memory_bytes() + archive_cache_memory_bytes()
  end

  @doc """
  Return all session IDs currently represented in pending local state.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    EventQueue.session_ids()
  end

  @doc """
  Return the maximum pending sequence for one session.
  """
  @spec max_seq(String.t()) :: {:ok, pos_integer()} | :error
  def max_seq(session_id) when is_binary(session_id) and session_id != "" do
    EventQueue.max_seq(session_id)
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    EventQueue.clear()
    _ = Store.clear_cache()
    :ok = cache_archive_memory_bytes(0)
    :ok
  end

  defp ensure_capacity(session_id) when is_binary(session_id) and session_id != "" do
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

  defp ensure_capacity_for_put_event(session_id, event)
       when is_binary(session_id) and session_id != "" and is_map(event) do
    if capacity_check_due?() do
      ensure_capacity(session_id)
    else
      {:ok, :capacity_check_skipped}
    end
  end

  defp ensure_capacity_for_puts(session_id, [first_event | _rest])
       when is_binary(session_id) and session_id != "" and is_map(first_event) do
    if capacity_check_due?() do
      ensure_capacity(session_id)
    else
      {:ok, :capacity_check_skipped}
    end
  end

  defp maybe_emit_backpressure(
         {:error, :event_store_backpressure, metadata},
         session_id,
         tenant_id
       )
       when is_binary(session_id) and is_binary(tenant_id) and is_map(metadata) do
    Telemetry.event_store_backpressure(
      session_id,
      tenant_id,
      metadata.current_memory_bytes,
      metadata.max_memory_bytes,
      metadata.reason
    )

    :ok
  end

  defp maybe_emit_backpressure(_result, _session_id, _tenant_id), do: :ok

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

  defp capacity_check_interval do
    raw =
      Application.get_env(
        :starcite,
        :event_store_capacity_check_interval,
        @default_capacity_check_interval
      )

    case :persistent_term.get(@capacity_check_interval_cache_key, :undefined) do
      {^raw, interval} when is_integer(interval) and interval > 0 ->
        interval

      _ ->
        interval = normalize_capacity_check_interval!(raw)
        :persistent_term.put(@capacity_check_interval_cache_key, {raw, interval})
        interval
    end
  end

  defp capacity_check_due? do
    interval = capacity_check_interval()

    if interval == 1 do
      true
    else
      counter = Process.get(@capacity_check_tick_key, 0) + 1
      Process.put(@capacity_check_tick_key, counter)
      rem(counter, interval) == 0
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
    pending_bytes = EventQueue.memory_bytes()
    target_total_bytes = reclaim_target_total_bytes(max_memory_bytes)
    target_cache_bytes = max(target_total_bytes - pending_bytes, 0)
    :ok = Store.evict_cache_to_target_memory(target_cache_bytes)
    :ok = refresh_archive_cache_memory_bytes()
    :ok
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

  defp payload_bytes(%{payload: payload}), do: :erlang.external_size(payload)
  defp payload_bytes(_event), do: 0

  defp emit_event_store_write_telemetry(session_id, tenant_id, %{seq: seq} = event)
       when is_binary(session_id) and is_binary(tenant_id) and is_integer(seq) and seq > 0 and
              is_map(event) do
    :ok = Telemetry.event_store_write(session_id, tenant_id, seq, payload_bytes(event))

    :ok
  end

  defp emit_event_store_write_telemetry(session_id, tenant_id, events)
       when is_binary(session_id) and is_binary(tenant_id) and is_list(events) do
    Enum.each(events, fn %{seq: seq} = event ->
      :ok = Telemetry.event_store_write(session_id, tenant_id, seq, payload_bytes(event))
    end)

    :ok
  end

  defp tenant_label_from_events([first_event | _rest]) when is_map(first_event) do
    Tenancy.label_from_event(first_event)
  end

  defp archive_cache_memory_bytes do
    case :persistent_term.get(@archive_cache_memory_bytes_cache_key, :undefined) do
      bytes when is_integer(bytes) and bytes >= 0 ->
        bytes

      _ ->
        bytes = Store.cache_memory_bytes_or_zero()
        :ok = cache_archive_memory_bytes(bytes)
        bytes
    end
  end

  defp refresh_archive_cache_memory_bytes do
    Store.cache_memory_bytes_or_zero()
    |> cache_archive_memory_bytes()
  end

  defp cache_archive_memory_bytes(bytes) when is_integer(bytes) and bytes >= 0 do
    :persistent_term.put(@archive_cache_memory_bytes_cache_key, bytes)
    :ok
  end

  defp normalize_max_memory_bytes!(value) when is_integer(value) and value > 0, do: value

  defp normalize_max_memory_bytes!(value) do
    raise ArgumentError,
          "invalid value for event_store_max_bytes: #{inspect(value)} (expected positive integer bytes)"
  end

  defp normalize_capacity_check_interval!(value) when is_integer(value) and value > 0, do: value

  defp normalize_capacity_check_interval!(value) do
    raise ArgumentError,
          "invalid value for event_store_capacity_check_interval: #{inspect(value)} (expected positive integer)"
  end
end
