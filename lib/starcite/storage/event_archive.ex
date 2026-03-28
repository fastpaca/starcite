defmodule Starcite.Storage.EventArchive do
  @moduledoc """
  Concrete archived event persistence and archived-read cache boundary.

  Archived events are stored in S3 through `Starcite.Storage.EventArchive.S3`.
  This module owns archived-read caching and tenant-aware reads on top of that
  concrete storage.
  """

  alias Starcite.DataPlane.SessionStore
  alias Starcite.Storage.{EventArchive.S3, SessionCatalog}

  @cache :starcite_archive_read_cache
  @default_cache_chunk_size 256

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @type event_row :: %{
          required(:session_id) => String.t(),
          required(:seq) => non_neg_integer(),
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          required(:producer_id) => String.t(),
          required(:producer_seq) => pos_integer(),
          required(:tenant_id) => String.t(),
          optional(:source) => String.t() | nil,
          required(:metadata) => map(),
          required(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  @doc """
  Start the concrete archived event storage runtime.
  """
  def start_link(opts \\ []) when is_list(opts) do
    S3.start_link(opts)
  end

  @spec write_events([event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Persist archive event rows to the concrete event archive.
  """
  def write_events(rows) when is_list(rows) do
    S3.write_events(rows)
  end

  @spec read_events(String.t(), pos_integer(), pos_integer()) :: {:ok, [map()]} | {:error, term()}
  @doc """
  Read archived events from cache first, then fill cache misses from persistence.
  """
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    case tenant_id_for(session_id) do
      {:ok, tenant_id} ->
        with {:ok, cached_by_seq, missing_ranges} <-
               cached_events_and_missing_ranges(session_id, from_seq, to_seq),
             {:ok, fetched_events} <-
               fetch_missing_ranges(session_id, tenant_id, missing_ranges) do
          :ok = cache_events(session_id, fetched_events)

          merged =
            cached_by_seq
            |> Map.merge(Map.new(fetched_events, fn %{seq: seq} = event -> {seq, event} end))
            |> Map.values()
            |> Enum.sort_by(& &1.seq)

          {:ok, merged}
        end

      {:error, :session_not_found} ->
        {:ok, []}

      {:error, _reason} = error ->
        error
    end
  end

  @spec get_cached_event(String.t(), pos_integer()) :: {:ok, map()} | :error
  @doc """
  Return one archived event from local cache by session and sequence.
  """
  def get_cached_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    chunk_start = chunk_start_for(seq, cache_chunk_size())

    case Map.fetch(get_cached_chunk(session_id, chunk_start), seq) do
      {:ok, event} -> {:ok, event}
      :error -> :error
    end
  end

  @spec cache_events(String.t(), [map()]) :: :ok
  @doc """
  Insert archived events into local archived-read cache.
  """
  def cache_events(session_id, events)
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

    :ok
  end

  @spec clear_cache() :: :ok | :error
  @doc """
  Clear all archived-read cache entries.
  """
  def clear_cache do
    case Cachex.clear(@cache) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec cache_memory_bytes() :: {:ok, non_neg_integer()} | :error
  @doc """
  Return archived-read cache memory usage in bytes.
  """
  def cache_memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 -> {:ok, bytes}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec cache_memory_bytes_or_zero() :: non_neg_integer()
  @doc """
  Return archived-read cache memory usage, falling back to `0` on inspection
  failures.
  """
  def cache_memory_bytes_or_zero do
    case cache_memory_bytes() do
      {:ok, bytes} -> bytes
      :error -> 0
    end
  end

  @spec evict_cache_to_target_memory(non_neg_integer()) :: :ok
  @doc """
  Evict archived-read cache entries until usage is at or below
  `target_cache_bytes`.
  """
  def evict_cache_to_target_memory(target_cache_bytes)
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

  defp fetch_missing_ranges(_session_id, _tenant_id, []), do: {:ok, []}

  defp fetch_missing_ranges(session_id, tenant_id, [{from_seq, to_seq} | rest])
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, events} <- S3.read_events(session_id, tenant_id, from_seq, to_seq),
         {:ok, tail} <- fetch_missing_ranges(session_id, tenant_id, rest) do
      {:ok, events ++ tail}
    end
  end

  defp tenant_id_for(session_id) when is_binary(session_id) and session_id != "" do
    case SessionStore.get_session_cached(session_id) do
      {:ok, %{tenant_id: tenant_id}} when is_binary(tenant_id) and tenant_id != "" ->
        {:ok, tenant_id}

      :error ->
        case SessionCatalog.get_tenant_id(session_id) do
          {:ok, tenant_id} when is_binary(tenant_id) and tenant_id != "" ->
            {:ok, tenant_id}

          {:error, _reason} = error ->
            error

          _other ->
            {:error, :session_not_found}
        end
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

  defp cache_chunk_size do
    case Application.get_env(:starcite, :event_store_cache_chunk_size, @default_cache_chunk_size) do
      size when is_integer(size) and size > 0 -> size
      _ -> @default_cache_chunk_size
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

  defp cache_stream do
    Cachex.stream!(@cache)
  rescue
    _ -> []
  end
end
