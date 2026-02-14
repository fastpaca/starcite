defmodule Starcite.Archive.Store do
  @moduledoc """
  Archive persistence and archived-read cache abstraction.

  Responsibilities:

  - resolve the configured archive adapter
  - persist archived event rows and session catalog rows
  - serve archived reads with local cache-first behavior and persistence fallback
  - manage archived-read cache memory/eviction primitives
  """

  alias Starcite.Archive.Adapter

  @default_adapter Starcite.Archive.Adapter.Postgres
  @cache :starcite_archive_read_cache
  @default_cache_chunk_size 256

  @spec adapter() :: module()
  @doc """
  Return the configured archive adapter module.
  """
  def adapter do
    Application.get_env(:starcite, :archive_adapter, @default_adapter)
  end

  @spec write_events([Adapter.event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Persist archive event rows using the configured adapter.
  """
  def write_events(rows) when is_list(rows) do
    write_events(adapter(), rows)
  end

  @spec write_events(module(), [Adapter.event_row()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Persist archive event rows using an explicit adapter.
  """
  def write_events(adapter_mod, rows) when is_atom(adapter_mod) and is_list(rows) do
    adapter_mod.write_events(rows)
  end

  @spec read_events(String.t(), pos_integer(), pos_integer()) :: {:ok, [map()]} | {:error, term()}
  @doc """
  Read archived events from cache first, then fill cache misses from persistence.
  """
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    read_events(adapter(), session_id, from_seq, to_seq)
  end

  @spec read_events(module(), String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  @doc """
  Read archived events with an explicit adapter.
  """
  def read_events(adapter_mod, session_id, from_seq, to_seq)
      when is_atom(adapter_mod) and is_binary(session_id) and session_id != "" and
             is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, cached_by_seq, missing_ranges} <-
           cached_events_and_missing_ranges(session_id, from_seq, to_seq),
         {:ok, fetched_events} <- fetch_missing_ranges(adapter_mod, session_id, missing_ranges) do
      :ok = cache_events(session_id, fetched_events)

      merged =
        cached_by_seq
        |> Map.merge(Map.new(fetched_events, fn %{seq: seq} = event -> {seq, event} end))
        |> Map.values()
        |> Enum.sort_by(& &1.seq)

      {:ok, merged}
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

  @spec upsert_session(Adapter.session_row()) :: :ok | {:error, term()}
  @doc """
  Persist one session catalog row using the configured adapter.
  """
  def upsert_session(session) when is_map(session) do
    upsert_session(adapter(), session)
  end

  @spec upsert_session(module(), Adapter.session_row()) :: :ok | {:error, term()}
  @doc """
  Persist one session catalog row using an explicit adapter.
  """
  def upsert_session(adapter_mod, session) when is_atom(adapter_mod) and is_map(session) do
    adapter_mod.upsert_session(session)
  end

  @spec list_sessions(Adapter.session_query()) :: {:ok, Adapter.session_page()} | {:error, term()}
  @doc """
  List sessions using the configured adapter.
  """
  def list_sessions(query_opts) when is_map(query_opts) do
    list_sessions(adapter(), query_opts)
  end

  @spec list_sessions(module(), Adapter.session_query()) ::
          {:ok, Adapter.session_page()} | {:error, term()}
  @doc """
  List sessions using an explicit adapter.
  """
  def list_sessions(adapter_mod, query_opts) when is_atom(adapter_mod) and is_map(query_opts) do
    adapter_mod.list_sessions(query_opts)
  end

  @spec list_sessions_by_ids([String.t()], Adapter.session_query()) ::
          {:ok, Adapter.session_page()} | {:error, term()}
  @doc """
  List sessions for an explicit set of IDs using the configured adapter.
  """
  def list_sessions_by_ids(ids, query_opts) when is_list(ids) and is_map(query_opts) do
    list_sessions_by_ids(adapter(), ids, query_opts)
  end

  @spec list_sessions_by_ids(module(), [String.t()], Adapter.session_query()) ::
          {:ok, Adapter.session_page()} | {:error, term()}
  @doc """
  List sessions for an explicit set of IDs using an explicit adapter.
  """
  def list_sessions_by_ids(adapter_mod, ids, query_opts)
      when is_atom(adapter_mod) and is_list(ids) and is_map(query_opts) do
    adapter_mod.list_sessions_by_ids(ids, query_opts)
  end

  defp fetch_missing_ranges(_adapter_mod, _session_id, []), do: {:ok, []}

  defp fetch_missing_ranges(adapter_mod, session_id, [{from_seq, to_seq} | rest])
       when is_atom(adapter_mod) and is_binary(session_id) and session_id != "" and
              is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, events} <- adapter_mod.read_events(session_id, from_seq, to_seq),
         {:ok, tail} <- fetch_missing_ranges(adapter_mod, session_id, rest) do
      {:ok, events ++ tail}
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
