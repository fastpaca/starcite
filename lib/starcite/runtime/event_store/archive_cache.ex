defmodule Starcite.Runtime.EventStore.ArchiveCache do
  @moduledoc false

  @cache :starcite_archive_read_cache
  @default_cache_chunk_size 256

  @spec get_event(String.t(), pos_integer()) :: {:ok, map()} | :error
  def get_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    chunk_start = chunk_start_for(seq, cache_chunk_size())

    case Map.fetch(get_cached_chunk(session_id, chunk_start), seq) do
      {:ok, event} -> {:ok, event}
      :error -> :error
    end
  end

  @spec cached_events_and_missing_ranges(String.t(), pos_integer(), pos_integer()) ::
          {:ok, %{required(pos_integer()) => map()}, [{pos_integer(), pos_integer()}]}
  def cached_events_and_missing_ranges(session_id, from_seq, to_seq)
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

  @spec cache_events(String.t(), [map()]) :: :ok
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

  @spec clear() :: :ok
  def clear do
    case Cachex.clear(@cache) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec memory_bytes() :: {:ok, non_neg_integer()} | :error
  def memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 -> {:ok, bytes}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec memory_bytes_or_zero() :: non_neg_integer()
  def memory_bytes_or_zero do
    case memory_bytes() do
      {:ok, bytes} -> bytes
      :error -> 0
    end
  end

  @spec evict_to_target_memory(non_neg_integer()) :: :ok
  def evict_to_target_memory(target_cache_bytes)
      when is_integer(target_cache_bytes) and target_cache_bytes >= 0 do
    cache_entries =
      cache_stream()
      |> Enum.filter(fn
        {:entry, {:event_chunk, _session_id, _chunk_start}, _touched, _ttl, _value} -> true
        _other -> false
      end)
      |> Enum.sort_by(fn {:entry, _key, touched, _ttl, _value} -> touched end)

    _bytes_after =
      Enum.reduce_while(cache_entries, memory_bytes_or_zero(), fn
        {:entry, key, _touched, _ttl, _value}, current_bytes ->
          if current_bytes <= target_cache_bytes do
            {:halt, current_bytes}
          else
            _ = cache_del(key)
            {:cont, memory_bytes_or_zero()}
          end
      end)

    :ok
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
