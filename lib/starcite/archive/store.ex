defmodule Starcite.Archive.Store do
  @moduledoc """
  Unified archive facade for adapter-backed reads and writes.

  Cold reads are cached in Cachex by adapter and range.
  """

  alias Starcite.Archive.Adapter
  alias Starcite.Config.Size

  @cache :starcite_archive_read_cache
  @default_adapter Starcite.Archive.Adapter.Postgres
  @cache_max_size_key "archive_read_cache_max_size"
  @cache_reclaim_fraction_key "archive_read_cache_reclaim_fraction"
  @default_cache_max_size "512MB"
  @default_cache_reclaim_fraction 0.25

  @spec adapter() :: module()
  def adapter do
    Application.get_env(:starcite, :archive_adapter, @default_adapter)
  end

  @spec write_events([Adapter.event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def write_events(rows) when is_list(rows) do
    write_events(adapter(), rows)
  end

  @spec write_events(module(), [Adapter.event_row()]) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def write_events(adapter_mod, rows) when is_atom(adapter_mod) and is_list(rows) do
    adapter_mod.write_events(rows)
  end

  @spec read_events(String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, :archive_read_unavailable}
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    read_events(adapter(), session_id, from_seq, to_seq)
  end

  @spec read_events(module(), String.t(), pos_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, :archive_read_unavailable}
  def read_events(adapter_mod, session_id, from_seq, to_seq)
      when is_atom(adapter_mod) and is_binary(session_id) and session_id != "" and
             is_integer(from_seq) and
             from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq do
    key = {:archive_range, adapter_mod, session_id, from_seq, to_seq}

    case get_cached(key) do
      {:ok, events} ->
        {:ok, events}

      :miss ->
        case adapter_mod.read_events(session_id, from_seq, to_seq) do
          {:ok, events} when is_list(events) ->
            :ok = put_cached(key, events)
            {:ok, events}

          {:error, _reason} ->
            {:error, :archive_read_unavailable}

          _other ->
            {:error, :archive_read_unavailable}
        end
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp get_cached(key) do
    case Cachex.get(@cache, key) do
      {:ok, nil} ->
        :miss

      {:ok, events} when is_list(events) ->
        {:ok, events}

      _ ->
        :miss
    end
  end

  defp put_cached(key, events) when is_list(events) do
    _ = Cachex.put(@cache, key, events)
    :ok = maybe_enforce_memory_limit()
    :ok
  end

  defp maybe_enforce_memory_limit do
    max_bytes = cache_max_bytes()

    case cache_memory_bytes() do
      {:ok, current_bytes} when current_bytes > max_bytes ->
        target_bytes = reclaim_target_bytes(max_bytes)
        evict_to_target_memory(target_bytes)

      _ ->
        :ok
    end
  end

  defp evict_to_target_memory(target_bytes) when is_integer(target_bytes) and target_bytes >= 0 do
    entries =
      @cache
      |> Cachex.stream!()
      |> Enum.filter(fn
        {:entry, _key, _touched, _ttl, _value} -> true
        _other -> false
      end)
      |> Enum.sort_by(fn {:entry, _key, touched, _ttl, _value} -> touched end)

    _bytes_after =
      Enum.reduce_while(entries, cache_memory_bytes_or_zero(), fn {:entry, key, _touched, _ttl,
                                                                   _value},
                                                                  current_bytes ->
        if current_bytes <= target_bytes do
          {:halt, current_bytes}
        else
          _ = Cachex.del(@cache, key)
          {:cont, cache_memory_bytes_or_zero()}
        end
      end)

    :ok
  end

  defp reclaim_target_bytes(max_bytes) when is_integer(max_bytes) and max_bytes > 0 do
    reclaim_fraction = cache_reclaim_fraction()
    keep_fraction = 1.0 - reclaim_fraction
    trunc(max_bytes * keep_fraction)
  end

  defp cache_memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 ->
        {:ok, bytes}

      _ ->
        :error
    end
  end

  defp cache_memory_bytes_or_zero do
    case cache_memory_bytes() do
      {:ok, bytes} -> bytes
      :error -> 0
    end
  end

  defp cache_max_bytes do
    Size.parse_bytes!(
      Application.get_env(:starcite, :archive_read_cache_max_size, @default_cache_max_size),
      @cache_max_size_key,
      examples: "512MB, 4G, 262144K"
    )
  end

  defp cache_reclaim_fraction do
    validate_reclaim_fraction!(
      Application.get_env(
        :starcite,
        :archive_read_cache_reclaim_fraction,
        @default_cache_reclaim_fraction
      ),
      @cache_reclaim_fraction_key
    )
  end

  defp validate_reclaim_fraction!(value, _key)
       when is_float(value) and value >= 0.01 and value <= 0.99,
       do: value

  defp validate_reclaim_fraction!(value, _key)
       when is_integer(value) and value >= 1 and value <= 99,
       do: value / 100

  defp validate_reclaim_fraction!(value, key) when is_binary(value) do
    case Float.parse(String.trim(value)) do
      {parsed, ""} when parsed >= 0.01 and parsed <= 0.99 -> parsed
      _ -> invalid_reclaim_fraction!(value, key)
    end
  end

  defp validate_reclaim_fraction!(value, key) do
    invalid_reclaim_fraction!(value, key)
  end

  defp invalid_reclaim_fraction!(value, key) do
    raise ArgumentError,
          "invalid value for #{key}: #{inspect(value)} (expected 0.01..0.99)"
  end
end
