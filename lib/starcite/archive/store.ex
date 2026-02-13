defmodule Starcite.Archive.Store do
  @moduledoc """
  Unified archive facade for adapter-backed reads and writes.

  Cold reads are cached in Cachex by adapter and range.
  """

  alias Starcite.Archive.Adapter

  @cache :starcite_archive_read_cache
  @default_adapter Starcite.Archive.Adapter.Postgres
  @default_cache_max_bytes 536_870_912
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

  @spec upsert_session(map()) :: :ok | {:error, :archive_write_unavailable}
  def upsert_session(session) when is_map(session) do
    upsert_session(adapter(), session)
  end

  @spec upsert_session(module(), map()) :: :ok | {:error, :archive_write_unavailable}
  def upsert_session(adapter_mod, session) when is_atom(adapter_mod) and is_map(session) do
    case adapter_mod.upsert_session(session) do
      :ok -> :ok
      {:error, _reason} -> {:error, :archive_write_unavailable}
      _other -> {:error, :archive_write_unavailable}
    end
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @spec list_sessions(map()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions(query_opts) when is_map(query_opts) do
    list_sessions(adapter(), query_opts)
  end

  @spec list_sessions(module(), map()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions(adapter_mod, query_opts) when is_atom(adapter_mod) and is_map(query_opts) do
    case adapter_mod.list_sessions(query_opts) do
      {:ok, %{sessions: sessions, next_cursor: next_cursor}}
      when is_list(sessions) and (is_binary(next_cursor) or is_nil(next_cursor)) ->
        {:ok, %{sessions: sessions, next_cursor: next_cursor}}

      {:error, _reason} ->
        {:error, :archive_read_unavailable}

      _other ->
        {:error, :archive_read_unavailable}
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  @spec list_sessions_by_ids([String.t()], map()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions_by_ids(ids, query_opts) when is_list(ids) and is_map(query_opts) do
    list_sessions_by_ids(adapter(), ids, query_opts)
  end

  @spec list_sessions_by_ids(module(), [String.t()], map()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions_by_ids(adapter_mod, ids, query_opts)
      when is_atom(adapter_mod) and is_list(ids) and is_map(query_opts) do
    case adapter_mod.list_sessions_by_ids(ids, query_opts) do
      {:ok, %{sessions: sessions, next_cursor: next_cursor}}
      when is_list(sessions) and (is_binary(next_cursor) or is_nil(next_cursor)) ->
        {:ok, %{sessions: sessions, next_cursor: next_cursor}}

      {:error, _reason} ->
        {:error, :archive_read_unavailable}

      _other ->
        {:error, :archive_read_unavailable}
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
    Application.get_env(:starcite, :archive_read_cache_max_bytes, @default_cache_max_bytes)
  end

  defp cache_reclaim_fraction do
    Application.get_env(
      :starcite,
      :archive_read_cache_reclaim_fraction,
      @default_cache_reclaim_fraction
    )
  end
end
