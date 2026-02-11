defmodule Starcite.Archive.Store do
  @moduledoc """
  Unified archive facade for adapter-backed reads and writes.

  Cold reads are cached in Cachex by adapter and range.
  """

  alias Starcite.Archive.Adapter

  @cache :starcite_archive_read_cache
  @default_adapter Starcite.Archive.Adapter.Postgres

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
    :ok
  end
end
