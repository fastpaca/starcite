defmodule Starcite.Archive.Store do
  @moduledoc """
  Unified archive facade for adapter-backed persistence reads and writes.

  This module is intentionally persistence-only. Runtime hot-cache behavior
  lives in `Starcite.Runtime.EventStore`.
  """

  alias Starcite.Archive.Adapter

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
    case adapter_mod.read_events(session_id, from_seq, to_seq) do
      {:ok, events} when is_list(events) ->
        {:ok, events}

      {:error, _reason} ->
        {:error, :archive_read_unavailable}

      _other ->
        {:error, :archive_read_unavailable}
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  @spec upsert_session(Adapter.session_row()) :: :ok | {:error, :archive_write_unavailable}
  def upsert_session(session) when is_map(session) do
    upsert_session(adapter(), session)
  end

  @spec upsert_session(module(), Adapter.session_row()) ::
          :ok | {:error, :archive_write_unavailable}
  def upsert_session(adapter_mod, session) when is_atom(adapter_mod) and is_map(session) do
    case adapter_mod.upsert_session(session) do
      :ok -> :ok
      {:error, _reason} -> {:error, :archive_write_unavailable}
      _other -> {:error, :archive_write_unavailable}
    end
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @spec list_sessions(Adapter.session_query()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions(query_opts) when is_map(query_opts) do
    list_sessions(adapter(), query_opts)
  end

  @spec list_sessions(module(), Adapter.session_query()) ::
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

  @spec list_sessions_by_ids([String.t()], Adapter.session_query()) ::
          {:ok, Adapter.session_page()} | {:error, :archive_read_unavailable}
  def list_sessions_by_ids(ids, query_opts) when is_list(ids) and is_map(query_opts) do
    list_sessions_by_ids(adapter(), ids, query_opts)
  end

  @spec list_sessions_by_ids(module(), [String.t()], Adapter.session_query()) ::
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
end
