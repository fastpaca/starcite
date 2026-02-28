defmodule Starcite.DataPlane.RaftAccess do
  @moduledoc """
  Shared Raft lookup/start/query helpers used by read and write service paths.
  """

  alias Starcite.DataPlane.RaftManager
  alias Starcite.Session
  alias Starcite.DataPlane.RaftFSM

  @spec group_for_session(String.t()) :: non_neg_integer()
  def group_for_session(id) when is_binary(id) and id != "" do
    RaftManager.group_for_session(id)
  end

  @spec local_server_for_group(non_neg_integer()) :: {:ok, atom()} | :error
  def local_server_for_group(group_id) when is_integer(group_id) and group_id >= 0 do
    server_id = RaftManager.server_id(group_id)

    if Process.whereis(server_id) != nil do
      {:ok, server_id}
    else
      :error
    end
  end

  @spec locate_and_ensure_started(String.t()) ::
          {:ok, atom(), non_neg_integer()} | {:error, term()}
  def locate_and_ensure_started(id) when is_binary(id) and id != "" do
    group_id = group_for_session(id)
    server_id = RaftManager.server_id(group_id)

    with :ok <- ensure_group_started(server_id, group_id) do
      {:ok, server_id, group_id}
    end
  end

  @spec query_session(atom(), String.t()) :: {:ok, Session.t()} | {:error, term()}
  def query_session(server_id, id)
      when is_atom(server_id) and is_binary(id) and id != "" do
    case :ra.consistent_query({server_id, Node.self()}, fn state ->
           RaftFSM.query_session(state, id)
         end) do
      {:ok, {:ok, session}, _leader} ->
        {:ok, session}

      {:ok, {:error, reason}, _leader} ->
        {:error, reason}

      {:ok, {{_term, _index}, {:ok, session}}, _leader} ->
        {:ok, session}

      {:ok, {{_term, _index}, {:error, reason}}, _leader} ->
        {:error, reason}

      {:timeout, leader} ->
        {:error, {:timeout, leader}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec fetch_archived_seq(String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fetch_archived_seq(id) when is_binary(id) and id != "" do
    with {:ok, server_id, _group_id} <- locate_and_ensure_started(id) do
      fetch_archived_seq(server_id, id)
    end
  end

  @spec fetch_archived_seq(atom(), String.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def fetch_archived_seq(server_id, id)
      when is_atom(server_id) and is_binary(id) and id != "" do
    case query_session(server_id, id) do
      {:ok, %Session{archived_seq: archived_seq}}
      when is_integer(archived_seq) and archived_seq >= 0 ->
        {:ok, archived_seq}

      {:ok, _session} ->
        {:error, :invalid_archived_seq}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_group_started(server_id, group_id)
       when is_atom(server_id) and is_integer(group_id) and group_id >= 0 do
    if Process.whereis(server_id) != nil do
      :ok
    else
      ensure_group_started_slow(group_id)
    end
  end

  defp ensure_group_started_slow(group_id) do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, {:shutdown, {:failed_to_start_child, _child, {:already_started, _pid}}}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
