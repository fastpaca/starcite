defmodule Starcite.Runtime do
  @moduledoc """
  Public interface for the Raft-backed Starcite runtime.

  The runtime exposes three session primitives:

  - create a session
  - append an event
  - tail events from a cursor
  """

  require Logger

  alias Starcite.Runtime.{RaftFSM, RaftManager, RaftTopology}
  alias Starcite.Session

  @timeout Application.compile_env(:starcite, :raft_command_timeout_ms, 2_000)
  @rpc_timeout Application.compile_env(:starcite, :rpc_timeout_ms, 5_000)

  @default_tail_batch_size 1_000

  # ---------------------------------------------------------------------------
  # Session lifecycle
  # ---------------------------------------------------------------------------

  @spec create_session(keyword()) :: {:ok, map()} | {:error, term()}
  def create_session(opts \\ []) when is_list(opts) do
    id =
      case Keyword.get(opts, :id) do
        nil -> generate_session_id()
        value -> value
      end

    title = Keyword.get(opts, :title)
    metadata = Keyword.get(opts, :metadata, %{})
    group = RaftManager.group_for_session(id)

    call_on_replica(group, :create_session_local, [id, title, metadata], fn ->
      create_session_local(id, title, metadata)
    end)
  end

  @doc false
  def create_session_local(id, title, metadata)
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and is_map(metadata) do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:create_session, lane, id, title, metadata},
             @timeout
           ) do
        {:ok, {:reply, {:ok, data}}, _leader} -> {:ok, data}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  def create_session_local(_id, _title, _metadata), do: {:error, :invalid_session}

  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(id) when is_binary(id) and id != "" do
    group = RaftManager.group_for_session(id)

    call_on_replica(group, :get_session_local, [id], fn ->
      get_session_local(id)
    end)
  end

  def get_session(_id), do: {:error, :invalid_session_id}

  @doc false
  def get_session_local(id) when is_binary(id) and id != "" do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.consistent_query(server_id, fn state ->
             RaftFSM.query_session(state, lane, id)
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

        other ->
          other
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Append and tail
  # ---------------------------------------------------------------------------

  @spec append_event(String.t(), map(), keyword()) ::
          {:ok, %{seq: non_neg_integer(), last_seq: non_neg_integer(), deduped: boolean()}}
          | {:error, term()}
          | {:timeout, term()}
  def append_event(id, event, opts \\ [])

  def append_event(id, event, opts) when is_binary(id) and id != "" and is_map(event) do
    group = RaftManager.group_for_session(id)

    call_on_replica(group, :append_event_local, [id, event, opts], fn ->
      append_event_local(id, event, opts)
    end)
  end

  def append_event(_id, _event, _opts), do: {:error, :invalid_event}

  @doc false
  def append_event_local(id, event, opts \\ [])
      when is_binary(id) and id != "" and is_map(event) do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:append_event, lane, id, event, opts},
             @timeout
           ) do
        {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec get_events_from_cursor(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_events_from_cursor(id, cursor, limit \\ @default_tail_batch_size)

  def get_events_from_cursor(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftManager.group_for_session(id)

    call_on_replica(group, :get_events_from_cursor_local, [id, cursor, limit], fn ->
      get_events_from_cursor_local(id, cursor, limit)
    end)
  end

  def get_events_from_cursor(_id, _cursor, _limit), do: {:error, :invalid_cursor}

  @doc false
  def get_events_from_cursor_local(id, cursor, limit)
      when is_binary(id) and id != "" and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
             limit > 0 do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.consistent_query(server_id, fn state ->
             RaftFSM.query_events_from_cursor(state, lane, id, cursor, limit)
           end) do
        {:ok, {:ok, events}, _leader} when is_list(events) ->
          {:ok, events}

        {:ok, {{_term, _index}, {:ok, events}}, _leader} when is_list(events) ->
          {:ok, events}

        {:ok, {:error, reason}, _leader} ->
          {:error, reason}

        {:ok, {{_term, _index}, {:error, reason}}, _leader} ->
          {:error, reason}

        {:timeout, leader} ->
          {:error, {:timeout, leader}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Archival acknowledgements (tiered storage integration point)
  # ---------------------------------------------------------------------------

  @spec ack_archived(String.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | {:timeout, term()}
  def ack_archived(id, upto_seq) when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    group = RaftManager.group_for_session(id)

    call_on_replica(group, :ack_archived_local, [id, upto_seq], fn ->
      ack_archived_local(id, upto_seq)
    end)
  end

  @doc false
  def ack_archived_local(id, upto_seq)
      when is_binary(id) and is_integer(upto_seq) and upto_seq >= 0 do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:ack_archived, lane, id, upto_seq},
             @timeout
           ) do
        {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp locate(id) do
    group = RaftManager.group_for_session(id)
    lane = RaftManager.lane_for_session(id)
    server_id = RaftManager.server_id(group)
    {:ok, server_id, lane, group}
  end

  defp ensure_group_started(group_id) do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp call_on_replica(group_id, fun, args, local_fun) do
    case route_target(group_id) do
      {:local, _node} ->
        local_fun.()

      {:remote, []} ->
        {:error, {:no_available_replicas, []}}

      {:remote, nodes} ->
        try_remote(nodes, fun, args, MapSet.new(), [])
    end
  end

  defp try_remote([], _fun, _args, _visited, failures) do
    {:error, {:no_available_replicas, Enum.reverse(failures)}}
  end

  defp try_remote([node | rest], fun, args, visited, failures) do
    if MapSet.member?(visited, node) do
      try_remote(rest, fun, args, visited, failures)
    else
      visited = MapSet.put(visited, node)

      case safe_rpc_call(node, fun, args) do
        {:badrpc, reason} ->
          Logger.warning("Runtime RPC to #{inspect(node)} failed: #{inspect(reason)}")
          try_remote(rest, fun, args, visited, [{node, {:badrpc, reason}} | failures])

        {:error, {:not_leader, leader}} ->
          rest = maybe_enqueue_leader(leader, rest, visited)
          try_remote(rest, fun, args, visited, [{node, {:not_leader, leader}} | failures])

        {:error, :not_leader} ->
          try_remote(rest, fun, args, visited, [{node, :not_leader} | failures])

        {:error, {:timeout, leader}} ->
          rest = maybe_enqueue_leader(leader, rest, visited)
          try_remote(rest, fun, args, visited, [{node, {:timeout, leader}} | failures])

        {:timeout, leader} ->
          rest = maybe_enqueue_leader(leader, rest, visited)
          try_remote(rest, fun, args, visited, [{node, {:timeout, leader}} | failures])

        {:error, reason} ->
          if retriable_error?(reason) do
            Logger.warning("Runtime RPC to #{inspect(node)} errored: #{inspect(reason)}")
            try_remote(rest, fun, args, visited, [{node, {:error, reason}} | failures])
          else
            {:error, reason}
          end

        other ->
          other
      end
    end
  end

  defp retriable_error?(reason)
       when reason in [
              :invalid_session,
              :invalid_session_id,
              :invalid_event,
              :invalid_cursor,
              :session_not_found,
              :session_exists,
              :idempotency_conflict
            ] do
    false
  end

  defp retriable_error?({:expected_seq_conflict, _current}), do: false
  defp retriable_error?({:expected_seq_conflict, _expected, _current}), do: false
  defp retriable_error?(_reason), do: true

  defp safe_rpc_call(node, fun, args) do
    :rpc.call(node, __MODULE__, fun, args, @rpc_timeout)
  catch
    :exit, reason ->
      {:badrpc, reason}
  end

  defp maybe_enqueue_leader({server_id, leader_node}, rest, visited)
       when is_atom(server_id) and is_atom(leader_node) do
    cond do
      MapSet.member?(visited, leader_node) -> rest
      Enum.member?(rest, leader_node) -> rest
      true -> [leader_node | rest]
    end
  end

  defp maybe_enqueue_leader(_leader, rest, _visited), do: rest

  @doc false
  def route_target(group_id, opts \\ []) do
    self_node = Keyword.get(opts, :self, Node.self())
    replicas = Keyword.get(opts, :replicas, RaftManager.replicas_for_group(group_id))
    ready_nodes = Keyword.get(opts, :ready_nodes, RaftTopology.ready_nodes())
    local_running = Keyword.get(opts, :local_running, group_running?(group_id))
    remote_replicas = if local_running, do: replicas, else: replicas -- [self_node]

    cond do
      self_node in replicas and local_running ->
        {:local, self_node}

      self_node in replicas and remote_replicas == [] ->
        {:local, self_node}

      true ->
        ready_candidates =
          remote_replicas
          |> Enum.filter(&(&1 in ready_nodes))

        fallbacks =
          remote_replicas -- ready_candidates

        {:remote, Enum.uniq(ready_candidates ++ fallbacks)}
    end
  end

  defp group_running?(group_id) do
    Process.whereis(RaftManager.server_id(group_id)) != nil
  end

  defp generate_session_id do
    "ses_" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
  end
end
