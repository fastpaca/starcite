defmodule Fastpaca.Runtime do
  @moduledoc """
  Public interface for the Raft-backed runtime.

  This is a message backend substrate - it stores and streams messages
  but does NOT manage prompt windows, token budgets, or compaction.
  """

  require Logger

  alias Fastpaca.Conversation
  alias Fastpaca.Runtime.{RaftFSM, RaftManager, RaftTopology}

  @timeout Application.compile_env(:fastpaca, :raft_command_timeout_ms, 2_000)
  @rpc_timeout Application.compile_env(:fastpaca, :rpc_timeout_ms, 5_000)

  # ---------------------------------------------------------------------------
  # Conversation lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Create or update a conversation.

  Options:
    - `metadata`: Optional map of conversation metadata
    - `status`: Optional status (:active or :tombstoned)

  Returns `{:ok, conversation}` on success.
  """
  @spec upsert_conversation(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def upsert_conversation(id, opts \\ []) when is_binary(id) do
    group = RaftManager.group_for_context(id)

    call_on_replica(group, :upsert_conversation_local, [id, opts], fn ->
      upsert_conversation_local(id, opts)
    end)
  end

  @doc false
  def upsert_conversation_local(id, opts \\ []) when is_binary(id) do
    status = Keyword.get(opts, :status, :active)
    metadata = Keyword.get(opts, :metadata, %{})

    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group),
         {:ok, {:reply, {:ok, data}}, _leader} <-
           :ra.process_command(
             {server_id, Node.self()},
             {:upsert_conversation, lane, id, status, metadata},
             @timeout
           ) do
      {:ok, data}
    else
      {:timeout, leader} -> {:timeout, leader}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Get a conversation by ID.

  Returns `{:ok, conversation}` on success, `{:error, :conversation_not_found}` if not found.
  """
  @spec get_conversation(String.t()) :: {:ok, Conversation.t()} | {:error, term()}
  def get_conversation(id) when is_binary(id) do
    group = RaftManager.group_for_context(id)

    call_on_replica(group, :get_conversation_local, [id], fn ->
      get_conversation_local(id)
    end)
  end

  @doc false
  def get_conversation_local(id) when is_binary(id) do
    with {:ok, server_id, lane, _group} <- locate(id) do
      case :ra.consistent_query(server_id, fn state ->
             RaftFSM.query_conversation(state, lane, id)
           end) do
        {:ok, {:ok, conversation}, _leader} ->
          {:ok, conversation}

        {:ok, {:error, reason}, _leader} ->
          {:error, reason}

        {:ok, {{_term, _index}, {:ok, conversation}}, _leader} ->
          {:ok, conversation}

        {:ok, {{_term, _index}, {:error, reason}}, _leader} ->
          {:error, reason}

        {:timeout, leader} ->
          {:error, {:timeout, leader}}

        other ->
          other
      end
    end
  end

  @doc """
  Tombstone a conversation (soft delete).

  Tombstoned conversations reject new writes but remain readable.
  Returns `{:ok, conversation}` on success.
  """
  @spec tombstone_conversation(String.t()) :: {:ok, map()} | {:error, term()}
  def tombstone_conversation(id) when is_binary(id) do
    with {:ok, conversation} <- get_conversation(id) do
      upsert_conversation(id, status: :tombstoned, metadata: conversation.metadata)
    end
  end

  # ---------------------------------------------------------------------------
  # Messaging
  # ---------------------------------------------------------------------------

  @doc """
  Append messages to a conversation.

  Each message is a map with:
    - `role`: required string
    - `parts`: required list of parts (each with `type`)
    - `metadata`: optional map
    - `token_count`: optional integer (client-provided)
    - `token_source`: :client or :server

  Options:
    - `if_version`: optimistic concurrency check

  Returns `{:ok, [reply]}` where each reply contains `seq`, `version`, `token_count`.
  Returns `{:error, :conversation_tombstoned}` (410) if tombstoned.
  Returns `{:error, {:version_conflict, current}}` (409) if version mismatch.
  """
  @spec append_messages(String.t(), [map()], keyword()) ::
          {:ok, [map()]} | {:error, term()} | {:timeout, term()}
  def append_messages(id, message_inputs, opts \\ []) when is_binary(id) do
    group = RaftManager.group_for_context(id)

    call_on_replica(group, :append_messages_local, [id, message_inputs, opts], fn ->
      append_messages_local(id, message_inputs, opts)
    end)
  end

  @doc false
  def append_messages_local(id, message_inputs, opts \\ [])
      when is_binary(id) do
    with {:ok, server_id, lane, group} <- locate(id),
         :ok <- ensure_group_started(group) do
      case :ra.process_command(
             {server_id, Node.self()},
             {:append_batch, lane, id, message_inputs, opts},
             @timeout
           ) do
        {:ok, {:reply, {:ok, reply}}, _leader} -> {:ok, reply}
        {:ok, {:reply, {:error, reason}}, _leader} -> {:error, reason}
        {:timeout, leader} -> {:timeout, leader}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @doc """
  Retrieves messages from the tail (newest) with offset-based pagination.

  Designed for backward iteration starting from the most recent messages.
  Future-proof for scenarios where older messages may be paged from disk/remote storage.

  ## Parameters
    - `id`: Conversation ID
    - `offset`: Number of messages to skip from tail (0 = most recent, default: 0)
    - `limit`: Maximum messages to return (must be > 0, default: 100)

  ## Examples
      # Get last 50 messages
      get_messages_tail("conv-123", 0, 50)

      # Get next page (messages 51-100 from tail)
      get_messages_tail("conv-123", 50, 50)

  Returns `{:ok, messages}` where messages are in chronological order (oldest to newest).
  """
  @spec get_messages_tail(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_messages_tail(id, offset \\ 0, limit \\ 100)
      when is_binary(id) and is_integer(offset) and offset >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftManager.group_for_context(id)

    call_on_replica(group, :get_messages_tail_local, [id, offset, limit], fn ->
      get_messages_tail_local(id, offset, limit)
    end)
  end

  @doc false
  def get_messages_tail_local(id, offset, limit)
      when is_binary(id) and is_integer(offset) and offset >= 0 and is_integer(limit) and
             limit > 0 do
    with {:ok, server_id, lane, _group} <- locate(id) do
      case :ra.consistent_query(server_id, fn state ->
             RaftFSM.query_messages_tail(state, lane, id, offset, limit)
           end) do
        {:ok, messages, _leader} when is_list(messages) ->
          {:ok, messages}

        {:ok, {{_term, _index}, messages}, _leader} when is_list(messages) ->
          {:ok, messages}

        {:timeout, leader} ->
          {:error, {:timeout, leader}}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Replay messages by sequence number range.

  Returns messages starting from `from_seq`, ordered by seq (oldest to newest).
  Use this for replaying from a known position (e.g., after a gap event in websocket).

  ## Parameters
    - `id`: Conversation ID
    - `from_seq`: Starting sequence number (inclusive)
    - `limit`: Maximum messages to return (must be > 0, default: 100)

  Returns `{:ok, messages}` in chronological order.
  """
  @spec get_messages_replay(String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  def get_messages_replay(id, from_seq \\ 0, limit \\ 100)
      when is_binary(id) and is_integer(from_seq) and from_seq >= 0 and is_integer(limit) and
             limit > 0 do
    group = RaftManager.group_for_context(id)

    call_on_replica(group, :get_messages_replay_local, [id, from_seq, limit], fn ->
      get_messages_replay_local(id, from_seq, limit)
    end)
  end

  @doc false
  def get_messages_replay_local(id, from_seq, limit)
      when is_binary(id) and is_integer(from_seq) and from_seq >= 0 and is_integer(limit) and
             limit > 0 do
    with {:ok, server_id, lane, _group} <- locate(id) do
      case :ra.consistent_query(server_id, fn state ->
             RaftFSM.query_messages_replay(state, lane, id, from_seq, limit)
           end) do
        {:ok, messages, _leader} when is_list(messages) ->
          {:ok, messages}

        {:ok, {{_term, _index}, messages}, _leader} when is_list(messages) ->
          {:ok, messages}

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
    group = RaftManager.group_for_context(id)

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
    group = RaftManager.group_for_context(id)
    lane = :erlang.phash2(id, 16)
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
          # If a timeout returns a leader hint, try the leader next
          rest = maybe_enqueue_leader(leader, rest, visited)
          try_remote(rest, fun, args, visited, [{node, {:timeout, leader}} | failures])

        {:timeout, leader} ->
          # Some paths may return bare {:timeout, leader}
          rest = maybe_enqueue_leader(leader, rest, visited)
          try_remote(rest, fun, args, visited, [{node, {:timeout, leader}} | failures])

        other ->
          other
      end
    end
  end

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

    cond do
      self_node in replicas ->
        {:local, self_node}

      true ->
        ready_candidates =
          replicas
          |> Enum.filter(&(&1 in ready_nodes))

        fallbacks = replicas -- ready_candidates

        {:remote, Enum.uniq(ready_candidates ++ fallbacks)}
    end
  end
end
