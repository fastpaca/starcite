defmodule FleetLM.Runtime.RaftFSM do
  @moduledoc """
  Minimal Raft state machine for FleetLM message substrate.

  Stores conversations as append-only message logs. No compaction, no token budgets.
  """

  @behaviour :ra_machine

  alias FleetLM.Conversation

  @num_lanes 16

  defmodule Lane do
    @moduledoc false
    defstruct conversations: %{}
  end

  defstruct [:group_id, :lanes]

  @impl true
  def init(%{group_id: group_id}) do
    lanes = for lane <- 0..(@num_lanes - 1), into: %{}, do: {lane, %Lane{}}
    %__MODULE__{group_id: group_id, lanes: lanes}
  end

  @impl true
  def apply(
        _meta,
        {:upsert_conversation, lane_id, conversation_id, status, metadata},
        state
      ) do
    lane = Map.fetch!(state.lanes, lane_id)

    conversation =
      case Map.get(lane.conversations, conversation_id) do
        nil ->
          Conversation.new(conversation_id, status: status, metadata: metadata)

        %Conversation{} = existing ->
          Conversation.update(existing, status: status, metadata: metadata)
      end

    new_lane = %{lane | conversations: Map.put(lane.conversations, conversation_id, conversation)}
    new_state = put_in(state.lanes[lane_id], new_lane)

    reply = Conversation.to_map(conversation)

    {new_state, {:reply, {:ok, reply}}}
  end

  @impl true
  def apply(_meta, {:append_batch, lane_id, conversation_id, inbound_messages, opts}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, conversation} <- fetch_conversation(lane, conversation_id),
         :ok <- ensure_active(conversation),
         :ok <- guard_version(conversation, opts[:if_version]) do
      {updated_conversation, appended} = Conversation.append(conversation, inbound_messages)

      new_lane = %{
        lane
        | conversations: Map.put(lane.conversations, conversation_id, updated_conversation)
      }

      new_state = put_in(state.lanes[lane_id], new_lane)

      reply =
        Enum.map(appended, fn %{seq: seq, token_count: tokens} ->
          %{
            conversation_id: conversation_id,
            seq: seq,
            version: updated_conversation.version,
            token_count: tokens
          }
        end)

      effects = build_effects(conversation_id, appended, updated_conversation)

      {new_state, {:reply, {:ok, reply}}, effects}
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  @impl true
  def apply(_meta, :force_snapshot, state) do
    {state, {:reply, :ok}}
  end

  @impl true
  def apply(_meta, {:ack_archived, lane_id, conversation_id, upto_seq}, state) do
    lane = Map.fetch!(state.lanes, lane_id)

    with {:ok, conversation} <- fetch_conversation(lane, conversation_id) do
      {updated_conversation, trimmed} = Conversation.persist_ack(conversation, upto_seq)

      new_lane = %{
        lane
        | conversations: Map.put(lane.conversations, conversation_id, updated_conversation)
      }

      new_state = put_in(state.lanes[lane_id], new_lane)

      # Emit telemetry about archival lag and trim
      tail_size = length(updated_conversation.message_log.entries)

      FleetLM.Observability.Telemetry.archive_ack_applied(
        conversation_id,
        updated_conversation.last_seq,
        updated_conversation.archived_seq,
        trimmed,
        updated_conversation.retention.tail_keep,
        tail_size,
        0
      )

      {new_state,
       {:reply, {:ok, %{archived_seq: updated_conversation.archived_seq, trimmed: trimmed}}}}
    else
      {:error, reason} -> {state, {:reply, {:error, reason}}}
    end
  end

  # ---------------------------------------------------------------------------
  # Queries
  # ---------------------------------------------------------------------------

  @doc """
  Query messages from the tail (newest) with offset-based pagination.
  """
  def query_messages_tail(state, lane_id, conversation_id, offset, limit) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, conversation} <- fetch_conversation(lane, conversation_id) do
      Conversation.messages_tail(conversation, offset, limit)
    else
      _ -> []
    end
  end

  @doc """
  Query messages by sequence range for replay.

  Returns messages where `seq >= from_seq`, ordered oldest to newest.
  """
  def query_messages_replay(state, lane_id, conversation_id, from_seq, limit) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, conversation} <- fetch_conversation(lane, conversation_id) do
      Conversation.messages_replay(conversation, from_seq, limit)
    else
      _ -> []
    end
  end

  @doc """
  Query unarchived messages (seq > archived_seq) up to `limit`.
  """
  def query_unarchived(state, lane_id, conversation_id, limit) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, %Conversation{} = conversation} <- fetch_conversation(lane, conversation_id) do
      conversation.message_log
      |> Map.get(:entries, [])
      # entries are newest-first; take while strictly newer than archive boundary
      |> Enum.take_while(fn %{seq: seq} -> seq > conversation.archived_seq end)
      |> Enum.reverse()
      |> Enum.take(limit)
    else
      _ -> []
    end
  end

  @doc """
  Query a conversation by ID.
  """
  def query_conversation(state, lane_id, conversation_id) do
    with {:ok, lane} <- Map.fetch(state.lanes, lane_id),
         {:ok, conversation} <- fetch_conversation(lane, conversation_id) do
      {:ok, conversation}
    else
      _ -> {:error, :conversation_not_found}
    end
  end

  def query_conversations(state, lane_id) do
    case Map.fetch(state.lanes, lane_id) do
      {:ok, %Lane{conversations: conversations}} -> conversations
      _ -> %{}
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp fetch_conversation(%Lane{conversations: conversations}, conversation_id) do
    case Map.get(conversations, conversation_id) do
      nil -> {:error, :conversation_not_found}
      conversation -> {:ok, conversation}
    end
  end

  defp ensure_active(%Conversation{status: :active}), do: :ok
  defp ensure_active(_), do: {:error, :conversation_tombstoned}

  defp guard_version(_conversation, nil), do: :ok

  defp guard_version(%Conversation{version: version}, expected) when version == expected, do: :ok

  defp guard_version(%Conversation{version: version}, _expected),
    do: {:error, {:version_conflict, version}}

  defp build_effects(conversation_id, messages, conversation) do
    message_events =
      Enum.map(messages, fn message ->
        {
          :mod_call,
          Phoenix.PubSub,
          :broadcast,
          [
            FleetLM.PubSub,
            "conversation:#{conversation_id}",
            {:message, message_payload(message, conversation)}
          ]
        }
      end)

    archive_event =
      {
        :mod_call,
        FleetLM.Archive,
        :append_messages,
        [conversation_id, messages]
      }

    message_events ++ [archive_event]
  end

  defp message_payload(message, %Conversation{} = conversation) do
    Map.put(message, :version, conversation.version)
  end
end
