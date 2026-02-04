defmodule FleetLMWeb.ConversationController do
  @moduledoc """
  HTTP controller for the conversation (message substrate) API.

  This is a stateless message backend - it stores and streams messages
  but does NOT manage prompt windows, token budgets, or compaction.
  """
  use FleetLMWeb, :controller

  alias FleetLM.Runtime

  action_fallback FleetLMWeb.FallbackController

  # ---------------------------------------------------------------------------
  # Conversation lifecycle
  # ---------------------------------------------------------------------------

  @doc """
  Create or update a conversation.

  PUT /v1/conversations/:id
  Body: { "metadata": { ... } }

  Returns the conversation record.
  """
  def upsert(conn, %{"id" => id} = params) do
    metadata = params["metadata"] || %{}

    unless is_map(metadata) and not is_list(metadata) do
      {:error, :invalid_metadata}
    else
      with {:ok, conversation} <- Runtime.upsert_conversation(id, metadata: metadata) do
        json(conn, conversation)
      end
    end
  end

  @doc """
  Get a conversation's metadata.

  GET /v1/conversations/:id

  Returns the conversation record including id, version, tombstoned, metadata, timestamps.
  """
  def show(conn, %{"id" => id}) do
    with {:ok, conversation} <- Runtime.get_conversation(id) do
      json(conn, FleetLM.Conversation.to_map(conversation))
    end
  end

  @doc """
  Tombstone a conversation (soft delete).

  DELETE /v1/conversations/:id

  Tombstoned conversations reject new writes but remain readable.
  Returns 204 No Content on success.
  """
  def delete(conn, %{"id" => id}) do
    with {:ok, _} <- Runtime.tombstone_conversation(id) do
      send_resp(conn, :no_content, "")
    end
  end

  # ---------------------------------------------------------------------------
  # Messaging
  # ---------------------------------------------------------------------------

  @doc """
  Append a message to a conversation.

  POST /v1/conversations/:id/messages
  Body: {
    "message": {
      "role": "user",
      "parts": [{ "type": "text", "text": "Hello" }],
      "metadata": { ... },
      "token_count": 5
    },
    "if_version": 41
  }

  Returns: { "seq": 42, "version": 42, "token_count": 5 }

  The if_version parameter provides optimistic concurrency control.
  Returns 409 Conflict if version doesn't match.
  Returns 410 Gone if conversation is tombstoned.
  """
  def append(conn, %{"id" => id, "message" => msg} = params) do
    with {:ok, validated} <- validate_message(msg),
         {:ok, [reply]} <-
           Runtime.append_messages(
             id,
             [validated],
             if_version: params["if_version"]
           ) do
      FleetLM.Observability.Telemetry.message_appended(
        validated.token_source,
        validated.token_count,
        id,
        validated.role
      )

      conn
      |> put_status(:created)
      |> json(%{seq: reply.seq, version: reply.version, token_count: reply.token_count})
    end
  end

  def append(_conn, _params), do: {:error, :invalid_message}

  @doc """
  Retrieve messages from the tail (newest first) with pagination.

  GET /v1/conversations/:id/tail?offset=0&limit=100

  Returns messages in chronological order (oldest to newest within the page).
  Use offset to page backwards through history.
  """
  def tail(conn, %{"id" => id} = params) do
    offset = parse_non_neg_integer(params, "offset", 0)
    limit = parse_pos_integer(params, "limit", 100)

    with {:ok, messages} <- Runtime.get_messages_tail(id, offset, limit) do
      json(conn, %{messages: messages})
    end
  end

  @doc """
  Replay messages by sequence number range.

  GET /v1/conversations/:id/messages?from=0&limit=100

  Returns messages starting from the given sequence number, ordered by seq.
  Use this for replaying from a known position (e.g., after a gap event).
  """
  def replay(conn, %{"id" => id} = params) do
    from_seq = parse_non_neg_integer(params, "from", 0)
    limit = parse_pos_integer(params, "limit", 100)

    with {:ok, messages} <- Runtime.get_messages_replay(id, from_seq, limit) do
      json(conn, %{messages: messages})
    end
  end

  # ---------------------------------------------------------------------------
  # Validation
  # ---------------------------------------------------------------------------

  defp validate_message(%{"role" => role, "parts" => parts} = msg)
       when is_binary(role) and byte_size(role) > 0 and is_list(parts) do
    with :ok <- validate_parts(parts) do
      metadata = Map.get(msg, "metadata", %{})

      unless is_map(metadata) and not is_list(metadata) do
        {:error, :invalid_metadata}
      else
        {token_count, token_source} = extract_token_count(msg)

        {:ok,
         %{
           role: role,
           parts: parts,
           metadata: metadata,
           token_count: token_count,
           token_source: token_source
         }}
      end
    end
  end

  defp validate_message(_), do: {:error, :invalid_message}

  defp validate_parts([]), do: {:error, :invalid_message}

  defp validate_parts(parts) do
    if Enum.all?(parts, &valid_part?/1) do
      :ok
    else
      {:error, :invalid_message}
    end
  end

  defp valid_part?(%{"type" => type}) when is_binary(type), do: true
  defp valid_part?(_), do: false

  defp extract_token_count(%{"token_count" => count})
       when is_integer(count) and count >= 0 do
    {count, :client}
  end

  defp extract_token_count(%{"parts" => parts}) do
    # Fallback: estimate tokens from text content
    count =
      parts
      |> Enum.map(fn
        %{"text" => text} when is_binary(text) -> div(byte_size(text), 4)
        _ -> 0
      end)
      |> Enum.sum()

    {count, :server}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp parse_non_neg_integer(params, key, default) do
    case Map.get(params, key) do
      nil -> default
      val when is_integer(val) and val >= 0 -> val
      val when is_binary(val) -> max(String.to_integer(val), 0)
      _ -> default
    end
  end

  defp parse_pos_integer(params, key, default) do
    case Map.get(params, key) do
      nil -> default
      val when is_integer(val) and val > 0 -> val
      val when is_binary(val) -> max(String.to_integer(val), 1)
      _ -> default
    end
  end
end
