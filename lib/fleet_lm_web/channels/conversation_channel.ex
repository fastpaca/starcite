defmodule FleetLMWeb.ConversationChannel do
  @moduledoc """
  WebSocket channel for conversation updates.

  Clients join `conversation:CONVERSATION_ID` to receive real-time updates.

  ## Events (from server to client)

  1. "message" - New message appended
     %{
       type: "message",
       seq: 42,
       version: 42,
       message: %{role: "user", parts: [...]}
     }

  2. "tombstone" - Conversation tombstoned (no new writes allowed)
     %{type: "tombstone"}

  3. "gap" - Client missed messages, must replay by seq
     %{type: "gap", expected: 120, actual: 124}

  That's it! No compaction events - this is a message substrate.
  """

  use Phoenix.Channel

  @impl true
  def join("conversation:" <> conversation_id, _params, socket) do
    # Subscribe to PubSub for this conversation
    Phoenix.PubSub.subscribe(FleetLM.PubSub, "conversation:#{conversation_id}")

    {:ok, assign(socket, :conversation_id, conversation_id)}
  end

  @impl true
  def handle_info({:message, payload}, socket) do
    push(socket, "message", %{
      type: "message",
      seq: payload.seq,
      version: payload.version,
      message: %{
        role: payload.role,
        parts: payload.parts,
        metadata: Map.get(payload, :metadata, %{}),
        token_count: Map.get(payload, :token_count)
      }
    })

    {:noreply, socket}
  end

  @impl true
  def handle_info({:tombstone, _payload}, socket) do
    push(socket, "tombstone", %{type: "tombstone"})
    {:noreply, socket}
  end

  @impl true
  def handle_info({:gap, payload}, socket) do
    push(socket, "gap", %{
      type: "gap",
      expected: payload.expected,
      actual: payload.actual
    })

    {:noreply, socket}
  end
end
