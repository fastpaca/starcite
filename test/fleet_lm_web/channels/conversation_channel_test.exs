defmodule FleetLMWeb.ConversationChannelTest do
  @moduledoc """
  WebSocket stream tests for the conversation channel.

  Tests the event types per spec:
  - "message" - New message appended (broadcast via Raft effects)
  - "tombstone" - Conversation tombstoned (client-side detection via API)
  - "gap" - Client missed messages, must replay

  Note: Tombstone events are NOT automatically broadcast via PubSub.
  Clients detect tombstones by checking API responses or handling 410 errors.
  """
  use ExUnit.Case, async: false

  alias FleetLM.Runtime
  alias Phoenix.PubSub

  @pubsub FleetLM.PubSub

  setup do
    FleetLM.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  describe "message event" do
    test "receives message event when message is appended" do
      id = unique_id("stream-msg")
      {:ok, _} = Runtime.upsert_conversation(id)

      # Subscribe to the conversation topic
      PubSub.subscribe(@pubsub, "conversation:#{id}")

      # Append a message
      {:ok, [result]} =
        Runtime.append_messages(id, [
          %{
            role: "user",
            parts: [%{type: "text", text: "Hello stream"}],
            metadata: %{source: "test"},
            token_count: 10
          }
        ])

      # Should receive the message event
      assert_receive {:message, payload}, 1000

      assert payload.seq == result.seq
      assert payload.version == result.version
      assert payload.role == "user"
      assert payload.parts == [%{type: "text", text: "Hello stream"}]
      assert payload.metadata == %{source: "test"}
      assert payload.token_count == 10
    end

    test "receives multiple message events for batch append" do
      id = unique_id("stream-batch")
      {:ok, _} = Runtime.upsert_conversation(id)

      PubSub.subscribe(@pubsub, "conversation:#{id}")

      # Append multiple messages
      {:ok, results} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "first"}], metadata: %{}, token_count: 5},
          %{
            role: "assistant",
            parts: [%{type: "text", text: "second"}],
            metadata: %{},
            token_count: 6
          }
        ])

      assert length(results) == 2

      # Should receive both message events
      assert_receive {:message, msg1}, 1000
      assert_receive {:message, msg2}, 1000

      seqs = Enum.sort([msg1.seq, msg2.seq])
      assert seqs == [1, 2]
    end

    test "message event includes all fields from spec" do
      id = unique_id("stream-fields")
      {:ok, _} = Runtime.upsert_conversation(id)

      PubSub.subscribe(@pubsub, "conversation:#{id}")

      {:ok, _} =
        Runtime.append_messages(id, [
          %{
            role: "assistant",
            parts: [%{type: "text", text: "response"}, %{type: "tool_call", id: "tc_1"}],
            metadata: %{model: "claude-3"},
            token_count: 42
          }
        ])

      assert_receive {:message, payload}, 1000

      # Verify all expected fields
      assert is_integer(payload.seq)
      assert is_integer(payload.version)
      assert payload.role == "assistant"
      assert length(payload.parts) == 2
      assert payload.metadata == %{model: "claude-3"}
      assert payload.token_count == 42
    end
  end

  describe "tombstone behavior" do
    test "tombstoned conversation rejects appends (client detects via API)" do
      id = unique_id("stream-tomb")
      {:ok, _} = Runtime.upsert_conversation(id)
      {:ok, _} = Runtime.tombstone_conversation(id)

      # Verify the conversation is tombstoned
      {:ok, conv} = Runtime.get_conversation(id)
      assert conv.status == :tombstoned

      # Append attempt should fail
      result =
        Runtime.append_messages(id, [
          %{
            role: "user",
            parts: [%{type: "text", text: "after tomb"}],
            metadata: %{},
            token_count: 5
          }
        ])

      assert {:error, :conversation_tombstoned} = result
    end

    test "tombstoned conversation is still readable" do
      id = unique_id("stream-tomb-read")
      {:ok, _} = Runtime.upsert_conversation(id)

      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "msg"}], metadata: %{}, token_count: 3}
        ])

      {:ok, _} = Runtime.tombstone_conversation(id)

      # Messages are still readable
      {:ok, messages} = Runtime.get_messages_tail(id, 0, 100)
      assert length(messages) == 1
    end
  end

  describe "gap event" do
    test "gap event has expected and actual fields" do
      id = unique_id("stream-gap")

      PubSub.subscribe(@pubsub, "conversation:#{id}")

      # Simulate a gap event broadcast (this would normally come from the runtime
      # when detecting a client has missed messages)
      PubSub.broadcast(@pubsub, "conversation:#{id}", {:gap, %{expected: 10, actual: 15}})

      assert_receive {:gap, payload}, 1000
      assert payload.expected == 10
      assert payload.actual == 15
    end
  end

  describe "channel join and subscription" do
    test "can subscribe to conversation topic before it exists" do
      id = unique_id("stream-early-sub")

      # Subscribe before conversation exists
      PubSub.subscribe(@pubsub, "conversation:#{id}")

      # Create conversation and append
      {:ok, _} = Runtime.upsert_conversation(id)

      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "early"}], metadata: %{}, token_count: 5}
        ])

      # Should still receive the message
      assert_receive {:message, payload}, 1000
      assert payload.role == "user"
    end

    test "unsubscribing stops receiving events" do
      id = unique_id("stream-unsub")
      {:ok, _} = Runtime.upsert_conversation(id)

      PubSub.subscribe(@pubsub, "conversation:#{id}")

      # Append first message
      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "first"}], metadata: %{}, token_count: 5}
        ])

      assert_receive {:message, _}, 1000

      # Unsubscribe
      PubSub.unsubscribe(@pubsub, "conversation:#{id}")

      # Append second message
      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "second"}], metadata: %{}, token_count: 6}
        ])

      # Should not receive this one
      refute_receive {:message, _}, 200
    end
  end
end
