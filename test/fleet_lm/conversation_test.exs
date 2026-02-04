defmodule FleetLM.ConversationTest do
  use ExUnit.Case, async: true

  alias FleetLM.Conversation
  alias FleetLM.Conversation.MessageLog

  describe "Conversation.new/2" do
    test "creates a new conversation with default values" do
      conv = Conversation.new("conv-1")

      assert conv.id == "conv-1"
      assert conv.status == :active
      assert conv.last_seq == 0
      assert conv.version == 0
      assert MessageLog.entries(conv.message_log) == []
    end

    test "creates with custom status and metadata" do
      conv = Conversation.new("conv-2", status: :tombstoned, metadata: %{foo: "bar"})

      assert conv.status == :tombstoned
      assert conv.metadata == %{foo: "bar"}
    end
  end

  describe "Conversation.append/2" do
    test "appends messages and increments sequence" do
      conv = Conversation.new("conv-1")

      messages = [
        %{role: "user", parts: [%{type: "text", text: "Hello"}], metadata: %{}, token_count: 10},
        %{
          role: "assistant",
          parts: [%{type: "text", text: "Hi there"}],
          metadata: %{},
          token_count: 15
        }
      ]

      {new_conv, appended} = Conversation.append(conv, messages)

      assert new_conv.last_seq == 2
      assert new_conv.version == 1
      assert length(appended) == 2

      [msg1, msg2] = appended
      assert msg1.seq == 1
      assert msg1.role == "user"
      assert msg1.token_count == 10

      assert msg2.seq == 2
      assert msg2.role == "assistant"
      assert msg2.token_count == 15
    end

    test "maintains monotonically increasing sequence" do
      conv = Conversation.new("conv-1")

      {conv, _} =
        Conversation.append(conv, [
          %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
        ])

      {conv, appended} =
        Conversation.append(conv, [
          %{role: "assistant", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
        ])

      assert hd(appended).seq == 2
      assert conv.last_seq == 2
    end
  end

  describe "Conversation.messages_tail/3" do
    test "returns last N messages when offset is 0" do
      conv = Conversation.new("conv-1")

      # Add 10 messages (seq 1..10)
      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      # Get last 3 messages (should be seq 8, 9, 10)
      result = Conversation.messages_tail(conv, 0, 3)

      assert length(result) == 3
      assert Enum.map(result, & &1.seq) == [8, 9, 10]
    end

    test "returns messages with offset from tail" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      # Skip last 3 messages, get next 3 (should be seq 5, 6, 7)
      result = Conversation.messages_tail(conv, 3, 3)

      assert length(result) == 3
      assert Enum.map(result, & &1.seq) == [5, 6, 7]
    end

    test "returns empty list when offset exceeds message count" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..5,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      result = Conversation.messages_tail(conv, 10, 5)

      assert result == []
    end

    test "returns messages in chronological order (oldest to newest)" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..5,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      result = Conversation.messages_tail(conv, 0, 5)

      seqs = Enum.map(result, & &1.seq)
      assert seqs == Enum.sort(seqs)
    end
  end

  describe "Conversation.messages_replay/3" do
    test "returns messages starting from sequence number" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      # Replay from seq 5
      result = Conversation.messages_replay(conv, 5, 10)

      assert length(result) == 6
      assert Enum.map(result, & &1.seq) == [5, 6, 7, 8, 9, 10]
    end

    test "respects limit" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      result = Conversation.messages_replay(conv, 1, 3)

      assert length(result) == 3
      assert Enum.map(result, & &1.seq) == [1, 2, 3]
    end

    test "returns empty when from_seq is beyond last_seq" do
      conv = Conversation.new("conv-1")

      messages =
        for _ <- 1..5,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {conv, _} = Conversation.append(conv, messages)

      result = Conversation.messages_replay(conv, 100, 10)

      assert result == []
    end
  end

  describe "Conversation.tombstone/1" do
    test "marks conversation as tombstoned" do
      conv = Conversation.new("conv-1")
      tombstoned = Conversation.tombstone(conv)

      assert tombstoned.status == :tombstoned
      assert NaiveDateTime.compare(tombstoned.updated_at, conv.updated_at) == :gt
    end
  end

  describe "Conversation.persist_ack/2 (archival trim)" do
    test "trims acknowledged range while retaining bounded tail" do
      # Set a small tail_keep for predictable trimming
      conv = Conversation.new("conv-arch", tail_keep: 2)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 1}

      {conv, _} = Conversation.append(conv, messages)

      # Acknowledge archival up to seq 5
      {conv, trimmed} = Conversation.persist_ack(conv, 5)

      assert conv.archived_seq == 5
      # Should keep seq 6..10 and retain up to 2 below boundary (5 and 4)
      kept_seqs = conv.message_log |> MessageLog.entries() |> Enum.map(& &1.seq)
      assert Enum.sort(kept_seqs) == Enum.sort([4, 5, 6, 7, 8, 9, 10])
      assert trimmed == 3
    end
  end

  describe "Conversation.to_map/1" do
    test "converts to API-friendly map" do
      conv = Conversation.new("conv-1", metadata: %{user_id: "u_123"})

      messages = [%{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}]
      {conv, _} = Conversation.append(conv, messages)

      result = Conversation.to_map(conv)

      assert result.id == "conv-1"
      assert result.version == 1
      assert result.tombstoned == false
      assert result.metadata == %{user_id: "u_123"}
      assert result.last_seq == 1
      assert is_binary(result.created_at)
      assert is_binary(result.updated_at)
    end
  end
end
