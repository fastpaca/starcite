defmodule FleetLM.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FleetLM.Runtime
  alias FleetLM.Runtime.RaftManager

  setup do
    # Clean up any existing Raft groups
    for group_id <- 0..(RaftManager.num_groups() - 1) do
      server_id = RaftManager.server_id(group_id)
      full_server_id = {server_id, Node.self()}

      case Process.whereis(server_id) do
        nil ->
          try do
            :ra.force_delete_server(:default, full_server_id)
          catch
            _, _ -> :ok
          end

        pid ->
          ref = Process.monitor(pid)
          Process.exit(pid, :kill)

          receive do
            {:DOWN, ^ref, :process, ^pid, _} -> :ok
          after
            1000 -> :ok
          end

          try do
            :ra.force_delete_server(:default, full_server_id)
          catch
            _, _ -> :ok
          end
      end
    end

    # Remove residual Raft data on disk
    FleetLM.Runtime.TestHelper.reset()

    # Give Ra time to fully clean up
    Process.sleep(100)

    :ok
  end

  # Helper to generate unique conversation IDs
  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{:rand.uniform(999_999_999)}"
  end

  describe "Runtime.upsert_conversation/2" do
    test "creates a new conversation via Raft" do
      {:ok, result} = Runtime.upsert_conversation("conv-1")

      assert result.id == "conv-1"
      assert result.version == 0
      assert result.last_seq == 0
      assert result.tombstoned == false
    end

    test "creates with metadata" do
      {:ok, result} = Runtime.upsert_conversation("conv-2", metadata: %{user_id: "u_123"})

      assert result.id == "conv-2"
      assert result.metadata == %{user_id: "u_123"}
    end

    test "updates an existing conversation" do
      {:ok, _} = Runtime.upsert_conversation("conv-3", metadata: %{foo: "bar"})
      {:ok, updated} = Runtime.upsert_conversation("conv-3", metadata: %{foo: "baz"})

      assert updated.metadata == %{foo: "baz"}
    end
  end

  describe "Runtime.get_conversation/1" do
    test "retrieves conversation from Raft" do
      {:ok, _} = Runtime.upsert_conversation("conv-4")

      messages = [
        %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
      ]

      {:ok, _} = Runtime.append_messages("conv-4", messages)
      {:ok, conv} = Runtime.get_conversation("conv-4")

      assert conv.id == "conv-4"
      assert conv.last_seq == 1
      assert conv.version > 0
    end

    test "returns error for non-existent conversation" do
      assert {:error, _} = Runtime.get_conversation("nonexistent")
    end
  end

  describe "Runtime.tombstone_conversation/1" do
    test "tombstones a conversation" do
      {:ok, _} = Runtime.upsert_conversation("conv-tomb")
      {:ok, result} = Runtime.tombstone_conversation("conv-tomb")

      assert result.tombstoned == true
    end

    test "tombstoned conversation rejects writes" do
      {:ok, _} = Runtime.upsert_conversation("conv-tomb-write")
      {:ok, _} = Runtime.tombstone_conversation("conv-tomb-write")

      messages = [%{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}]
      result = Runtime.append_messages("conv-tomb-write", messages)

      assert {:error, :conversation_tombstoned} = result
    end
  end

  describe "Runtime.append_messages/3" do
    test "appends messages to a conversation" do
      {:ok, _} = Runtime.upsert_conversation("conv-append")

      messages = [
        %{role: "user", parts: [%{type: "text", text: "Hello"}], metadata: %{}, token_count: 10},
        %{role: "assistant", parts: [%{type: "text", text: "Hi"}], metadata: %{}, token_count: 10}
      ]

      {:ok, results} = Runtime.append_messages("conv-append", messages)

      assert length(results) == 2
      assert hd(results).seq == 1
      assert hd(results).conversation_id == "conv-append"
    end

    test "sequences are monotonically increasing" do
      {:ok, _} = Runtime.upsert_conversation("conv-seq")

      {:ok, r1} =
        Runtime.append_messages("conv-seq", [
          %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
        ])

      {:ok, r2} =
        Runtime.append_messages("conv-seq", [
          %{role: "assistant", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
        ])

      assert hd(r1).seq == 1
      assert hd(r2).seq == 2
    end

    test "version guard rejects stale writes (409 Conflict)" do
      {:ok, _} = Runtime.upsert_conversation("conv-version")

      {:ok, _} =
        Runtime.append_messages("conv-version", [
          %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
        ])

      # Try to append with wrong version
      result =
        Runtime.append_messages(
          "conv-version",
          [%{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}],
          if_version: 0
        )

      assert {:error, {:version_conflict, _current}} = result
    end
  end

  describe "Runtime.get_messages_tail/3" do
    test "retrieves last N messages with default offset" do
      id = unique_id("tail-last-n")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      {:ok, result} = Runtime.get_messages_tail(id, 0, 3)

      assert length(result) == 3
      assert Enum.map(result, & &1.seq) == [8, 9, 10]
    end

    test "retrieves messages with offset from tail" do
      id = unique_id("tail-offset")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      # Skip last 3, get next 4 (should be seq 4, 5, 6, 7)
      {:ok, result} = Runtime.get_messages_tail(id, 3, 4)

      assert length(result) == 4
      assert Enum.map(result, & &1.seq) == [4, 5, 6, 7]
    end

    test "pagination through entire message history" do
      id = unique_id("tail-pages")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      {:ok, page1} = Runtime.get_messages_tail(id, 0, 3)
      assert Enum.map(page1, & &1.seq) == [8, 9, 10]

      {:ok, page2} = Runtime.get_messages_tail(id, 3, 3)
      assert Enum.map(page2, & &1.seq) == [5, 6, 7]

      {:ok, page3} = Runtime.get_messages_tail(id, 6, 3)
      assert Enum.map(page3, & &1.seq) == [2, 3, 4]

      {:ok, page4} = Runtime.get_messages_tail(id, 9, 3)
      assert Enum.map(page4, & &1.seq) == [1]
    end

    test "returns empty list when offset exceeds count" do
      id = unique_id("tail-empty")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..5,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      {:ok, result} = Runtime.get_messages_tail(id, 100, 10)

      assert result == []
    end
  end

  describe "Runtime.get_messages_replay/3" do
    test "replays messages from sequence number" do
      id = unique_id("replay")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      # Replay from seq 5
      {:ok, result} = Runtime.get_messages_replay(id, 5, 100)

      assert length(result) == 6
      assert Enum.map(result, & &1.seq) == [5, 6, 7, 8, 9, 10]
    end

    test "respects limit" do
      id = unique_id("replay-limit")
      {:ok, _} = Runtime.upsert_conversation(id)

      messages =
        for _ <- 1..10,
            do: %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}

      {:ok, _} = Runtime.append_messages(id, messages)

      {:ok, result} = Runtime.get_messages_replay(id, 1, 3)

      assert length(result) == 3
      assert Enum.map(result, & &1.seq) == [1, 2, 3]
    end
  end

  describe "Raft failover and recovery" do
    test "recovers state after server crash and restart" do
      capture_log(fn ->
        {:ok, _} = Runtime.upsert_conversation("conv-failover")

        {:ok, r1} =
          Runtime.append_messages("conv-failover", [
            %{
              role: "user",
              parts: [%{type: "text", text: "before crash"}],
              metadata: %{},
              token_count: 10
            }
          ])

        assert hd(r1).seq == 1

        # Kill the Raft group
        group_id = RaftManager.group_for_conversation("conv-failover")
        server_id = RaftManager.server_id(group_id)
        pid = Process.whereis(server_id)

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          2000 -> flunk("Raft process did not die")
        end

        # Restart the group
        start_result = RaftManager.start_group(group_id)
        assert start_result in [:ok, {:error, :cluster_not_formed}]

        eventually(
          fn ->
            assert Process.whereis(server_id)
          end,
          timeout: 3_000
        )

        # Append after crash
        {:ok, r2} =
          Runtime.append_messages("conv-failover", [
            %{
              role: "user",
              parts: [%{type: "text", text: "after crash"}],
              metadata: %{},
              token_count: 10
            }
          ])

        assert hd(r2).seq >= 2

        # Verify both messages exist
        {:ok, messages} = Runtime.get_messages_tail("conv-failover", 0, 10)
        texts = Enum.map(messages, &(&1.parts |> hd() |> Map.get(:text)))

        assert "before crash" in texts
        assert "after crash" in texts
      end)
    end
  end

  describe "Concurrent access" do
    test "multiple conversations can be appended concurrently" do
      # Create 10 conversations
      for i <- 1..10 do
        {:ok, _} = Runtime.upsert_conversation("conv-concurrent-#{i}")
      end

      # Append to all concurrently
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            Runtime.append_messages("conv-concurrent-#{i}", [
              %{role: "user", parts: [%{type: "text"}], metadata: %{}, token_count: 10}
            ])
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  # Helper to wait for eventually condition
  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          fun.()
        end
    end
  end
end
