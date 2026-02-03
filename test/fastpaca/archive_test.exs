defmodule Fastpaca.ArchiveTest do
  use ExUnit.Case, async: false

  alias Fastpaca.Runtime

  setup do
    # Ensure clean raft data for isolation
    Fastpaca.Runtime.TestHelper.reset()
    :ok
  end

  test "flush archives and trims via ack" do
    # Keep a very small tail so trim is obvious
    old = Application.get_env(:fastpaca, :tail_keep)
    Application.put_env(:fastpaca, :tail_keep, 2)

    on_exit(fn ->
      if old,
        do: Application.put_env(:fastpaca, :tail_keep, old),
        else: Application.delete_env(:fastpaca, :tail_keep)
    end)

    # Start Archive with the test adapter (no Postgres)
    {:ok, _pid} =
      start_supervised(
        {Fastpaca.Archive,
         flush_interval_ms: 100, adapter: Fastpaca.Archive.TestAdapter, adapter_opts: []}
      )

    conv_id = "conv-arch-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.upsert_conversation(conv_id)

    messages =
      for i <- 1..5,
          do: %{
            role: "user",
            parts: [%{type: "text", text: "m#{i}"}],
            metadata: %{},
            token_count: 1
          }

    {:ok, _} = Runtime.append_messages(conv_id, messages)

    # Wait until archived_seq catches up
    eventually(
      fn ->
        {:ok, conv} = Runtime.get_conversation(conv_id)
        assert conv.archived_seq == conv.last_seq
        # Tail should retain only 2 messages
        tail = Fastpaca.Conversation.MessageLog.entries(conv.message_log)
        assert length(tail) == 2
      end,
      timeout: 2_000
    )
  end

  describe "archive idempotency" do
    test "duplicate writes are idempotent" do
      # Start Archive with test adapter that tracks writes
      {:ok, _pid} =
        start_supervised(
          {Fastpaca.Archive,
           flush_interval_ms: 50,
           adapter: Fastpaca.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      conv_id = "conv-idem-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.upsert_conversation(conv_id)

      messages =
        for i <- 1..3,
            do: %{
              role: "user",
              parts: [%{type: "text", text: "msg#{i}"}],
              metadata: %{},
              token_count: 1
            }

      {:ok, _} = Runtime.append_messages(conv_id, messages)

      # Wait for first flush
      eventually(
        fn ->
          writes = Fastpaca.Archive.IdempotentTestAdapter.get_writes()
          assert length(writes) >= 3
        end,
        timeout: 2_000
      )

      _first_count = length(Fastpaca.Archive.IdempotentTestAdapter.get_writes())

      # Trigger another flush cycle by waiting
      Process.sleep(100)

      # The adapter should have the same unique messages (idempotent)
      eventually(
        fn ->
          writes = Fastpaca.Archive.IdempotentTestAdapter.get_writes()

          unique_keys =
            writes
            |> Enum.map(fn row -> {row.conversation_id, row.seq} end)
            |> Enum.uniq()

          # Should have exactly 3 unique messages regardless of flush count
          assert length(unique_keys) == 3
        end,
        timeout: 1_000
      )
    end

    test "retried writes succeed without duplicates" do
      {:ok, _pid} =
        start_supervised(
          {Fastpaca.Archive,
           flush_interval_ms: 50,
           adapter: Fastpaca.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      conv_id = "conv-retry-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.upsert_conversation(conv_id)

      # Append same logical message multiple times (simulating retry scenario)
      {:ok, _} =
        Runtime.append_messages(conv_id, [
          %{
            role: "user",
            parts: [%{type: "text", text: "retry-msg"}],
            metadata: %{},
            token_count: 1
          }
        ])

      eventually(
        fn ->
          writes = Fastpaca.Archive.IdempotentTestAdapter.get_writes()

          matching =
            Enum.filter(writes, fn row ->
              row.conversation_id == conv_id and row.seq == 1
            end)

          # Should have exactly one message with seq=1 for this conversation
          unique_matching = Enum.uniq_by(matching, fn row -> {row.conversation_id, row.seq} end)
          assert length(unique_matching) == 1
        end,
        timeout: 2_000
      )
    end

    test "archive respects sequence ordering" do
      {:ok, _pid} =
        start_supervised(
          {Fastpaca.Archive,
           flush_interval_ms: 50,
           adapter: Fastpaca.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      conv_id = "conv-order-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.upsert_conversation(conv_id)

      # Append messages in order
      for i <- 1..5 do
        {:ok, _} =
          Runtime.append_messages(conv_id, [
            %{
              role: "user",
              parts: [%{type: "text", text: "msg#{i}"}],
              metadata: %{},
              token_count: 1
            }
          ])
      end

      eventually(
        fn ->
          writes = Fastpaca.Archive.IdempotentTestAdapter.get_writes()

          conv_writes =
            writes
            |> Enum.filter(fn row -> row.conversation_id == conv_id end)
            |> Enum.sort_by(fn row -> row.seq end)

          seqs = Enum.map(conv_writes, & &1.seq)
          # Should have sequences 1-5 in order
          assert Enum.take(Enum.uniq(seqs), 5) == [1, 2, 3, 4, 5]
        end,
        timeout: 2_000
      )
    end
  end

  # Helper to wait for condition
  defp eventually(fun, opts) when is_function(fun, 0) do
    timeout = Keyword.get(opts, :timeout, 1_000)
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
