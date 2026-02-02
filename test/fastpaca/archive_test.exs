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
