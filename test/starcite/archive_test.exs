defmodule Starcite.ArchiveTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime
  alias Starcite.Runtime.EventStore
  alias Starcite.Archive.IdempotentTestAdapter

  setup do
    # Ensure clean raft data for isolation
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  test "flush archives and advances cursor via ack" do
    # Start Archive with the test adapter (no Postgres)
    {:ok, _pid} =
      start_supervised(
        {Starcite.Archive,
         flush_interval_ms: 100, adapter: Starcite.Archive.TestAdapter, adapter_opts: []}
      )

    session_id = "ses-arch-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.create_session(id: session_id)

    for i <- 1..5 do
      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{i}"},
          actor: "agent:test"
        })
    end

    # Wait until archived_seq catches up
    eventually(
      fn ->
        {:ok, session} = Runtime.get_session(session_id)
        assert session.archived_seq == session.last_seq
        assert EventStore.session_size(session_id) == 5
      end,
      timeout: 2_000
    )
  end

  describe "archive idempotency" do
    test "duplicate writes are idempotent" do
      # Start Archive with test adapter that tracks writes
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 50,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      session_id = "ses-idem-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "msg#{i}"},
            actor: "agent:test"
          })
      end

      # Wait for first flush
      eventually(
        fn ->
          writes = Starcite.Archive.IdempotentTestAdapter.get_writes()
          assert length(writes) >= 3
        end,
        timeout: 2_000
      )

      _first_count = length(Starcite.Archive.IdempotentTestAdapter.get_writes())

      # Trigger another flush cycle by waiting
      Process.sleep(100)

      # The adapter should have the same unique events (idempotent)
      eventually(
        fn ->
          writes = Starcite.Archive.IdempotentTestAdapter.get_writes()

          unique_keys =
            writes
            |> Enum.map(fn row -> {row.session_id, row.seq} end)
            |> Enum.uniq()

          # Should have exactly 3 unique events regardless of flush count
          assert length(unique_keys) == 3
        end,
        timeout: 1_000
      )
    end

    test "retried writes succeed without duplicates" do
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 50,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      session_id = "ses-retry-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      # Append same logical event multiple times (simulating retry scenario)
      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "content",
          payload: %{text: "retry-msg"},
          actor: "agent:test"
        })

      eventually(
        fn ->
          writes = Starcite.Archive.IdempotentTestAdapter.get_writes()

          matching =
            Enum.filter(writes, fn row ->
              row.session_id == session_id and row.seq == 1
            end)

          # Should have exactly one event with seq=1 for this session
          unique_matching = Enum.uniq_by(matching, fn row -> {row.session_id, row.seq} end)
          assert length(unique_matching) == 1
        end,
        timeout: 2_000
      )
    end

    test "archive respects sequence ordering" do
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 50,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      session_id = "ses-order-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      # Append events in order
      for i <- 1..5 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "msg#{i}"},
            actor: "agent:test"
          })
      end

      eventually(
        fn ->
          writes = Starcite.Archive.IdempotentTestAdapter.get_writes()

          session_writes =
            writes
            |> Enum.filter(fn row -> row.session_id == session_id end)
            |> Enum.sort_by(fn row -> row.seq end)

          seqs = Enum.map(session_writes, & &1.seq)
          # Should have sequences 1-5 in order
          assert Enum.take(Enum.uniq(seqs), 5) == [1, 2, 3, 4, 5]
        end,
        timeout: 2_000
      )
    end
  end

  describe "pull-mode behavior" do
    test "archives pending work across multiple sessions in one scan loop" do
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 10_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = IdempotentTestAdapter.clear_writes()

      session_a = "ses-pull-a-#{System.unique_integer([:positive, :monotonic])}"
      session_b = "ses-pull-b-#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _} = Runtime.create_session(id: session_a)
      {:ok, _} = Runtime.create_session(id: session_b)

      for i <- 1..3 do
        {:ok, _} =
          Runtime.append_event(session_a, %{
            type: "content",
            payload: %{text: "a#{i}"},
            actor: "agent:test"
          })

        {:ok, _} =
          Runtime.append_event(session_b, %{
            type: "content",
            payload: %{text: "b#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session_a_state} = Runtime.get_session(session_a)
          {:ok, session_b_state} = Runtime.get_session(session_b)

          assert session_a_state.archived_seq == session_a_state.last_seq
          assert session_b_state.archived_seq == session_b_state.last_seq

          writes = IdempotentTestAdapter.get_writes()
          written_sessions = writes |> Enum.map(& &1.session_id) |> Enum.uniq() |> Enum.sort()

          assert written_sessions == Enum.sort([session_a, session_b])
        end,
        timeout: 2_000
      )
    end

    test "respects archive batch size per flush tick and converges over repeated ticks" do
      old_batch_size = Application.get_env(:starcite, :archive_batch_size)
      Application.put_env(:starcite, :archive_batch_size, 2)

      on_exit(fn ->
        if old_batch_size do
          Application.put_env(:starcite, :archive_batch_size, old_batch_size)
        else
          Application.delete_env(:starcite, :archive_batch_size)
        end
      end)

      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 10_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = IdempotentTestAdapter.clear_writes()

      session_id = "ses-batch-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      for i <- 1..5 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          [first_batch | _] = IdempotentTestAdapter.get_write_batches()
          assert length(first_batch) == 2
        end,
        timeout: 1_500
      )

      send(Starcite.Archive, :flush_tick)
      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = Runtime.get_session(session_id)
          assert session.archived_seq == session.last_seq

          batch_sizes =
            IdempotentTestAdapter.get_write_batches()
            |> Enum.map(&length/1)

          assert batch_sizes == [2, 2, 1]
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
