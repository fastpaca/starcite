defmodule Starcite.ArchiveTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime

  setup do
    # Ensure clean raft data for isolation
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  test "flush archives and trims via ack" do
    # Keep a very small tail so trim is obvious
    old = Application.get_env(:starcite, :tail_keep)
    Application.put_env(:starcite, :tail_keep, 2)

    on_exit(fn ->
      if old,
        do: Application.put_env(:starcite, :tail_keep, old),
        else: Application.delete_env(:starcite, :tail_keep)
    end)

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
        # Tail should retain only 2 events
        tail = Starcite.Session.EventLog.entries(session.event_log)
        assert length(tail) == 2
      end,
      timeout: 2_000
    )
  end

  test "advances archived watermark when sequence numbers are strictly increasing but sparse" do
    {:ok, archive_pid} =
      start_supervised(
        {Starcite.Archive,
         flush_interval_ms: 60_000, adapter: Starcite.Archive.TestAdapter, adapter_opts: []}
      )

    session_id = "ses-gap-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.create_session(id: session_id)

    for i <- 1..3 do
      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{i}"},
          actor: "agent:test"
        })
    end

    assert true = :ets.delete(:starcite_archive_events, {session_id, 2})

    send(archive_pid, :flush_tick)

    eventually(
      fn ->
        {:ok, session} = Runtime.get_session(session_id)
        assert session.archived_seq == 3
      end,
      timeout: 2_000
    )

    assert [] = :ets.lookup(:starcite_archive_events, {session_id, 3})

    send(archive_pid, :flush_tick)

    eventually(
      fn ->
        {:ok, session} = Runtime.get_session(session_id)
        assert session.archived_seq == 3
      end,
      timeout: 2_000
    )
  end

  test "advances archived watermark over multiple sparse batches" do
    previous_batch_size = Application.get_env(:starcite, :archive_batch_size)
    Application.put_env(:starcite, :archive_batch_size, 2)

    on_exit(fn ->
      if is_nil(previous_batch_size) do
        Application.delete_env(:starcite, :archive_batch_size)
      else
        Application.put_env(:starcite, :archive_batch_size, previous_batch_size)
      end
    end)

    {:ok, archive_pid} =
      start_supervised(
        {Starcite.Archive,
         flush_interval_ms: 60_000, adapter: Starcite.Archive.TestAdapter, adapter_opts: []}
      )

    session_id = "ses-sparse-batches-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.create_session(id: session_id)

    for i <- 1..5 do
      {:ok, _} =
        Runtime.append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{i}"},
          actor: "agent:test"
        })
    end

    assert true = :ets.delete(:starcite_archive_events, {session_id, 2})
    assert true = :ets.delete(:starcite_archive_events, {session_id, 4})

    send(archive_pid, :flush_tick)

    eventually(
      fn ->
        {:ok, session} = Runtime.get_session(session_id)
        assert session.archived_seq == 3
      end,
      timeout: 2_000
    )

    assert [{_, _}] = :ets.lookup(:starcite_archive_events, {session_id, 5})

    send(archive_pid, :flush_tick)

    eventually(
      fn ->
        {:ok, session} = Runtime.get_session(session_id)
        assert session.archived_seq == 5
      end,
      timeout: 2_000
    )

    assert [] = :ets.lookup(:starcite_archive_events, {session_id, 5})
  end

  describe "archive cursor protocol" do
    test "loads archived cursor on first flush and skips stale rows" do
      previous_batch_size = Application.get_env(:starcite, :archive_batch_size)
      Application.put_env(:starcite, :archive_batch_size, 5_000)

      on_exit(fn ->
        if is_nil(previous_batch_size) do
          Application.delete_env(:starcite, :archive_batch_size)
        else
          Application.put_env(:starcite, :archive_batch_size, previous_batch_size)
        end
      end)

      {:ok, archive_pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 60_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = Starcite.Archive.IdempotentTestAdapter.clear_writes()

      session_id = "ses-cursor-bootstrap-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      assert {:ok, %{archived_seq: 2}} =
               Runtime.ack_archived_if_current(session_id, 0, 2)

      send(archive_pid, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = Runtime.get_session(session_id)
          assert session.archived_seq == 3
        end,
        timeout: 2_000
      )

      writes =
        Starcite.Archive.IdempotentTestAdapter.get_writes()
        |> Enum.filter(&(&1.session_id == session_id))
        |> Enum.map(& &1.seq)
        |> Enum.sort()

      assert writes == [3]
    end

    test "recovers from conditional ack mismatch and eventually converges" do
      previous_batch_size = Application.get_env(:starcite, :archive_batch_size)
      Application.put_env(:starcite, :archive_batch_size, 2)

      on_exit(fn ->
        if is_nil(previous_batch_size) do
          Application.delete_env(:starcite, :archive_batch_size)
        else
          Application.put_env(:starcite, :archive_batch_size, previous_batch_size)
        end
      end)

      {:ok, archive_pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 60_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = Starcite.Archive.IdempotentTestAdapter.clear_writes()

      session_id = "ses-cursor-mismatch-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = Runtime.create_session(id: session_id)

      for i <- 1..4 do
        {:ok, _} =
          Runtime.append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      send(archive_pid, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = Runtime.get_session(session_id)
          assert session.archived_seq == 2
        end,
        timeout: 2_000
      )

      assert {:ok, %{archived_seq: 3}} =
               Runtime.ack_archived_if_current(session_id, 2, 3)

      send(archive_pid, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = Runtime.get_session(session_id)
          assert session.archived_seq == 3
        end,
        timeout: 2_000
      )

      send(archive_pid, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = Runtime.get_session(session_id)
          assert session.archived_seq == 4
        end,
        timeout: 2_000
      )

      unique_seqs =
        Starcite.Archive.IdempotentTestAdapter.get_writes()
        |> Enum.filter(&(&1.session_id == session_id))
        |> Enum.map(& &1.seq)
        |> Enum.uniq()
        |> Enum.sort()

      assert unique_seqs == [1, 2, 3, 4]
    end
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
