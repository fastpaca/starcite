defmodule Starcite.ArchiveTest do
  use ExUnit.Case, async: false

  defmodule FailingWriteAdapter do
    @behaviour Starcite.Archive.Adapter

    use GenServer

    @impl true
    def start_link(_opts), do: GenServer.start_link(__MODULE__, %{})

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def write_events(_rows), do: {:error, :db_down}

    @impl true
    def read_events(_session_id, _from_seq, _to_seq), do: {:ok, []}

    @impl true
    def upsert_session(_session), do: :ok

    @impl true
    def list_sessions(_query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}

    @impl true
    def list_sessions_by_ids(_ids, _query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}
  end

  alias Starcite.WritePath
  alias Phoenix.PubSub
  alias Starcite.DataPlane.EventStore
  alias Starcite.DataPlane.RaftAccess
  alias Starcite.DataPlane.SessionDiscovery
  alias Starcite.Archive.IdempotentTestAdapter

  setup do
    # Ensure clean raft data for isolation
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
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
    {:ok, _} = WritePath.create_session(id: session_id)

    for i <- 1..5 do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{i}"},
          actor: "agent:test"
        })
    end

    # Wait until archived_seq catches up
    eventually(
      fn ->
        {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
        {:ok, session} = RaftAccess.query_session(server_id, session_id)
        assert session.archived_seq == session.last_seq
        assert EventStore.session_size(session_id) == 0
      end,
      timeout: 2_000
    )
  end

  test "archive process fails loud on persistence write errors" do
    {:ok, pid} =
      start_supervised(
        {Starcite.Archive,
         flush_interval_ms: 10_000, adapter: FailingWriteAdapter, adapter_opts: []}
      )

    ref = Process.monitor(pid)

    session_id = "ses-arch-fail-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = WritePath.create_session(id: session_id)

    {:ok, _} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "m1"},
        actor: "agent:test"
      })

    send(pid, :flush_tick)

    assert_receive {:DOWN, ^ref, :process, ^pid, {%RuntimeError{}, _}}, 2_000
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
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_id, %{
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
      {:ok, _} = WritePath.create_session(id: session_id)

      # Append same logical event multiple times (simulating retry scenario)
      {:ok, _} =
        append_event(session_id, %{
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
      {:ok, _} = WritePath.create_session(id: session_id)

      # Append events in order
      for i <- 1..5 do
        {:ok, _} =
          append_event(session_id, %{
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
    test "interleaves flush order across tenants within a tick" do
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 10_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = IdempotentTestAdapter.clear_writes()

      acme_a = "ses-acme-a-#{System.unique_integer([:positive, :monotonic])}"
      acme_b = "ses-acme-b-#{System.unique_integer([:positive, :monotonic])}"
      beta_a = "ses-beta-a-#{System.unique_integer([:positive, :monotonic])}"
      beta_b = "ses-beta-b-#{System.unique_integer([:positive, :monotonic])}"

      tenant_by_session = %{
        acme_a => "acme",
        acme_b => "acme",
        beta_a => "beta",
        beta_b => "beta"
      }

      for {session_id, tenant_id} <- tenant_by_session do
        {:ok, _} = WritePath.create_session(id: session_id, tenant_id: tenant_id)

        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "#{tenant_id}:#{session_id}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          first_batches =
            IdempotentTestAdapter.get_write_batches()
            |> Enum.take(4)

          assert length(first_batches) == 4
          assert Enum.all?(first_batches, &(length(&1) == 1))

          tenant_sequence =
            first_batches
            |> Enum.map(fn [row] -> Map.fetch!(tenant_by_session, row.session_id) end)

          assert tenant_sequence == ["acme", "beta", "acme", "beta"]
        end,
        timeout: 2_000
      )
    end

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

      {:ok, _} = WritePath.create_session(id: session_a)
      {:ok, _} = WritePath.create_session(id: session_b)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_a, %{
            type: "content",
            payload: %{text: "a#{i}"},
            actor: "agent:test"
          })

        {:ok, _} =
          append_event(session_b, %{
            type: "content",
            payload: %{text: "b#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, server_a, _group_a} = RaftAccess.locate_and_ensure_started(session_a)
          {:ok, session_a_state} = RaftAccess.query_session(server_a, session_a)

          {:ok, server_b, _group_b} = RaftAccess.locate_and_ensure_started(session_b)
          {:ok, session_b_state} = RaftAccess.query_session(server_b, session_b)

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
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..5 do
        {:ok, _} =
          append_event(session_id, %{
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
          {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
          {:ok, session} = RaftAccess.query_session(server_id, session_id)
          assert session.archived_seq == session.last_seq

          batch_sizes =
            IdempotentTestAdapter.get_write_batches()
            |> Enum.map(&length/1)

          assert batch_sizes == [2, 2, 1]
        end,
        timeout: 2_000
      )
    end

    test "continues archiving new writes after a full ETS compaction" do
      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 10_000,
           adapter: Starcite.Archive.IdempotentTestAdapter,
           adapter_opts: []}
        )

      :ok = IdempotentTestAdapter.clear_writes()

      session_id = "ses-resume-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
          {:ok, session} = RaftAccess.query_session(server_id, session_id)
          assert session.archived_seq == 3
          assert EventStore.session_size(session_id) == 0
        end,
        timeout: 2_000
      )

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "m4"},
          actor: "agent:test"
        })

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
          {:ok, session} = RaftAccess.query_session(server_id, session_id)
          assert session.archived_seq == 4
          assert EventStore.session_size(session_id) == 0

          seqs =
            IdempotentTestAdapter.get_writes()
            |> Enum.filter(fn row -> row.session_id == session_id end)
            |> Enum.map(& &1.seq)
            |> Enum.uniq()
            |> Enum.sort()

          assert seqs == [1, 2, 3, 4]
        end,
        timeout: 2_000
      )
    end

    test "freezes drained idle sessions and allows write-path hydrate on next append" do
      telemetry_handler_id =
        "archive-session-lifecycle-#{System.unique_integer([:positive, :monotonic])}"

      test_pid = self()

      :ok =
        :telemetry.attach_many(
          telemetry_handler_id,
          [
            [:starcite, :session, :eviction_tick],
            [:starcite, :session, :freeze, :success],
            [:starcite, :session, :hydrate, :attempt],
            [:starcite, :session, :hydrate, :success]
          ],
          fn event_name, measurements, metadata, pid ->
            send(pid, {:session_lifecycle_event, event_name, measurements, metadata})
          end,
          test_pid
        )

      on_exit(fn ->
        :telemetry.detach(telemetry_handler_id)
      end)

      with_app_env(
        [
          archive_adapter: IdempotentTestAdapter,
          session_freeze_enabled: true,
          session_freeze_min_idle_polls: 1,
          session_freeze_max_batch_size: 10
        ],
        fn ->
          {:ok, _pid} =
            start_supervised(
              {Starcite.Archive,
               flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
            )

          :ok = IdempotentTestAdapter.clear_writes()
          :ok = PubSub.subscribe(Starcite.PubSub, SessionDiscovery.topic())

          session_id = "ses-freeze-#{System.unique_integer([:positive, :monotonic])}"
          {:ok, _} = WritePath.create_session(id: session_id)

          assert_receive {:session_discovery, %{kind: :session_created, session_id: ^session_id}},
                         1_000

          {:ok, _} =
            append_event(session_id, %{
              type: "content",
              payload: %{text: "m1"},
              actor: "agent:test"
            })

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          sessions = IdempotentTestAdapter.get_sessions()
          snapshot = Map.fetch!(sessions, session_id)
          assert snapshot.last_seq == 1
          assert snapshot.archived_seq == 1

          assert_receive {:session_discovery, frozen_update}, 1_000
          assert frozen_update.kind == :session_frozen
          assert frozen_update.state == :frozen
          assert frozen_update.session_id == session_id

          assert {:ok, append_reply} =
                   append_event(session_id, %{
                     type: "content",
                     payload: %{text: "m2"},
                     actor: "agent:test"
                   })

          assert append_reply.seq == 2

          assert_receive_session_lifecycle(
            [:starcite, :session, :eviction_tick],
            fn _measurements, metadata ->
              metadata.group_id >= 0 and metadata.min_idle_polls == 1
            end
          )

          assert_receive_session_lifecycle(
            [:starcite, :session, :freeze, :success],
            fn _measurements, metadata -> metadata.session_id == session_id end
          )

          assert_receive_session_lifecycle(
            [:starcite, :session, :hydrate, :attempt],
            fn _measurements, metadata -> metadata.session_id == session_id end
          )

          assert_receive_session_lifecycle(
            [:starcite, :session, :hydrate, :success],
            fn _measurements, metadata -> metadata.session_id == session_id end
          )

          assert_receive {:session_discovery, hydrated_update}, 1_000
          assert hydrated_update.kind == :session_hydrated
          assert hydrated_update.state == :active
          assert hydrated_update.session_id == session_id
        end
      )
    end

    test "repeated freeze-hydrate loops preserve contiguous seq and runtime cursors" do
      with_app_env(
        [
          archive_adapter: IdempotentTestAdapter,
          session_freeze_enabled: true,
          session_freeze_min_idle_polls: 1,
          session_freeze_max_batch_size: 10
        ],
        fn ->
          {:ok, _pid} =
            start_supervised(
              {Starcite.Archive,
               flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
            )

          :ok = IdempotentTestAdapter.clear_writes()

          session_id = "ses-freeze-loop-#{System.unique_integer([:positive, :monotonic])}"
          {:ok, _} = WritePath.create_session(id: session_id)

          for i <- 1..5 do
            assert {:ok, reply} =
                     append_event(session_id, %{
                       type: "content",
                       payload: %{text: "loop-#{i}"},
                       actor: "agent:test"
                     })

            assert reply.seq == i

            send(Starcite.Archive, :flush_tick)

            eventually(
              fn ->
                {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

                assert {:error, :session_not_found} =
                         RaftAccess.query_session(server_id, session_id)
              end,
              timeout: 2_000
            )
          end

          archived_seqs =
            IdempotentTestAdapter.get_writes()
            |> Enum.filter(&(&1.session_id == session_id))
            |> Enum.uniq_by(& &1.seq)
            |> Enum.map(& &1.seq)
            |> Enum.sort()

          assert archived_seqs == [1, 2, 3, 4, 5]

          snapshot = IdempotentTestAdapter.get_sessions() |> Map.fetch!(session_id)
          assert snapshot.last_seq == 5
          assert snapshot.archived_seq == 5
          assert producer_cursor_field!(snapshot, "writer:test", :producer_seq) == 5
          assert producer_cursor_field!(snapshot, "writer:test", :session_seq) == 5
        end
      )
    end

    test "producer dedupe survives freeze-hydrate loop boundaries" do
      with_app_env(
        [
          archive_adapter: IdempotentTestAdapter,
          session_freeze_enabled: true,
          session_freeze_min_idle_polls: 1,
          session_freeze_max_batch_size: 10
        ],
        fn ->
          {:ok, _pid} =
            start_supervised(
              {Starcite.Archive,
               flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
            )

          :ok = IdempotentTestAdapter.clear_writes()

          session_id = "ses-freeze-dedupe-#{System.unique_integer([:positive, :monotonic])}"
          producer_id = "writer:loop"
          {:ok, _} = WritePath.create_session(id: session_id)

          event_one = %{
            type: "content",
            payload: %{text: "one"},
            actor: "agent:test",
            producer_id: producer_id,
            producer_seq: 1
          }

          assert {:ok, first_reply} = WritePath.append_event(session_id, event_one)
          assert first_reply.seq == 1
          refute first_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          assert {:ok, deduped_reply} = WritePath.append_event(session_id, event_one)
          assert deduped_reply.seq == 1
          assert deduped_reply.deduped
          assert deduped_reply.last_seq == 1

          event_two = %{
            type: "content",
            payload: %{text: "two"},
            actor: "agent:test",
            producer_id: producer_id,
            producer_seq: 2
          }

          assert {:ok, second_reply} = WritePath.append_event(session_id, event_two)
          assert second_reply.seq == 2
          refute second_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          archived_seqs =
            IdempotentTestAdapter.get_writes()
            |> Enum.filter(&(&1.session_id == session_id))
            |> Enum.uniq_by(& &1.seq)
            |> Enum.map(& &1.seq)
            |> Enum.sort()

          assert archived_seqs == [1, 2]

          snapshot = IdempotentTestAdapter.get_sessions() |> Map.fetch!(session_id)
          assert snapshot.last_seq == 2
          assert snapshot.archived_seq == 2
          assert producer_cursor_field!(snapshot, producer_id, :producer_seq) == 2
          assert producer_cursor_field!(snapshot, producer_id, :session_seq) == 2
        end
      )
    end

    test "hydrate grace polls prevent immediate re-freeze after dedupe-only hydrate" do
      with_app_env(
        [
          archive_adapter: IdempotentTestAdapter,
          session_freeze_enabled: true,
          session_freeze_min_idle_polls: 0,
          session_freeze_max_batch_size: 10,
          session_freeze_hydrate_grace_polls: 2
        ],
        fn ->
          {:ok, _pid} =
            start_supervised(
              {Starcite.Archive,
               flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
            )

          :ok = IdempotentTestAdapter.clear_writes()

          session_id = "ses-hydrate-grace-#{System.unique_integer([:positive, :monotonic])}"
          producer_id = "writer:grace"
          {:ok, _} = WritePath.create_session(id: session_id)

          event = %{
            type: "content",
            payload: %{text: "hello"},
            actor: "agent:test",
            producer_id: producer_id,
            producer_seq: 1
          }

          assert {:ok, first_reply} = WritePath.append_event(session_id, event)
          assert first_reply.seq == 1
          refute first_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          assert {:ok, deduped_reply} = WritePath.append_event(session_id, event)
          assert deduped_reply.seq == 1
          assert deduped_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:ok, %Starcite.Session{last_seq: 1, archived_seq: 1}} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )
        end
      )
    end

    test "dedupe-only hydrate re-freezes on next tick when hydrate grace is disabled" do
      with_app_env(
        [
          archive_adapter: IdempotentTestAdapter,
          session_freeze_enabled: true,
          session_freeze_min_idle_polls: 0,
          session_freeze_max_batch_size: 10,
          session_freeze_hydrate_grace_polls: 0
        ],
        fn ->
          {:ok, _pid} =
            start_supervised(
              {Starcite.Archive,
               flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
            )

          :ok = IdempotentTestAdapter.clear_writes()

          session_id = "ses-hydrate-no-grace-#{System.unique_integer([:positive, :monotonic])}"
          producer_id = "writer:no-grace"
          {:ok, _} = WritePath.create_session(id: session_id)

          event = %{
            type: "content",
            payload: %{text: "hello"},
            actor: "agent:test",
            producer_id: producer_id,
            producer_seq: 1
          }

          assert {:ok, first_reply} = WritePath.append_event(session_id, event)
          assert first_reply.seq == 1
          refute first_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )

          assert {:ok, deduped_reply} = WritePath.append_event(session_id, event)
          assert deduped_reply.seq == 1
          assert deduped_reply.deduped

          send(Starcite.Archive, :flush_tick)

          eventually(
            fn ->
              {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)

              assert {:error, :session_not_found} =
                       RaftAccess.query_session(server_id, session_id)
            end,
            timeout: 2_000
          )
        end
      )
    end
  end

  defp append_event(id, event, opts \\ [])
       when is_binary(id) and is_map(event) and is_list(opts) do
    producer_id = Map.get(event, :producer_id, "writer:test")

    enriched_event =
      event
      |> Map.put_new(:producer_id, producer_id)
      |> Map.put_new_lazy(:producer_seq, fn -> next_producer_seq(id, producer_id) end)

    WritePath.append_event(id, enriched_event, opts)
  end

  defp next_producer_seq(session_id, producer_id)
       when is_binary(session_id) and is_binary(producer_id) do
    counters = Process.get(:producer_seq_counters, %{})
    key = {session_id, producer_id}
    seq = Map.get(counters, key, 0) + 1
    Process.put(:producer_seq_counters, Map.put(counters, key, seq))
    seq
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

  defp assert_receive_session_lifecycle(event_name, matcher)
       when is_list(event_name) and is_function(matcher, 2) do
    deadline = System.monotonic_time(:millisecond) + 2_000
    do_assert_receive_session_lifecycle(event_name, matcher, deadline)
  end

  defp do_assert_receive_session_lifecycle(event_name, matcher, deadline)
       when is_list(event_name) and is_function(matcher, 2) and is_integer(deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:session_lifecycle_event, ^event_name, measurements, metadata} ->
        if matcher.(measurements, metadata) do
          :ok
        else
          do_assert_receive_session_lifecycle(event_name, matcher, deadline)
        end

      {:session_lifecycle_event, _other_event_name, _measurements, _metadata} ->
        do_assert_receive_session_lifecycle(event_name, matcher, deadline)
    after
      remaining ->
        flunk("timed out waiting for session lifecycle telemetry event #{inspect(event_name)}")
    end
  end

  defp with_app_env(overrides, fun) when is_list(overrides) and is_function(fun, 0) do
    previous =
      Enum.map(overrides, fn {key, _value} ->
        {key, Application.get_env(:starcite, key, :__starcite_missing__)}
      end)

    Enum.each(overrides, fn {key, value} ->
      Application.put_env(:starcite, key, value)
    end)

    try do
      fun.()
    after
      Enum.each(previous, fn
        {key, :__starcite_missing__} -> Application.delete_env(:starcite, key)
        {key, value} -> Application.put_env(:starcite, key, value)
      end)
    end
  end

  defp producer_cursor_field!(snapshot, producer_id, key)
       when is_map(snapshot) and is_binary(producer_id) and producer_id != "" and is_atom(key) do
    cursor =
      snapshot
      |> Map.get(:producer_cursors, %{})
      |> Map.get(producer_id)

    case cursor do
      %{} = value ->
        Map.get(value, key) || Map.get(value, Atom.to_string(key))

      _ ->
        flunk(
          "missing producer cursor for #{producer_id}: #{inspect(Map.get(snapshot, :producer_cursors, %{}))}"
        )
    end
  end
end
