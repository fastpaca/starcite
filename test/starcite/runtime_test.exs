defmodule Starcite.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Starcite.Auth.Principal
  alias Starcite.Operations
  alias Starcite.{ReadPath, WritePath}
  alias Starcite.DataPlane.{EventStore, SessionQuorum, SessionStore}
  alias Starcite.Routing.Store, as: RoutingStore
  alias Starcite.Routing.Watcher
  alias Starcite.Session
  alias Starcite.Storage.{EventArchive, SessionCatalog}

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{:rand.uniform(999_999_999)}"
  end

  defp put_assignment(session_id, assignment) when is_binary(session_id) and is_map(assignment) do
    :khepri.put(
      Starcite.Routing.Store.store_id(),
      [:sessions, session_id],
      assignment,
      %{async: false, reply_from: :local, timeout: 5_000}
    )
  end

  describe "create/get session" do
    test "creates a new session with generated id" do
      {:ok, session} = WritePath.create_session(title: "Draft", metadata: %{workflow: "legal"})

      assert is_binary(session.id)
      assert String.starts_with?(session.id, "ses_")
      assert session.title == "Draft"
      assert session.metadata == %{workflow: "legal"}
      assert session.last_seq == 0
    end

    test "creates with caller-provided id and rejects duplicates" do
      id = unique_id("ses")

      {:ok, session} = WritePath.create_session(id: id, title: "Draft")
      assert session.id == id

      assert {:error, :session_exists} = WritePath.create_session(id: id)
    end

    test "gets existing session" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, session} = ReadPath.get_session(id)
      assert session.id == id
      assert session.last_seq == 0
    end

    test "gets existing session through routed read" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, session} = ReadPath.get_session_routed(id, true)
      assert session.id == id
      assert session.last_seq == 0
    end

    test "cold session reads restore archived cursors from the durable catalog" do
      id = unique_id("ses-cold-read")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      assert {:ok, %{seq: 1}} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "cold read"},
                 actor: "agent:test"
               })

      flush_archive_until(id, 1)
      :ok = SessionStore.delete_session(id)

      assert {:ok, %Session{id: ^id, last_seq: 1, archived_seq: 1}} = ReadPath.get_session(id)
    end

    test "returns not found for missing session" do
      session_id = unique_id("missing")

      assert {:error, :session_not_found} = ReadPath.get_session(session_id)
      assert {:error, :not_found} = RoutingStore.get_assignment(session_id, favor: :consistency)
    end

    test "create_session warms session store for immediate reads" do
      id = unique_id("ses")
      principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}
      {:ok, _session} = WritePath.create_session(id: id, creator_principal: principal)
      assert {:ok, loaded} = SessionStore.get_session(id)
      assert loaded.id == id
    end

    test "auth lookup returns session store hit without raft/archive read-through" do
      id = unique_id("ses")
      principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}
      session = Session.new(id, creator_principal: principal, metadata: %{"source" => "cache"})
      assert :ok = SessionStore.put_session(session)

      assert {:ok, loaded} = ReadPath.get_session(id)
      assert loaded.id == id
    end

    test "create_session rolls back local owner/session cache when replication quorum fails" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      end)

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), :"missing@127.0.0.1"])
      Application.put_env(:starcite, :routing_replication_factor, 2)

      id = unique_id("ses")
      assert {:error, _reason} = WritePath.create_session(id: id, tenant_id: "acme")

      refute SessionQuorum.local_owner?(id)
      assert :error == SessionStore.get_session_cached(id)
      assert {:error, :session_not_found} = ReadPath.get_session(id)
    end

    test "local_owner? promotes a follower log after routing ownership changes" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      end)

      peer = :"runtime-owner-peer@127.0.0.1"
      id = unique_id("ses")

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
      Application.put_env(:starcite, :routing_replication_factor, 2)
      Starcite.Runtime.TestHelper.reset()

      assert :ok =
               put_assignment(id, %{
                 owner: peer,
                 epoch: 1,
                 replicas: [peer, Node.self()],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               })

      session = %Session{Session.new(id) | epoch: 1}
      assert :ok = SessionStore.put_session(session)
      assert {:ok, loaded} = SessionQuorum.get_session(id)
      assert loaded.id == id
      refute SessionQuorum.local_owner?(id)

      assert :ok =
               put_assignment(id, %{
                 owner: Node.self(),
                 epoch: 2,
                 replicas: [Node.self(), peer],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               })

      assert SessionQuorum.local_owner?(id)
    end

    test "follower runtime emits a routing fence when it receives an append" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
      peer = :"runtime-follower-peer@127.0.0.1"
      id = unique_id("ses")
      session = %Session{Session.new(id) | epoch: 1}
      handler_id = "routing-fence-runtime-#{System.unique_integer([:positive, :monotonic])}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:starcite, :routing, :fence],
          fn _event, measurements, metadata, pid ->
            send(pid, {:routing_fence_event, measurements, metadata})
          end,
          test_pid
        )

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
        :telemetry.detach(handler_id)
      end)

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
      Application.put_env(:starcite, :routing_replication_factor, 2)
      Starcite.Runtime.TestHelper.reset()

      assert :ok =
               put_assignment(id, %{
                 owner: peer,
                 epoch: 1,
                 replicas: [peer, Node.self()],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               })

      assert :ok = SessionStore.put_session(session)
      assert {:ok, ^session} = SessionQuorum.get_session(id)
      refute SessionQuorum.local_owner?(id)

      [{pid, _value}] =
        Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)

      assert {:error, :not_owner} =
               :gen_batch_server.call(
                 pid,
                 {:append_event, %{type: "content", payload: %{text: "nope"}}, nil,
                  [peer, Node.self()]},
                 5_000
               )

      assert_receive {:routing_fence_event, %{count: 1},
                      %{session_id: ^id, source: :session_log, reason: :not_owner}},
                     1_000
    end

    test "watcher startup rejoins a drained node as ready" do
      assert :ok = RoutingStore.mark_node_drained(Node.self())
      assert RoutingStore.node_status(Node.self()) == :drained

      old_pid = Process.whereis(Watcher)
      assert is_pid(old_pid)
      assert :ok = GenServer.stop(old_pid, :normal)

      restarted_pid = wait_for_process_restart(Watcher, old_pid, 5_000)
      assert is_pid(restarted_pid)
      assert :ok = Operations.wait_local_ready(5_000)
      assert RoutingStore.node_status(Node.self()) == :ready
    end

    test "watcher startup ignores a stale ready cache until the consistent store is ready" do
      assert :ok = RoutingStore.mark_node_drained(Node.self())
      assert RoutingStore.node_status(Node.self()) == :drained

      true =
        :ets.insert(:starcite_routing_node_record_cache, {
          Node.self(),
          %{
            status: :ready,
            lease_until_ms: System.system_time(:millisecond) + 60_000,
            updated_at_ms: 0
          }
        })

      old_pid = Process.whereis(Watcher)
      assert is_pid(old_pid)
      assert :ok = GenServer.stop(old_pid, :normal)

      restarted_pid = wait_for_process_restart(Watcher, old_pid, 5_000)
      assert is_pid(restarted_pid)
      assert :ok = Operations.wait_local_ready(5_000)
      assert {:ok, %{status: :ready}} = RoutingStore.node_record(Node.self(), favor: :consistency)
    end

    test "local readiness rejects an expired authoritative lease even with a stale ready cache" do
      now_ms = System.system_time(:millisecond)

      on_exit(fn ->
        assert :ok = RoutingStore.renew_local_lease()
      end)

      true =
        :ets.insert(:starcite_routing_node_record_cache, {
          Node.self(),
          %{
            status: :ready,
            lease_until_ms: now_ms + 60_000,
            updated_at_ms: now_ms
          }
        })

      assert :ok =
               :khepri.put(
                 RoutingStore.store_id(),
                 [:nodes, Atom.to_string(Node.self())],
                 %{
                   status: :ready,
                   lease_until_ms: now_ms - 1,
                   updated_at_ms: now_ms
                 },
                 %{async: false, reply_from: :local, timeout: 5_000}
               )

      readiness = Operations.local_readiness(refresh?: true)
      refute readiness.ready?
      assert readiness.reason == :lease_expired
      assert readiness.detail.lease_until_ms == now_ms - 1
      assert {503, %{reason: "lease_expired"}} = StarciteWeb.Health.ready()
    end

    test "local readiness rejects a missing authoritative node record even with a stale ready cache" do
      now_ms = System.system_time(:millisecond)

      on_exit(fn ->
        assert :ok = RoutingStore.renew_local_lease()
      end)

      true =
        :ets.insert(:starcite_routing_node_record_cache, {
          Node.self(),
          %{
            status: :ready,
            lease_until_ms: now_ms + 60_000,
            updated_at_ms: now_ms
          }
        })

      assert :ok =
               :khepri.delete(
                 RoutingStore.store_id(),
                 [:nodes, Atom.to_string(Node.self())],
                 %{async: false, reply_from: :local, timeout: 5_000}
               )

      assert {:error, {:khepri, :node_not_found, _}} =
               RoutingStore.node_record(Node.self(), favor: :consistency)

      readiness = Operations.local_readiness(refresh?: true)
      refute readiness.ready?
      assert readiness.reason == :routing_sync
      assert readiness.checks.routing_store.detail.status == :unknown
      assert {503, %{reason: "routing_sync"}} = StarciteWeb.Health.ready()
    end

    test "watcher startup keeps a draining node draining while active sessions remain" do
      id = unique_id("ses")
      now_ms = System.system_time(:millisecond)

      assert :ok =
               put_assignment(id, %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self()],
                 status: :active,
                 updated_at_ms: now_ms
               })

      assert :ok = RoutingStore.mark_node_draining(Node.self())
      assert RoutingStore.node_status(Node.self()) == :draining

      old_pid = Process.whereis(Watcher)
      assert is_pid(old_pid)
      assert :ok = GenServer.stop(old_pid, :normal)

      restarted_pid = wait_for_process_restart(Watcher, old_pid, 5_000)
      assert is_pid(restarted_pid)

      Process.sleep(250)
      assert RoutingStore.node_status(Node.self()) == :draining
      assert {:error, :timeout} = Operations.wait_local_ready(500)
    end

    test "watcher startup reopens a drained node back to draining when drain work remains" do
      id = unique_id("ses")
      now_ms = System.system_time(:millisecond)

      assert :ok =
               put_assignment(id, %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self()],
                 status: :moving,
                 target_owner: :"peer@127.0.0.1",
                 transfer_id: "xfer-startup-reopen",
                 updated_at_ms: now_ms
               })

      assert :ok = RoutingStore.mark_node_drained(Node.self())
      assert RoutingStore.node_status(Node.self()) == :drained

      old_pid = Process.whereis(Watcher)
      assert is_pid(old_pid)
      assert :ok = GenServer.stop(old_pid, :normal)

      restarted_pid = wait_for_process_restart(Watcher, old_pid, 5_000)
      assert is_pid(restarted_pid)

      Process.sleep(250)
      assert RoutingStore.node_status(Node.self()) == :draining
      assert {:error, :timeout} = Operations.wait_local_ready(500)
    end
  end

  describe "append_event/3" do
    test "appends events with monotonic seq" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, r1} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      {:ok, r2} =
        append_event(id, %{
          type: "content",
          payload: %{text: "two"},
          actor: "agent:1"
        })

      assert r1.seq == 1
      assert r2.seq == 2
      assert r2.last_seq == 2
      refute r2.deduped
    end

    test "guards on expected_seq" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:error, {:expected_seq_conflict, 0, 1}} =
               append_event(
                 id,
                 %{type: "content", payload: %{text: "two"}, actor: "agent:1"},
                 expected_seq: 0
               )
    end

    test "dedupes when producer sequence repeats with same payload" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      event = %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      {:ok, first} = append_event(id, event)
      {:ok, second} = append_event(id, event)

      assert first.seq == second.seq
      assert second.deduped
      assert second.last_seq == 1
    end

    test "deduped retry does not require fresh quorum" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
      missing = :"missing-dedup-replica@127.0.0.1"
      id = unique_id("ses")
      now_ms = System.system_time(:millisecond)

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      end)

      {:ok, _} = WritePath.create_session(id: id)

      event = %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      assert {:ok, first} = append_event(id, event)
      refute first.deduped

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), missing])
      Application.put_env(:starcite, :routing_replication_factor, 2)

      assert :ok =
               put_assignment(id, %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self(), missing],
                 status: :active,
                 updated_at_ms: now_ms
               })

      true = :ets.delete(:starcite_routing_assignment_cache, id)

      assert {:ok, second} = append_event(id, event)
      assert second.deduped
      assert second.seq == 1
      assert second.last_seq == 1

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.last_seq == 1

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 10)
      assert Enum.map(events, & &1.seq) == [1]
    end

    test "errors on producer replay conflict" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:error, :producer_replay_conflict} =
               append_event(id, %{
                 type: "state",
                 payload: %{state: "completed"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 1
               })
    end

    test "does not leak visible state when replication quorum is not met" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
      missing = :"missing-replica@127.0.0.1"
      id = unique_id("ses")
      now_ms = System.system_time(:millisecond)

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      end)

      {:ok, _} = WritePath.create_session(id: id)

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), missing])
      Application.put_env(:starcite, :routing_replication_factor, 2)

      assert :ok =
               put_assignment(id, %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self(), missing],
                 status: :active,
                 updated_at_ms: now_ms
               })

      true = :ets.delete(:starcite_routing_assignment_cache, id)
      assert {:ok, assignment} = RoutingStore.get_assignment(id, favor: :consistency)
      assert assignment.owner == Node.self()
      assert assignment.replicas == [Node.self(), missing]

      assert {:error, {:replication_quorum_not_met, details}} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "should not commit"},
                 actor: "agent:1"
               })

      assert details.required_remote_acks == 1
      assert details.successful_remote_acks == 0

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.last_seq == 0

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 10)
      assert events == []

      assert {:ok, cursor} = SessionQuorum.fetch_cursor_snapshot(id)
      assert cursor.last_seq == 0
      assert cursor.committed_seq == 0
    end
  end

  describe "append_events/3" do
    test "appends a batch with contiguous sequence numbers" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, reply} =
        WritePath.append_events(id, [
          %{
            type: "content",
            payload: %{text: "one"},
            actor: "agent:1",
            producer_id: "writer:test",
            producer_seq: 1
          },
          %{
            type: "content",
            payload: %{text: "two"},
            actor: "agent:1",
            producer_id: "writer:test",
            producer_seq: 2
          }
        ])

      assert reply.last_seq == 2
      assert Enum.map(reply.results, & &1.seq) == [1, 2]
      assert Enum.map(reply.results, & &1.last_seq) == [1, 2]
      refute Enum.any?(reply.results, & &1.deduped)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 10)
      assert Enum.map(events, & &1.seq) == [1, 2]
      assert Enum.map(events, & &1.payload) == [%{text: "one"}, %{text: "two"}]
    end

    test "guards on expected_seq for a batch append" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:error, {:expected_seq_conflict, 0, 1}} =
               WritePath.append_events(
                 id,
                 [
                   %{
                     type: "content",
                     payload: %{text: "two"},
                     actor: "agent:1",
                     producer_id: "writer:test",
                     producer_seq: 2
                   }
                 ],
                 expected_seq: 0
               )
    end
  end

  describe "get_events_from_cursor/3" do
    test "returns events strictly after cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      {:ok, events} = ReadPath.get_events_from_cursor(id, 2, 100)
      assert Enum.map(events, & &1.seq) == [3, 4, 5]
    end

    test "returns only unarchived events after archive cutover compacts ETS" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 3, 100)
      assert Enum.map(events, & &1.seq) == [4, 5]
    end

    test "returns hot events even when session metadata is unavailable" do
      missing_id = unique_id("missing")

      :ok =
        EventStore.put_event(missing_id, "acme", %{
          seq: 1,
          type: "content",
          payload: %{text: "rogue"},
          actor: "agent:1",
          producer_id: "writer:test",
          producer_seq: 1,
          source: nil,
          metadata: %{},
          refs: %{},
          idempotency_key: nil,
          inserted_at: NaiveDateTime.utc_now()
        })

      assert {:ok, events} = ReadPath.get_events_from_cursor(missing_id, 0, 100)
      assert Enum.map(events, & &1.seq) == [1]
    end

    test "rejects replay when the first returned event skips past the cursor" do
      missing_id = unique_id("missing-gap")

      for seq <- [7, 8] do
        :ok =
          EventStore.put_event(missing_id, "acme", %{
            seq: seq,
            type: "content",
            payload: %{text: "rogue-#{seq}"},
            actor: "agent:1",
            producer_id: "writer:test",
            producer_seq: seq,
            source: nil,
            metadata: %{},
            refs: %{},
            idempotency_key: nil,
            inserted_at: NaiveDateTime.utc_now()
          })
      end

      assert {:error, :event_gap_detected} = ReadPath.get_events_from_cursor(missing_id, 5, 100)
    end

    test "returns ordered events across archive cold + ETS hot boundary" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      cold_rows = EventStore.from_cursor(id, 0, 3)
      insert_cold_rows(id, cold_rows)

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
      assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5]
    end

    test "respects limit across archive cold + ETS hot boundary" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      cold_rows = EventStore.from_cursor(id, 0, 3)
      insert_cold_rows(id, cold_rows)

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 2, 2)
      assert Enum.map(events, & &1.seq) == [3, 4]
    end
  end

  describe "ack_archived/2" do
    test "is idempotent for the same archive cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..4 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{archived_seq: 2, trimmed: 2}} = WritePath.ack_archived(id, 2)
      assert EventStore.session_size(id) == 2

      assert {:ok, %{archived_seq: 2, trimmed: 0}} = WritePath.ack_archived(id, 2)
      assert EventStore.session_size(id) == 2
    end

    test "clamps archive cursor to last_seq and compacts the full hot tail" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..3 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 10_000)
      assert EventStore.session_size(id) == 0
    end

    test "does not regress the committed frontier when acked below the current archive cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..4 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)
      assert EventStore.session_size(id) == 1

      assert {:ok, %{archived_seq: 3, trimmed: 0}} = WritePath.ack_archived(id, 1)
      assert EventStore.session_size(id) == 1

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.archived_seq == 3

      assert {:ok, cursor} = SessionQuorum.fetch_cursor_snapshot(id)
      assert cursor.committed_seq == 3
    end

    test "emits invariant telemetry and preserves the committed frontier when publication sees a stale lower watermark" do
      id = unique_id("ses")

      handler_id =
        "data-plane-invariant-runtime-#{System.unique_integer([:positive, :monotonic])}"

      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:starcite, :data_plane, :invariant],
          fn _event, measurements, metadata, pid ->
            send(pid, {:data_plane_invariant_event, measurements, metadata})
          end,
          test_pid
        )

      on_exit(fn ->
        :telemetry.detach(handler_id)
      end)

      {:ok, _} = WritePath.create_session(id: id)

      assert {:ok, _reply} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "seed"},
                 actor: "agent:1"
               })

      assert {:ok, %Session{} = current} = SessionQuorum.get_session(id)

      assert :ok =
               SessionStore.put_session(%Session{
                 current
                 | archived_seq: current.archived_seq + 1
               })

      assert {:ok, reply} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "next"},
                 actor: "agent:1"
               })

      assert_receive {:data_plane_invariant_event, %{count: 1},
                      %{
                        session_id: ^id,
                        source: :publish_events,
                        reason: :publication_watermark_regression
                      }},
                     1_000

      assert reply.committed_cursor.seq == 1
      assert {:ok, session} = SessionQuorum.get_session(id)
      assert session.archived_seq == 1

      assert {:ok, cursor} = SessionQuorum.fetch_cursor_snapshot(id)
      assert cursor.committed_seq == 1
    end
  end

  describe "replica monotonicity" do
    test "ignores stale lower-epoch replica state even when it advertises a larger last_seq" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..2 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %Session{} = current} = SessionQuorum.get_session(id)

      newer =
        %Session{
          current
          | epoch: current.epoch + 1,
            archived_seq: current.archived_seq + 1
        }

      assert :ok = SessionQuorum.apply_replica(newer, [])
      assert {:ok, %Session{} = promoted} = SessionQuorum.get_session(id)
      assert promoted.epoch == newer.epoch
      assert promoted.last_seq == current.last_seq
      assert promoted.archived_seq == newer.archived_seq

      stale =
        %Session{
          promoted
          | epoch: current.epoch,
            last_seq: promoted.last_seq + 50,
            archived_seq: promoted.archived_seq + 50
        }

      assert :ok = SessionQuorum.apply_replica(stale, [])

      assert {:ok, %Session{} = final} = SessionQuorum.get_session(id)
      assert final.epoch == promoted.epoch
      assert final.last_seq == promoted.last_seq
      assert final.archived_seq == promoted.archived_seq
    end
  end

  describe "session runtime recovery" do
    test "bootstraps an archived owner session when hot state is gone everywhere local" do
      id = unique_id("ses-archive-bootstrap")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      assert {:ok, %{seq: 1}} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "archived"},
                 actor: "agent:test"
               })

      flush_archive_until(id, 1)

      assert {:ok, %Session{id: ^id, last_seq: 1, archived_seq: 1}} =
               SessionCatalog.get_session(id)

      :ok = SessionQuorum.stop_session(id)
      :ok = SessionStore.delete_session(id)

      eventually(
        fn ->
          assert [] == Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
        end,
        timeout: 1_000
      )

      assert {:ok, %Session{id: ^id, last_seq: 1, archived_seq: 1}} =
               SessionQuorum.get_session(id)
    end

    test "freezes idle owner logs and hydrates them on the next access" do
      original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

      original_idle_check_interval =
        Application.get_env(:starcite, :session_log_idle_check_interval_ms)

      on_exit(fn ->
        restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
        restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
      end)

      Application.put_env(:starcite, :session_log_idle_timeout_ms, 120)
      Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)

      Phoenix.PubSub.subscribe(Starcite.PubSub, "lifecycle:acme")

      id = unique_id("ses-idle-freeze")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      [{pid, _value}] = Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
      ref = Process.monitor(pid)

      flush_lifecycle_events()

      assert_receive {:session_lifecycle, event}, 2_000
      assert event == %{kind: "session.freezing", session_id: id, tenant_id: "acme"}

      assert_receive {:session_lifecycle, event}, 2_000
      assert event == %{kind: "session.frozen", session_id: id, tenant_id: "acme"}

      assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 2_000
      assert reason in [:normal, :shutdown]

      eventually(
        fn ->
          assert [] == Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
        end,
        timeout: 1_000
      )

      assert {:ok, %Session{id: ^id}} = SessionQuorum.get_session(id)

      assert_receive {:session_lifecycle, event}, 1_000
      assert event == %{kind: "session.hydrating", session_id: id, tenant_id: "acme"}

      assert_receive {:session_lifecycle, event}, 1_000
      assert event == %{kind: "session.activated", session_id: id, tenant_id: "acme"}

      [{new_pid, _value}] = Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
      assert is_pid(new_pid)
      refute new_pid == pid
    end

    test "does not treat archive cursor reads as session activity" do
      original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

      original_idle_check_interval =
        Application.get_env(:starcite, :session_log_idle_check_interval_ms)

      on_exit(fn ->
        restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
        restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
      end)

      Application.put_env(:starcite, :session_log_idle_timeout_ms, 180)
      Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)

      Phoenix.PubSub.subscribe(Starcite.PubSub, "lifecycle:acme")

      id = unique_id("ses-idle-archive-read")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")
      flush_lifecycle_events()

      Process.sleep(40)
      assert {:ok, 0} = SessionQuorum.fetch_archived_seq(id)

      Process.sleep(40)
      assert {:ok, 0} = SessionQuorum.fetch_archived_seq(id)

      Process.sleep(40)
      assert {:ok, 0} = SessionQuorum.fetch_archived_seq(id)

      assert_receive {:session_lifecycle, event}, 120
      assert event == %{kind: "session.freezing", session_id: id, tenant_id: "acme"}

      assert_receive {:session_lifecycle, event}, 120
      assert event == %{kind: "session.frozen", session_id: id, tenant_id: "acme"}
    end

    test "freezes once the archive cursor catches up after a dirty interval" do
      original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

      original_idle_check_interval =
        Application.get_env(:starcite, :session_log_idle_check_interval_ms)

      on_exit(fn ->
        restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
        restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
      end)

      Application.put_env(:starcite, :session_log_idle_timeout_ms, 120)
      Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)

      Phoenix.PubSub.subscribe(Starcite.PubSub, "lifecycle:acme")

      id = unique_id("ses-idle-ack")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")
      flush_lifecycle_events()

      assert {:ok, %{seq: 1, last_seq: 1}} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "makes session dirty"},
                 actor: "agent:test"
               })

      Process.sleep(250)

      refute_receive {:session_lifecycle, %{kind: "session.freezing", session_id: ^id}},
                     200

      assert {:ok, %{archived_seq: 1}} = WritePath.ack_archived(id, 1)

      assert_receive {:session_lifecycle, event}, 2_000
      assert event == %{kind: "session.freezing", session_id: id, tenant_id: "acme"}

      assert_receive {:session_lifecycle, event}, 2_000
      assert event == %{kind: "session.frozen", session_id: id, tenant_id: "acme"}
    end

    test "does not freeze dirty owner logs while unarchived events remain" do
      original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

      original_idle_check_interval =
        Application.get_env(:starcite, :session_log_idle_check_interval_ms)

      on_exit(fn ->
        restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
        restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
      end)

      Application.put_env(:starcite, :session_log_idle_timeout_ms, 120)
      Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)

      Phoenix.PubSub.subscribe(Starcite.PubSub, "lifecycle:acme")

      id = unique_id("ses-idle-dirty")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")
      flush_lifecycle_events()

      assert {:ok, %{seq: 1, last_seq: 1}} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "keeps hot tail dirty"},
                 actor: "agent:test"
               })

      Process.sleep(250)

      refute_receive {:session_lifecycle, %{kind: "session.freezing", session_id: ^id}},
                     200

      refute_receive {:session_lifecycle, %{kind: "session.frozen", session_id: ^id}},
                     50

      assert match?(
               [{pid, _value}] when is_pid(pid),
               Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
             )

      assert {:ok, %Session{last_seq: 1, archived_seq: 0}} = SessionQuorum.get_session(id)
    end

    test "does not freeze follower logs on idle" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
      original_idle_timeout = Application.get_env(:starcite, :session_log_idle_timeout_ms)

      original_idle_check_interval =
        Application.get_env(:starcite, :session_log_idle_check_interval_ms)

      peer = :"runtime-lifecycle-peer@127.0.0.1"
      id = unique_id("ses-follower-idle")

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
        restore_app_env(:session_log_idle_timeout_ms, original_idle_timeout)
        restore_app_env(:session_log_idle_check_interval_ms, original_idle_check_interval)
      end)

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
      Application.put_env(:starcite, :routing_replication_factor, 2)
      Application.put_env(:starcite, :session_log_idle_timeout_ms, 120)
      Application.put_env(:starcite, :session_log_idle_check_interval_ms, 20)
      Starcite.Runtime.TestHelper.reset()

      assert :ok =
               put_assignment(id, %{
                 owner: peer,
                 epoch: 1,
                 replicas: [peer, Node.self()],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               })

      Phoenix.PubSub.subscribe(Starcite.PubSub, "lifecycle:acme")

      session = %Session{Session.new(id, tenant_id: "acme") | epoch: 1}
      assert :ok = SessionStore.put_session(session)
      assert {:ok, %Session{id: ^id}} = SessionQuorum.get_session(id)

      refute_receive {:session_lifecycle, _event}, 200

      Process.sleep(250)

      refute_receive {:session_lifecycle, %{kind: "session.freezing", session_id: ^id}},
                     200

      refute_receive {:session_lifecycle, %{kind: "session.frozen", session_id: ^id}},
                     50

      assert match?(
               [{pid, _value}] when is_pid(pid),
               Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
             )
    end

    test "recovers state after owner crash and restart" do
      id = unique_id("ses-failover")

      capture_log(fn ->
        {:ok, _} = WritePath.create_session(id: id)

        {:ok, first} =
          append_event(id, %{
            type: "content",
            payload: %{text: "before crash"},
            actor: "agent:1"
          })

        assert first.seq == 1

        [{pid, _value}] = Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          2_000 -> flunk("session runtime did not die")
        end

        eventually(
          fn ->
            assert match?(
                     [{owner_pid, _}] when is_pid(owner_pid),
                     Registry.lookup(Starcite.DataPlane.SessionRuntimeRegistry, id)
                   )
          end,
          timeout: 3_000
        )

        {:ok, second} =
          append_event(id, %{
            type: "content",
            payload: %{text: "after crash"},
            actor: "agent:1"
          })

        assert second.seq >= 2

        {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
        texts = Enum.map(events, &Map.get(&1.payload, :text))

        assert "before crash" in texts
        assert "after crash" in texts
      end)
    end
  end

  describe "Concurrent access" do
    test "multiple sessions can be created concurrently" do
      ids =
        for i <- 1..25 do
          "ses-create-concurrent-#{i}-#{System.unique_integer([:positive, :monotonic])}"
        end

      results =
        ids
        |> Task.async_stream(
          fn id -> WritePath.create_session(id: id, tenant_id: "acme", title: "Draft") end,
          max_concurrency: 25,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, %{id: _}}, &1))

      Enum.each(ids, fn id ->
        assert {:ok, session} = ReadPath.get_session(id)
        assert session.id == id
      end)
    end

    test "concurrent creates for the same session id only commit once" do
      id = unique_id("ses-create-race")

      results =
        1..20
        |> Task.async_stream(
          fn _ ->
            WritePath.create_session(id: id, tenant_id: "acme", title: "Race")
          end,
          max_concurrency: 20,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      successes = Enum.count(results, &match?({:ok, %{id: ^id}}, &1))
      failures = Enum.count(results, &match?({:error, :session_exists}, &1))

      assert successes >= 1
      assert successes + failures == 20

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.id == id
      assert session.last_seq == 0
      assert {:error, :session_exists} = WritePath.create_session(id: id, tenant_id: "acme")
    end

    test "multiple sessions can append concurrently" do
      ids =
        for i <- 1..10 do
          id = "ses-concurrent-#{i}"
          {:ok, _} = WritePath.create_session(id: id)
          id
        end

      tasks =
        Enum.map(ids, fn id ->
          Task.async(fn ->
            append_event(id, %{
              type: "content",
              payload: %{text: "hello"},
              actor: "agent:1"
            })
          end)
        end)

      results = Task.await_many(tasks, 5_000)

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end

    test "one session can batch many concurrent appends and keep contiguous sequence order" do
      id = unique_id("ses-hot-lane")
      {:ok, _} = WritePath.create_session(id: id)

      results =
        1..100
        |> Task.async_stream(
          fn n ->
            WritePath.append_event(id, %{
              type: "content",
              payload: %{text: "m#{n}"},
              actor: "agent:1",
              producer_id: "writer:#{n}",
              producer_seq: 1
            })
          end,
          max_concurrency: 100,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, _}, &1))

      seqs =
        results
        |> Enum.map(fn {:ok, reply} -> reply.seq end)
        |> Enum.sort()

      assert seqs == Enum.to_list(1..100)

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 200)
      assert Enum.map(events, & &1.seq) == Enum.to_list(1..100)
    end

    test "one session can batch many concurrent append_events requests and keep contiguous sequence order" do
      id = unique_id("ses-hot-batches")
      {:ok, _} = WritePath.create_session(id: id)

      results =
        1..50
        |> Task.async_stream(
          fn n ->
            WritePath.append_events(id, [
              %{
                type: "content",
                payload: %{text: "m#{n}-a"},
                actor: "agent:1",
                producer_id: "writer-batch:#{n}:a",
                producer_seq: 1
              },
              %{
                type: "content",
                payload: %{text: "m#{n}-b"},
                actor: "agent:1",
                producer_id: "writer-batch:#{n}:b",
                producer_seq: 1
              }
            ])
          end,
          max_concurrency: 50,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, %{results: [_ | _]}}, &1))

      seqs =
        results
        |> Enum.flat_map(fn {:ok, %{results: replies}} ->
          Enum.map(replies, & &1.seq)
        end)
        |> Enum.sort()

      assert seqs == Enum.to_list(1..100)

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 200)
      assert Enum.map(events, & &1.seq) == Enum.to_list(1..100)
    end

    test "expected_seq barrier requests stay coherent under concurrent batched appends" do
      id = unique_id("ses-expected-seq-barrier")
      {:ok, _} = WritePath.create_session(id: id)

      barrier_task =
        Task.async(fn ->
          append_event(
            id,
            %{
              type: "content",
              payload: %{text: "barrier"},
              actor: "agent:1",
              producer_id: "writer:barrier",
              producer_seq: 1
            },
            expected_seq: 0
          )
        end)

      concurrent_results =
        1..50
        |> Task.async_stream(
          fn n ->
            WritePath.append_event(id, %{
              type: "content",
              payload: %{text: "m#{n}"},
              actor: "agent:1",
              producer_id: "writer:#{n}",
              producer_seq: 1
            })
          end,
          max_concurrency: 50,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      barrier_result = Task.await(barrier_task, 5_000)
      results = [barrier_result | concurrent_results]

      assert Enum.all?(results, fn
               {:ok, _reply} -> true
               {:error, {:expected_seq_conflict, 0, _last_seq}} -> true
               _other -> false
             end)

      successes =
        Enum.filter(results, fn
          {:ok, _reply} -> true
          _other -> false
        end)

      conflicts =
        Enum.filter(results, fn
          {:error, {:expected_seq_conflict, 0, _last_seq}} -> true
          _other -> false
        end)

      assert length(successes) + length(conflicts) == 51
      assert length(conflicts) in [0, 1]

      case barrier_result do
        {:ok, reply} ->
          assert reply.seq == 1

        {:error, {:expected_seq_conflict, 0, last_seq}} ->
          assert last_seq >= 1
      end

      successful_seqs =
        successes
        |> Enum.map(fn {:ok, reply} -> reply.seq end)
        |> Enum.sort()

      assert successful_seqs == Enum.to_list(1..length(successes))

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.last_seq == length(successes)

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
      assert Enum.map(events, & &1.seq) == Enum.to_list(1..length(successes))
    end

    test "concurrent replay conflicts commit one event and reject the conflicting append" do
      id = unique_id("ses-hot-conflict")
      {:ok, _} = WritePath.create_session(id: id)

      results =
        [
          %{
            type: "state",
            payload: %{state: "running"},
            actor: "agent:1",
            producer_id: "writer-conflict",
            producer_seq: 1
          },
          %{
            type: "state",
            payload: %{state: "completed"},
            actor: "agent:1",
            producer_id: "writer-conflict",
            producer_seq: 1
          }
        ]
        |> Task.async_stream(
          &WritePath.append_event(id, &1),
          max_concurrency: 2,
          timeout: 5_000,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.count(results, &match?({:ok, %{seq: 1, deduped: false}}, &1)) == 1
      assert Enum.count(results, &match?({:error, :producer_replay_conflict}, &1)) == 1

      assert {:ok, session} = ReadPath.get_session(id)
      assert session.last_seq == 1

      assert {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 10)
      assert Enum.map(events, & &1.seq) == [1]
      assert hd(events).payload.state in ["running", "completed"]
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

  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
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

  defp insert_cold_rows(session_id, events) when is_binary(session_id) and is_list(events) do
    base_rows =
      Enum.map(events, fn event ->
        %{
          session_id: session_id,
          seq: event.seq,
          type: event.type,
          payload: event.payload,
          actor: event.actor,
          producer_id: event.producer_id,
          producer_seq: event.producer_seq,
          tenant_id: event.tenant_id,
          source: event.source,
          metadata: event.metadata,
          refs: event.refs,
          idempotency_key: event.idempotency_key,
          inserted_at: as_datetime(event.inserted_at)
        }
      end)

    rows =
      Enum.zip(base_rows, events)
      |> Enum.map(fn {row, event} -> Map.put(row, :tenant_id, event_tenant_id!(event)) end)

    assert {:ok, inserted} = EventArchive.write_events(rows)
    assert inserted == length(rows)
  end

  defp flush_archive_until(session_id, archived_seq)
       when is_binary(session_id) and session_id != "" and is_integer(archived_seq) and
              archived_seq >= 0 do
    eventually(
      fn ->
        send(archive_process_name(), :flush_tick)

        assert {:ok,
                %Session{id: ^session_id, archived_seq: ^archived_seq, last_seq: ^archived_seq}} =
                 SessionCatalog.get_session(session_id)
      end,
      timeout: 3_000
    )
  end

  defp archive_process_name do
    Application.get_env(:starcite, :archive_name, Starcite.Archive)
  end

  defp event_tenant_id!(%{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_tenant_id!(event) when is_map(event) do
    raise ArgumentError,
          "event row missing tenant_id: #{inspect(Map.take(event, [:seq, :producer_id, :producer_seq]))}"
  end

  defp wait_for_process_restart(name, previous_pid, timeout_ms)
       when is_atom(name) and is_pid(previous_pid) and is_integer(timeout_ms) and timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_process_restart(name, previous_pid, deadline_ms)
  end

  defp do_wait_for_process_restart(name, previous_pid, deadline_ms)
       when is_atom(name) and is_pid(previous_pid) and is_integer(deadline_ms) do
    case Process.whereis(name) do
      pid when is_pid(pid) and pid != previous_pid ->
        pid

      _other ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          flunk("timed out waiting for #{inspect(name)} to restart")
        else
          Process.sleep(50)
          do_wait_for_process_restart(name, previous_pid, deadline_ms)
        end
    end
  end

  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp as_datetime(%DateTime{} = value), do: value

  defp flush_lifecycle_events do
    receive do
      {:session_lifecycle, _event} -> flush_lifecycle_events()
    after
      0 -> :ok
    end
  end

  defp restore_app_env(key, nil), do: Application.delete_env(:starcite, key)
  defp restore_app_env(key, value), do: Application.put_env(:starcite, key, value)
end
