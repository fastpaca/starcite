defmodule Starcite.DataPlane.SessionQuorumTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.SessionCatalog
  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.SessionQuorum
  alias Starcite.Routing.Store
  alias Starcite.Session

  setup do
    original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
    handler_id = "session-replication-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :replication, :session],
        fn _event, measurements, metadata, pid ->
          send(pid, {:session_replication_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
      Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      :telemetry.detach(handler_id)
    end)

    :ok
  end

  test "succeeds locally when no remote acknowledgements are required" do
    Application.put_env(:starcite, :cluster_node_ids, [Node.self()])
    Application.put_env(:starcite, :routing_replication_factor, 1)

    session = Session.new("ses-repl-local")

    assert :ok = SessionQuorum.replicate_state(session, [])

    assert_receive {:session_replication_event, measurements, metadata}, 1_000
    assert measurements.count == 1
    assert measurements.standby_count == 0
    assert measurements.required_remote_acks == 0
    assert measurements.successful_remote_acks == 0
    assert measurements.failure_count == 0
    assert is_integer(measurements.duration_ms)
    assert measurements.duration_ms >= 0

    assert metadata.session_id == "ses-repl-local"
    assert metadata.tenant_id == "service"
    assert metadata.outcome == :ok
    assert metadata.failure_reason == :none
  end

  test "returns quorum error when required remote acknowledgement cannot be obtained" do
    missing = :"missing-replica@127.0.0.1"
    session_id = "ses-repl-missing"

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), missing])
    Application.put_env(:starcite, :routing_replication_factor, 2)

    session = Session.new(session_id)

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: Node.self(),
                 epoch: 1,
                 replicas: [Node.self(), missing],
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    assert {:error, {:replication_quorum_not_met, details}} =
             SessionQuorum.replicate_state(session, [])

    assert details.required_remote_acks == 1
    assert details.successful_remote_acks == 0
    assert details.failures != []

    assert_receive {:session_replication_event, measurements, metadata}, 1_000
    assert measurements.count == 1
    assert measurements.standby_count == 1
    assert measurements.required_remote_acks == 1
    assert measurements.successful_remote_acks == 0
    assert measurements.failure_count >= 1
    assert is_integer(measurements.duration_ms)
    assert measurements.duration_ms >= 0

    assert metadata.session_id == "ses-repl-missing"
    assert metadata.tenant_id == "service"
    assert metadata.outcome == :quorum_not_met
    assert metadata.failure_reason in [:badrpc, :timeout, :error]
  end
end

defmodule Starcite.DataPlane.SessionQuorumDistributedTest do
  use ExUnit.Case, async: false

  alias Phoenix.PubSub
  alias Starcite.Archive.SessionCatalog
  alias Starcite.Auth.Principal
  alias Starcite.DataPlane.{CursorUpdate, EventStore, SessionQuorum, SessionStore}
  alias Starcite.Operations
  alias Starcite.Routing.Store
  alias Starcite.Session
  alias Starcite.WritePath
  alias Starcite.TestSupport.DistributedPeerDataPlane

  @moduletag :distributed

  unless Node.alive?() do
    @moduletag skip: "requires distributed node (run tests with --sname or --name)"
  end

  setup_all do
    original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)
    original_routing_store_dir = Application.get_env(:starcite, :routing_store_dir)

    routing_store_dir =
      Path.join(
        System.tmp_dir!(),
        "starcite-dist-#{System.unique_integer([:positive, :monotonic])}"
      )

    peers = start_peers(3)
    peer_nodes = Enum.map(peers, fn {_pid, node} -> node end)
    cluster_nodes = [Node.self() | peer_nodes]

    Application.put_env(:starcite, :routing_store_dir, routing_store_dir)
    Application.put_env(:starcite, :cluster_node_ids, cluster_nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)

    Enum.with_index(peers, 2)
    |> Enum.each(fn {{_peer_pid, peer_node}, expected_cluster_size} ->
      true = Node.connect(peer_node)
      :ok = add_code_paths(peer_node)
      :ok = configure_peer(peer_node, cluster_nodes, routing_store_dir)
      :ok = start_peer_data_plane(peer_node)
      :ok = wait_for_cluster_size(expected_cluster_size)
    end)

    on_exit(fn ->
      clear_local_data_plane()

      Enum.each(peers, fn {peer_pid, peer_node} ->
        _ = stop_peer_data_plane(peer_node)
        _ = stop_peer(peer_pid)
      end)

      restore_env(:routing_store_dir, original_routing_store_dir)
      restore_env(:cluster_node_ids, original_cluster_node_ids)
      restore_env(:routing_replication_factor, original_replication_factor)
      File.rm_rf(routing_store_dir)
    end)

    {:ok, peers: peers}
  end

  setup %{peers: peers} do
    clear_local_data_plane()
    :ok = Store.clear()
    :ok = Store.renew_local_lease()

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      :ok = start_peer_data_plane(peer_node)
      sync_peer_lease_ttl(peer_node)
    end)

    :ok = wait_for_cluster_size(length(peers) + 1)

    Enum.each(peers, fn {_peer_pid, peer_node} ->
      assert :ok = :rpc.call(peer_node, DistributedPeerDataPlane, :clear, [], 15_000)
      assert :ok = :rpc.call(peer_node, Store, :renew_local_lease, [], 5_000)
    end)

    :ok
  end

  test "replicates to every live standby before returning", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-repl-dist-#{System.unique_integer([:positive, :monotonic])}"
    input = append_input("replicated-producer", 1)
    seed_session = Session.new(session_id)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = SessionQuorum.replicate_state(seed_session, [])
    assert {:appended, updated_session, event} = Session.append_event(seed_session, input)

    replicated_session = %Session{updated_session | epoch: 7}
    assert :ok = SessionQuorum.replicate_state(replicated_session, [event])

    eventually(fn ->
      assert_peer_session(peer_a, replicated_session)
      assert_peer_session(peer_b, replicated_session)
      assert_peer_events(peer_a, session_id, replicated_session.epoch, [1])
      assert_peer_events(peer_b, session_id, replicated_session.epoch, [1])
    end)
  end

  test "log bootstrap prefers fresher peer state over stale local cache", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-repl-bootstrap-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = %Session{Session.new(session_id) | epoch: 1}

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), seed_session, [])

    assert {:ok, reply_one} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 1))

    assert reply_one.seq == 1

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 1
    end)

    assert {:ok, session_one} = SessionQuorum.get_session(session_id)
    [event_one] = EventStore.from_cursor(session_id, 0, 1)
    assert :ok = :rpc.call(peer_b, SessionQuorum, :stop_session, [session_id], 5_000)
    assert :ok = seed_peer_cache(peer_b, session_one, [event_one])

    put_assignment(session_id, Node.self(), [Node.self(), peer_a])

    eventually(fn ->
      assert {:ok, session_one} =
               :rpc.call(peer_b, SessionStore, :get_session_cached, [session_id], 5_000)

      assert session_one.last_seq == 1
      assert_stored_session(peer_b, session_one)
      assert_peer_events(peer_b, session_id, session_one.epoch, [1])
    end)

    assert {:ok, reply_two} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 2))

    assert reply_two.seq == 2

    assert {:ok, reply_three} =
             WritePath.append_event(session_id, append_input("bootstrap-writer", 3))

    assert reply_three.seq == 3

    assert {:ok, session_three} = SessionQuorum.get_session(session_id)
    assert session_three.last_seq == 3

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])

    eventually(fn ->
      assert {:ok, ^session_three} =
               :rpc.call(peer_b, SessionQuorum, :get_session, [session_id], 5_000)

      assert_stored_session(peer_b, session_three)
      assert_peer_events(peer_b, session_id, session_three.epoch, [1, 2, 3])
    end)

    assert_peer_session(peer_a, session_three)
  end

  test "empty peer bootstrap is ignored when the session catalog row was never persisted", %{
    peers: peers
  } do
    [peer_a | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-bootstrap-empty-missing-catalog-#{System.unique_integer([:positive, :monotonic])}"
    principal = %Principal{tenant_id: "acme", id: "creator-1", type: :service}
    session = Session.new(session_id, tenant_id: "acme", creator_principal: principal)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a])
    assert :ok = seed_peer_cache(peer_a, session, [])

    assert {:error, :session_not_found} = SessionQuorum.get_session(session_id)
  end

  test "empty peer bootstrap remains available when the session catalog row exists", %{
    peers: peers
  } do
    [peer_a | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-bootstrap-empty-persisted-catalog-#{System.unique_integer([:positive, :monotonic])}"
    principal = %Principal{tenant_id: "acme", id: "creator-1", type: :service}
    session = Session.new(session_id, tenant_id: "acme", creator_principal: principal)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a])
    assert :ok = seed_peer_cache(peer_a, session, [])
    assert :ok = SessionCatalog.persist_created(Session.to_map(session), principal, "acme", %{})

    assert {:ok, bootstrapped} = SessionQuorum.get_session(session_id)
    assert bootstrapped.id == session_id
    assert bootstrapped.last_seq == 0
    assert bootstrapped.archived_seq == 0
  end

  test "manual drain hands ownership to a replica and waits for the node to drain", %{
    peers: peers
  } do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-drain-dist-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), seed_session, [])

    {:ok, append_reply} = WritePath.append_event(session_id, append_input("drain-writer", 1))
    assert append_reply.seq == 1

    assert :ok = Operations.drain_node(Node.self())
    assert :ok = Operations.wait_local_drained(5_000)

    eventually(fn ->
      assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
               Store.get_assignment(session_id, favor: :consistency)

      assert owner in [peer_a, peer_b]
      assert false == SessionQuorum.local_owner?(session_id)
      assert true == :rpc.call(owner, SessionQuorum, :local_owner?, [session_id], 5_000)
    end)

    {:ok, routed_reply} = WritePath.append_event(session_id, append_input("drain-writer", 2))
    assert routed_reply.seq == 2

    eventually(fn ->
      assert {:ok, session} = :rpc.call(peer_a, SessionQuorum, :get_session, [session_id], 5_000)
      assert session.last_seq == 2

      events = :rpc.call(peer_a, EventStore, :from_cursor, [session_id, 0, 10], 5_000)

      assert is_list(events)
      assert Enum.map(events, & &1.seq) == [1, 2]
      assert Enum.map(events, & &1.epoch) == [1, 2]
    end)
  end

  test "lease expiry triggers failover without using nodedown as authority", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-lease-failover-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)
    original_ttl = Application.get_env(:starcite, :routing_lease_ttl_ms)

    on_exit(fn ->
      restore_env(:routing_lease_ttl_ms, original_ttl)

      Enum.each([peer_a, peer_b], fn peer_node ->
        _ = restore_peer_env(peer_node, :routing_lease_ttl_ms, original_ttl)
      end)
    end)

    Application.put_env(:starcite, :routing_lease_ttl_ms, 300)

    Enum.each([peer_a, peer_b], fn peer_node ->
      assert :ok =
               :rpc.call(
                 peer_node,
                 Application,
                 :put_env,
                 [:starcite, :routing_lease_ttl_ms, 300],
                 5_000
               )
    end)

    put_assignment(session_id, peer_a, [peer_a, Node.self(), peer_b])
    assert :ok = seed_replica_set(peer_a, seed_session, [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("lease-writer", 1)],
               5_000
             )

    assert reply.seq == 1

    assert :ok = stop_peer_data_plane(peer_a)

    eventually(
      fn ->
        assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
                 Store.get_assignment(session_id, favor: :consistency)

        assert owner in [Node.self(), peer_b]
        assert owner != peer_a
      end,
      timeout: 6_000
    )
  end

  test "lease expiry promotes a moving target owner and preserves writes", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-moving-failover-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(session_id)
    original_ttl = Application.get_env(:starcite, :routing_lease_ttl_ms)

    on_exit(fn ->
      restore_env(:routing_lease_ttl_ms, original_ttl)

      Enum.each([peer_a, peer_b], fn peer_node ->
        _ = restore_peer_env(peer_node, :routing_lease_ttl_ms, original_ttl)
      end)
    end)

    Application.put_env(:starcite, :routing_lease_ttl_ms, 300)

    Enum.each([peer_a, peer_b], fn peer_node ->
      assert :ok =
               :rpc.call(
                 peer_node,
                 Application,
                 :put_env,
                 [:starcite, :routing_lease_ttl_ms, 300],
                 5_000
               )
    end)

    assert :ok = seed_replica_set(peer_a, seed_session, [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("moving-writer", 1)],
               5_000
             )

    assert reply.seq == 1

    assert :ok =
             seed_moving_assignment(session_id, peer_a, Node.self(), [peer_a, Node.self(), peer_b])

    assert :ok = stop_peer_data_plane(peer_a)

    eventually(
      fn ->
        assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
                 Store.get_assignment(session_id, favor: :consistency)

        assert owner == Node.self()
        assert true == SessionQuorum.local_owner?(session_id)
      end,
      timeout: 6_000
    )

    assert {:ok, routed_reply} =
             WritePath.append_event(session_id, append_input("moving-writer", 2))

    assert routed_reply.seq == 2

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 2

      assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
      assert assignment.status == :active
      assert assignment.owner == Node.self()
      assert assignment.owner in assignment.replicas
    end)
  end

  test "creates and appends continue while an expired owner is being failed over", %{peers: peers} do
    [peer_a, peer_b, peer_c] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    failover_session_id = "ses-failover-live-#{System.unique_integer([:positive, :monotonic])}"
    seed_session = Session.new(failover_session_id)
    now_ms = System.system_time(:millisecond)

    put_assignment(failover_session_id, peer_a, [peer_a, Node.self(), peer_b])
    assert :ok = seed_replica_set(peer_a, seed_session, [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [failover_session_id, append_input("failover-live-writer", 1)],
               5_000
             )

    assert reply.seq == 1
    assert :ok = stop_peer_data_plane(peer_a)

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:nodes, peer_a],
               %{
                 status: :ready,
                 lease_until_ms: now_ms - 1,
                 owned_session_count: 1,
                 updated_at_ms: now_ms
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    create_append_task =
      Task.async(fn ->
        1..24
        |> Task.async_stream(
          fn i ->
            session_id =
              "ses-failover-live-claim-#{i}-#{System.unique_integer([:positive, :monotonic])}"

            assert {:ok, _session} =
                     eventually_value(
                       fn ->
                         case WritePath.create_session(id: session_id, tenant_id: "acme") do
                           {:ok, _session} = ok ->
                             ok

                           {:error, :routing_store_unavailable} ->
                             :retry

                           {:error, {:routing_rpc_failed, _, _reason}} ->
                             :retry

                           {:timeout, _reason} ->
                             :retry

                           _other ->
                             :retry
                         end
                       end,
                       timeout: 15_000,
                       interval: 50
                     )

            assert {:ok, append_reply} =
                     WritePath.append_event(session_id, append_input(session_id, 1))

            {session_id, append_reply.seq}
          end,
          max_concurrency: 24,
          timeout: 7_500,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)
      end)

    failover_task = Task.async(fn -> Store.reassign_sessions_from(peer_a) end)

    create_results = Task.await(create_append_task, 10_000)
    assert {:ok, 1} = Task.await(failover_task, 5_000)

    assert Enum.all?(create_results, fn {_session_id, seq} -> seq == 1 end)

    Enum.each(create_results, fn {session_id, _seq} ->
      assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
      assert assignment.owner in [Node.self(), peer_b, peer_c]
      assert assignment.status == :active
      assert length(assignment.replicas) == Starcite.Routing.Topology.replication_factor()
      assert assignment.owner in assignment.replicas
      assert assignment.replicas == Enum.uniq(assignment.replicas)
    end)

    eventually(
      fn ->
        assert {:ok, %{owner: owner, epoch: 2, status: :active}} =
                 Store.get_assignment(failover_session_id, favor: :consistency)

        assert owner in [Node.self(), peer_b, peer_c]
        assert owner != peer_a
      end,
      timeout: 6_000
    )

    assert {:ok, routed_reply} =
             WritePath.append_event(failover_session_id, append_input("failover-live-writer", 2))

    assert routed_reply.seq == 2
  end

  @tag :chaos
  test "rolling drain and restart preserves session invariants under live traffic", %{
    peers: peers
  } do
    [peer_a, peer_b, peer_c] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    churn_nodes = [peer_a, peer_b]

    seeded_session_ids =
      for i <- 1..12 do
        session_id =
          "ses-chaos-seed-#{i}-#{System.unique_integer([:positive, :monotonic])}"

        assert {:ok, _session} = WritePath.create_session(id: session_id, tenant_id: "acme")
        assert {:ok, reply} = WritePath.append_event(session_id, append_input(session_id, 1))
        assert reply.seq == 1
        session_id
      end

    churn_session_ids =
      Enum.flat_map(churn_nodes, fn churn_node ->
        create_task =
          Task.async(fn ->
            1..12
            |> Task.async_stream(
              fn i ->
                session_id =
                  "ses-chaos-live-#{sanitize_node_name(churn_node)}-#{i}-#{System.unique_integer([:positive, :monotonic])}"

                assert {:ok, _session} =
                         WritePath.create_session(id: session_id, tenant_id: "acme")

                assert {:ok, reply} =
                         eventually_value(
                           fn ->
                             case WritePath.append_event(session_id, append_input(session_id, 1)) do
                               {:ok, _reply} = ok ->
                                 ok

                               {:error, {:routing_rpc_failed, _, _reason}} ->
                                 :retry

                               {:timeout, _reason} ->
                                 :retry

                               _other ->
                                 :retry
                             end
                           end,
                           timeout: 30_000,
                           interval: 50
                         )

                assert reply.seq == 1
                session_id
              end,
              max_concurrency: 12,
              timeout: 30_000,
              ordered: false
            )
            |> Enum.map(fn {:ok, session_id} -> session_id end)
          end)

        assert :ok = :rpc.call(churn_node, Operations, :drain_node, [churn_node], 5_000)
        assert :ok = :rpc.call(churn_node, Operations, :wait_local_drained, [30_000], 35_000)
        assert :ok = stop_peer_data_plane(churn_node)
        assert :ok = start_peer_data_plane(churn_node, wait_for_runtime: false)
        sync_peer_lease_ttl(churn_node)
        :ok = wait_for_cluster_size(length(peers) + 1)
        assert :ok = :rpc.call(churn_node, Operations, :wait_local_ready, [30_000], 35_000)

        Task.await(create_task, 60_000)
      end)

    all_session_ids = seeded_session_ids ++ churn_session_ids
    ready_nodes = [Node.self(), peer_a, peer_b, peer_c]

    eventually(
      fn ->
        assert {:ok, assignments} = Store.all_assignments(:consistency)

        assert Enum.all?(all_session_ids, fn session_id ->
                 case Map.fetch(assignments, session_id) do
                   {:ok, assignment} ->
                     assignment.status == :active and
                       assignment.owner in ready_nodes and
                       Enum.all?(assignment.replicas, &(&1 in ready_nodes))

                   :error ->
                     false
                 end
               end)

        assert [] ==
                 Enum.filter(assignments, fn {session_id, assignment} ->
                   session_id in all_session_ids and assignment.status != :active
                 end)
      end,
      timeout: 20_000,
      interval: 100
    )

    Enum.each(all_session_ids, fn session_id ->
      :ok = PubSub.subscribe(Starcite.PubSub, CursorUpdate.topic(session_id))
    end)

    Enum.each(all_session_ids, fn session_id ->
      assert {:ok, session} = Starcite.ReadPath.get_session_routed(session_id, true)
      assert session.last_seq == 1

      assert {:ok, reply} =
               WritePath.append_event(
                 session_id,
                 append_input("final-#{session_id}", 1)
               )

      assert reply.seq == 2
    end)

    assert_cursor_updates(all_session_ids, 2)

    eventually(fn ->
      Enum.each(all_session_ids, fn session_id ->
        assert {:ok, session} = Starcite.ReadPath.get_session_routed(session_id, true)
        assert session.last_seq == 2

        assert_hot_owner_sees_latest(session_id, 2)
      end)
    end)
  end

  test "restart during drain keeps the node out of the ready set while drain work remains", %{
    peers: peers
  } do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-restart-drain-#{System.unique_integer([:positive, :monotonic])}"

    put_assignment(session_id, peer_a, [peer_a, Node.self(), peer_b])

    assert :ok = :rpc.call(peer_a, Operations, :drain_node, [peer_a], 5_000)

    eventually(fn ->
      assert :draining == Store.node_status(peer_a)
    end)

    assert :ok = stop_peer_data_plane(peer_a)
    assert :ok = start_peer_data_plane(peer_a)
    sync_peer_lease_ttl(peer_a)
    :ok = wait_for_cluster_size(length(peers) + 1)

    eventually(fn ->
      assert :draining == :rpc.call(peer_a, Operations, :node_status, [peer_a], 5_000)
      assert false == :rpc.call(peer_a, Operations, :local_ready, [], 5_000)
      refute peer_a in Store.ready_nodes()
    end)
  end

  test "stale former owner rejects appends after authoritative ownership moves", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-stale-owner-#{System.unique_integer([:positive, :monotonic])}"

    initial_assignment = %{
      owner: peer_a,
      epoch: 1,
      replicas: [peer_a, Node.self(), peer_b],
      status: :active,
      updated_at_ms: System.system_time(:millisecond)
    }

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               initial_assignment,
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    assert :ok = seed_replica_set(peer_a, Session.new(session_id), [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("stale-owner-writer", 1)],
               5_000
             )

    assert reply.seq == 1

    authoritative_assignment = %{
      owner: Node.self(),
      epoch: 2,
      replicas: [Node.self(), peer_a, peer_b],
      status: :active,
      updated_at_ms: System.system_time(:millisecond)
    }

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               authoritative_assignment,
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    stale_assignment = initial_assignment

    assert true =
             :rpc.call(
               peer_a,
               :ets,
               :insert,
               [:starcite_routing_assignment_cache, {session_id, stale_assignment}],
               5_000
             )

    assert {:error, reason} =
             :rpc.call(
               peer_a,
               SessionQuorum,
               :append_event,
               [session_id, append_input("stale-owner-writer", 2), nil],
               5_000
             )

    assert reason in [:not_owner, :ownership_transfer_in_progress] or
             match?({:not_leader, _}, reason)

    assert {:ok, routed_reply} =
             WritePath.append_event(session_id, append_input("fresh-owner-writer", 2))

    assert routed_reply.seq == 2

    eventually(fn ->
      assert false == :rpc.call(peer_a, SessionQuorum, :local_owner?, [session_id], 5_000)
      assert true == SessionQuorum.local_owner?(session_id)
    end)
  end

  test "stale clients hammering the old owner during moving transfer do not leak writes", %{
    peers: peers
  } do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-moving-stale-owner-#{System.unique_integer([:positive, :monotonic])}"

    initial_assignment = %{
      owner: peer_a,
      epoch: 1,
      replicas: [peer_a, Node.self(), peer_b],
      status: :active,
      updated_at_ms: System.system_time(:millisecond)
    }

    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               initial_assignment,
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    assert :ok = seed_replica_set(peer_a, Session.new(session_id), [])

    assert {:ok, reply} =
             :rpc.call(
               peer_a,
               WritePath,
               :append_event,
               [session_id, append_input("moving-stale-owner", 1)],
               5_000
             )

    assert reply.seq == 1

    assert :ok =
             seed_moving_assignment(session_id, peer_a, Node.self(), [peer_a, Node.self(), peer_b])

    assert true =
             :rpc.call(
               peer_a,
               :ets,
               :insert,
               [:starcite_routing_assignment_cache, {session_id, initial_assignment}],
               5_000
             )

    results =
      1..20
      |> Task.async_stream(
        fn i ->
          :rpc.call(
            peer_a,
            SessionQuorum,
            :append_event,
            [session_id, append_input("moving-stale-owner-#{i}", 1), nil],
            5_000
          )
        end,
        max_concurrency: 20,
        timeout: 7_500,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, fn
             {:error, reason} ->
               reason in [:not_owner, :ownership_transfer_in_progress] or
                 match?({:not_leader, _}, reason)

             _other ->
               false
           end)

    assert {:ok, session} = SessionQuorum.get_session(session_id)
    assert session.last_seq == 1

    assert {:ok, events} = Starcite.ReadPath.get_events_from_cursor(session_id, 0, 10)
    assert Enum.map(events, & &1.seq) == [1]
  end

  test "owner crash after quorum success but before reply does not duplicate the append", %{
    peers: peers
  } do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-owner-crash-after-commit-#{System.unique_integer([:positive, :monotonic])}"
    attach_append_boundary_crash_handler(session_id, :after_commit_before_reply)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), Session.new(session_id), [])

    assert {:ok, reply} =
             WritePath.append_event(session_id, append_input("crash-writer", 1))

    assert reply.seq == 1
    assert reply.last_seq == 1
    assert reply.deduped

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 1
      assert_peer_events(peer_a, session_id, session.epoch, [1])
      assert_peer_events(peer_b, session_id, session.epoch, [1])
    end)

    assert {:ok, events} = Starcite.ReadPath.get_events_from_cursor(session_id, 0, 10)
    assert Enum.map(events, & &1.seq) == [1]
  end

  test "owner crash before quorum replicate does not leak the append", %{peers: peers} do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-owner-crash-before-quorum-#{System.unique_integer([:positive, :monotonic])}"
    attach_append_boundary_crash_handler(session_id, :before_quorum_replicate)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), Session.new(session_id), [])

    initial_result = WritePath.append_event(session_id, append_input("crash-before-quorum", 1))

    assert match?({:ok, _reply}, initial_result) or
             match?({:timeout, _reason}, initial_result) or
             initial_result == {:error, :session_not_found}

    assert {:ok, reply} =
             eventually_value(fn ->
               case WritePath.append_event(session_id, append_input("crash-before-quorum", 1)) do
                 {:ok, _reply} = ok -> ok
                 _other -> :retry
               end
             end)

    assert reply.seq == 1

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 1
      assert_peer_events(peer_a, session_id, session.epoch, [1])
      assert_peer_events(peer_b, session_id, session.epoch, [1])
    end)

    assert {:ok, events} = Starcite.ReadPath.get_events_from_cursor(session_id, 0, 10)
    assert Enum.map(events, & &1.seq) == [1]
  end

  test "owner timeout after commit before reply surfaces timeout without internal replay", %{
    peers: peers
  } do
    [peer_a, peer_b | _rest] = Enum.map(peers, fn {_peer_pid, peer_node} -> peer_node end)
    session_id = "ses-owner-timeout-after-commit-#{System.unique_integer([:positive, :monotonic])}"
    attach_append_boundary_delay_handler(session_id, :after_commit_before_reply, 2_500)

    put_assignment(session_id, Node.self(), [Node.self(), peer_a, peer_b])
    assert :ok = seed_replica_set(Node.self(), Session.new(session_id), [])

    assert {:timeout, :session_log_call_timeout} =
             WritePath.append_event(session_id, append_input("timeout-writer", 1))

    eventually(fn ->
      assert {:ok, session} = SessionQuorum.get_session(session_id)
      assert session.last_seq == 1
      assert_peer_events(peer_a, session_id, session.epoch, [1])
      assert_peer_events(peer_b, session_id, session.epoch, [1])
    end)

    assert {:ok, events} = Starcite.ReadPath.get_events_from_cursor(session_id, 0, 10)
    assert Enum.map(events, & &1.seq) == [1]
  end

  defp attach_append_boundary_crash_handler(session_id, stage)
       when is_binary(session_id) and session_id != "" and
              stage in [:before_quorum_replicate, :after_commit_before_reply] do
    handler_id = "append-boundary-crash-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()
    fired = start_supervised!({Agent, fn -> false end})

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :append, :boundary],
        fn _event,
           _measurements,
           metadata,
           %{test_pid: test_pid, session_id: session_id, stage: stage, fired: fired} ->
          if metadata.session_id == session_id and metadata.stage == stage do
            already_fired =
              Agent.get_and_update(fired, fn current ->
                {current, true}
              end)

            unless already_fired do
              send(test_pid, {:append_boundary_hit, stage, metadata})
              Process.exit(self(), :kill)
            end
          end
        end,
        %{test_pid: test_pid, session_id: session_id, stage: stage, fired: fired}
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp attach_append_boundary_delay_handler(session_id, stage, sleep_ms)
       when is_binary(session_id) and session_id != "" and
              stage in [:before_quorum_replicate, :after_commit_before_reply] and
              is_integer(sleep_ms) and sleep_ms > 0 do
    handler_id = "append-boundary-delay-#{System.unique_integer([:positive, :monotonic])}"
    fired = start_supervised!({Agent, fn -> false end})

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :append, :boundary],
        fn _event,
           _measurements,
           metadata,
           %{session_id: session_id, stage: stage, sleep_ms: sleep_ms, fired: fired} ->
          if metadata.session_id == session_id and metadata.stage == stage do
            already_fired =
              Agent.get_and_update(fired, fn current ->
                {current, true}
              end)

            unless already_fired do
              Process.sleep(sleep_ms)
            end
          end
        end,
        %{session_id: session_id, stage: stage, sleep_ms: sleep_ms, fired: fired}
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp start_peers(count) when is_integer(count) and count > 0 do
    Enum.map(1..count, fn _index ->
      start_named_peer(:"replication_peer_#{System.unique_integer([:positive])}")
    end)
  end

  defp start_named_peer(name) when is_atom(name) do
    start_named_peer(name, 3)
  end

  defp start_named_peer(name, attempts_left)
       when is_atom(name) and is_integer(attempts_left) and attempts_left > 1 do
    case :peer.start_link(%{name: name}) do
      {:ok, pid, node} ->
        {pid, node}

      {:error, :timeout} ->
        Process.sleep(250)
        start_named_peer(name, attempts_left - 1)
    end
  end

  defp start_named_peer(name, 1) when is_atom(name) do
    {:ok, pid, node} = :peer.start_link(%{name: name})
    {pid, node}
  end

  defp add_code_paths(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, :code, :add_paths, [:code.get_path()]) do
      :ok -> :ok
      other -> raise "failed to add code paths on #{peer_node}: #{inspect(other)}"
    end
  end

  defp configure_peer(peer_node, cluster_nodes, routing_store_dir)
       when is_atom(peer_node) and is_list(cluster_nodes) and is_binary(routing_store_dir) do
    routing_lease_ttl_ms = Application.get_env(:starcite, :routing_lease_ttl_ms, 5_000)

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :routing_store_dir, routing_store_dir]
      )

    :ok =
      :rpc.call(peer_node, Application, :put_env, [:starcite, :cluster_node_ids, cluster_nodes])

    :ok = :rpc.call(peer_node, Application, :put_env, [:starcite, :routing_replication_factor, 3])

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :routing_lease_ttl_ms, routing_lease_ttl_ms]
      )

    :ok =
      :rpc.call(
        peer_node,
        Application,
        :put_env,
        [:starcite, :archive_adapter, Starcite.Archive.Adapter.Postgres]
      )

    :ok
  end

  defp start_peer_data_plane(peer_node, opts \\ [])
       when is_atom(peer_node) and is_list(opts) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :start, [], 15_000) do
      :ok ->
        if Keyword.get(opts, :wait_for_runtime, true) do
          :ok = wait_for_peer_runtime(peer_node)
        end

        :ok

      other ->
        raise "failed to start peer data plane on #{peer_node}: #{inspect(other)}"
    end
  end

  defp stop_peer_data_plane(peer_node) when is_atom(peer_node) do
    case :rpc.call(peer_node, DistributedPeerDataPlane, :stop, [], 15_000) do
      :ok -> :ok
      {:badrpc, _reason} -> :ok
      _other -> :ok
    end
  end

  defp stop_peer(peer_pid) when is_pid(peer_pid) do
    :peer.stop(peer_pid)
  catch
    :exit, _reason -> :ok
  end

  defp wait_for_peer_runtime(peer_node) when is_atom(peer_node) do
    eventually_value(
      fn ->
        case :rpc.call(peer_node, DistributedPeerDataPlane, :ready?, [], 10_000) do
          true -> :ok
          _other -> :retry
        end
      end,
      timeout: 30_000,
      interval: 100
    )
  end

  defp sync_peer_lease_ttl(peer_node) when is_atom(peer_node) do
    lease_ttl_ms = Application.get_env(:starcite, :routing_lease_ttl_ms, 5_000)

    assert :ok =
             :rpc.call(
               peer_node,
               Application,
               :put_env,
               [:starcite, :routing_lease_ttl_ms, lease_ttl_ms],
               5_000
             )

    :ok
  end

  defp clear_local_data_plane do
    :ok = SessionQuorum.clear()
    :ok = SessionStore.clear()
    :ok = EventStore.clear()
    :ok
  end

  defp wait_for_cluster_size(expected_size)
       when is_integer(expected_size) and expected_size > 0 do
    eventually(
      fn ->
        assert {:ok, nodes} = :khepri_cluster.nodes(Store.store_id(), %{favor: :consistency})
        assert length(nodes) == expected_size
        assert Node.self() in nodes
      end,
      timeout: 15_000,
      interval: 100
    )

    :ok
  end

  defp put_assignment(session_id, owner, replicas)
       when is_binary(session_id) and is_atom(owner) and is_list(replicas) do
    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: owner,
                 epoch: 1,
                 replicas: replicas,
                 status: :active,
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    :ok
  end

  defp seed_moving_assignment(session_id, owner, target_owner, replicas)
       when is_binary(session_id) and is_atom(owner) and is_atom(target_owner) and
              is_list(replicas) do
    assert :ok =
             :khepri.put(
               Store.store_id(),
               [:sessions, session_id],
               %{
                 owner: owner,
                 epoch: 1,
                 replicas: replicas,
                 status: :moving,
                 target_owner: target_owner,
                 transfer_id: "xfer-#{System.unique_integer([:positive, :monotonic])}",
                 updated_at_ms: System.system_time(:millisecond)
               },
               %{async: false, reply_from: :local, timeout: 5_000}
             )

    :ok
  end

  defp seed_replica_set(owner_node, %Session{} = session, events) when is_list(events) do
    if owner_node == Node.self() do
      with :ok <- SessionQuorum.apply_replica(session, events),
           :ok <- SessionQuorum.replicate_state(session, events) do
        :ok
      end
    else
      with :ok <- :rpc.call(owner_node, SessionQuorum, :apply_replica, [session, events], 5_000),
           :ok <- :rpc.call(owner_node, SessionQuorum, :replicate_state, [session, events], 5_000) do
        :ok
      end
    end
  end

  defp assert_peer_session(peer_node, %Session{id: session_id} = expected_session)
       when is_atom(peer_node) and is_binary(session_id) do
    case :rpc.call(peer_node, SessionQuorum, :get_session, [session_id], 5_000) do
      {:ok, %Session{} = actual_session} ->
        assert actual_session == expected_session

      other ->
        raise "unexpected session state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp assert_peer_events(peer_node, session_id, epoch, expected_seqs)
       when is_atom(peer_node) and is_binary(session_id) and is_integer(epoch) and epoch >= 0 and
              is_list(expected_seqs) do
    case :rpc.call(peer_node, EventStore, :from_cursor, [session_id, 0, 10], 5_000) do
      events when is_list(events) ->
        assert Enum.map(events, & &1.seq) == expected_seqs
        assert Enum.all?(events, &(&1.epoch == epoch))

      other ->
        raise "unexpected event state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp assert_hot_owner_sees_latest(session_id, expected_seq)
       when is_binary(session_id) and is_integer(expected_seq) and expected_seq > 0 do
    assert {:ok, %{owner: owner}} = Store.get_assignment(session_id, favor: :consistency)

    events =
      if owner == Node.self() do
        EventStore.from_cursor(session_id, 0, 10)
      else
        :rpc.call(owner, EventStore, :from_cursor, [session_id, 0, 10], 5_000)
      end

    assert is_list(events)

    seqs = Enum.map(events, & &1.seq)

    assert seqs == Enum.uniq(seqs)
    assert seqs == Enum.sort(seqs)
    assert Enum.all?(seqs, &(&1 <= expected_seq))
    assert expected_seq in seqs
  end

  defp assert_stored_session(peer_node, %Session{id: session_id} = expected_session)
       when is_atom(peer_node) and is_binary(session_id) do
    case :rpc.call(peer_node, SessionStore, :get_session_cached, [session_id], 5_000) do
      {:ok, %Session{} = actual_session} ->
        assert actual_session == expected_session

      other ->
        raise "unexpected cached session state on #{peer_node}: #{inspect(other)}"
    end
  end

  defp seed_peer_cache(
         peer_node,
         %Session{id: session_id, tenant_id: tenant_id} = session,
         events
       )
       when is_atom(peer_node) and is_binary(session_id) and is_binary(tenant_id) and
              is_list(events) do
    with :ok <- :rpc.call(peer_node, SessionStore, :put_session, [session], 5_000),
         :ok <-
           :rpc.call(peer_node, EventStore, :put_events, [session_id, tenant_id, events], 5_000) do
      :ok
    else
      other ->
        raise "failed to seed peer cache on #{peer_node}: #{inspect(other)}"
    end
  end

  defp append_input(producer_id, producer_seq)
       when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
              producer_seq > 0 do
    %{
      type: "message",
      payload: %{"text" => "replicated"},
      actor: "user",
      source: "test",
      metadata: %{"path" => "replication"},
      refs: %{},
      idempotency_key: "key-#{producer_id}-#{producer_seq}",
      producer_id: producer_id,
      producer_seq: producer_seq
    }
  end

  defp sanitize_node_name(node_name) when is_atom(node_name) do
    node_name
    |> Atom.to_string()
    |> String.replace(~r/[^a-zA-Z0-9]+/, "-")
  end

  defp restore_env(key, nil) when is_atom(key), do: Application.delete_env(:starcite, key)
  defp restore_env(key, value) when is_atom(key), do: Application.put_env(:starcite, key, value)

  defp restore_peer_env(peer_node, key, nil) when is_atom(peer_node) and is_atom(key) do
    :rpc.call(peer_node, Application, :delete_env, [:starcite, key], 5_000)
  end

  defp restore_peer_env(peer_node, key, value) when is_atom(peer_node) and is_atom(key) do
    :rpc.call(peer_node, Application, :put_env, [:starcite, key, value], 5_000)
  end

  defp assert_cursor_updates(session_ids, seq)
       when is_list(session_ids) and is_integer(seq) and seq > 0 do
    session_ids
    |> MapSet.new()
    |> do_assert_cursor_updates(seq)
  end

  defp do_assert_cursor_updates(expected, seq) do
    if MapSet.size(expected) == 0 do
      :ok
    else
      receive do
        {:cursor_update, %{session_id: session_id, seq: ^seq, last_seq: ^seq}} ->
          assert session_id in expected
          do_assert_cursor_updates(MapSet.delete(expected, session_id), seq)
      after
        10_000 ->
          flunk("timed out waiting for cursor updates for #{inspect(MapSet.to_list(expected))}")
      end
    end
  end

  defp eventually(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 3_000)
    interval = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      error in [ExUnit.AssertionError] ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          reraise(error, __STACKTRACE__)
        end
    end
  end

  defp eventually_value(fun, opts \\ []) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 3_000)
    interval = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually_value(fun, deadline, interval)
  end

  defp do_eventually_value(fun, deadline, interval) do
    case fun.() do
      :retry ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually_value(fun, deadline, interval)
        else
          flunk("timed out waiting for value")
        end

      value ->
        value
    end
  end
end
