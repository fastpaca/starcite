defmodule Starcite.Routing.StoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Routing.Store
  alias Starcite.Runtime.TestHelper

  setup do
    original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
    original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

    on_exit(fn ->
      Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
      Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      TestHelper.reset()
    end)

    :ok
  end

  test "commit_transfer promotes the target assignment on the local store" do
    peer = :"routing-store-peer@127.0.0.1"
    session_id = "routing-store-transfer-#{System.unique_integer([:positive, :monotonic])}"
    transfer_id = "xfer-local-1"
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
    Application.put_env(:starcite, :routing_replication_factor, 2)
    TestHelper.reset()

    assert :ok = Store.renew_local_lease()
    assert :ok = Store.mark_node_draining(Node.self())

    assert :ok =
             put_node_record(peer, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), peer],
               status: :moving,
               target_owner: peer,
               transfer_id: transfer_id,
               updated_at_ms: now_ms
             })

    assert {:ok, assignment} = Store.commit_transfer(session_id, transfer_id)
    assert assignment.owner == peer
    assert assignment.epoch == 2
    assert assignment.status == :active
    assert assignment.replicas == [peer]

    assert {:ok, stored} = Store.get_assignment(session_id, favor: :consistency)
    assert stored == assignment
  end

  test "start_drain_transfers falls back to a ready node outside the stale replica set" do
    replica_a = :"routing-store-replica-a@127.0.0.1"
    replica_b = :"routing-store-replica-b@127.0.0.1"
    standby = :"routing-store-standby@127.0.0.1"
    session_id = "routing-store-drain-fallback-#{System.unique_integer([:positive, :monotonic])}"
    now_ms = System.system_time(:millisecond)

    Application.put_env(
      :starcite,
      :cluster_node_ids,
      [Node.self(), replica_a, replica_b, standby]
    )

    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok = Store.mark_node_draining(Node.self())

    assert :ok =
             put_node_record(replica_a, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(replica_b, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(standby, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), replica_a, replica_b],
               status: :active,
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.start_drain_transfers(Node.self())

    assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
    assert assignment.owner == Node.self()
    assert assignment.status == :moving
    assert assignment.target_owner == standby
  end

  test "reassign_sessions_from falls back outside the stale replica set when replicas are unavailable" do
    replica_a = :"routing-store-failover-a@127.0.0.1"
    replica_b = :"routing-store-failover-b@127.0.0.1"
    standby = :"routing-store-failover-standby@127.0.0.1"

    session_id =
      "routing-store-failover-fallback-#{System.unique_integer([:positive, :monotonic])}"

    now_ms = System.system_time(:millisecond)

    Application.put_env(
      :starcite,
      :cluster_node_ids,
      [Node.self(), replica_a, replica_b, standby]
    )

    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :ready,
               lease_until_ms: now_ms - 1,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(replica_a, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(replica_b, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(standby, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), replica_a, replica_b],
               status: :active,
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.reassign_sessions_from(Node.self())

    assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
    assert assignment.owner == standby
    assert assignment.epoch == 2
    assert assignment.status == :active
    assert standby in assignment.replicas
  end

  test "reassign_sessions_from promotes the moving target owner when it is ready" do
    target_owner = :"routing-store-moving-target@127.0.0.1"
    standby = :"routing-store-moving-standby@127.0.0.1"

    session_id =
      "routing-store-moving-failover-#{System.unique_integer([:positive, :monotonic])}"

    now_ms = System.system_time(:millisecond)

    Application.put_env(
      :starcite,
      :cluster_node_ids,
      [Node.self(), target_owner, standby]
    )

    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :ready,
               lease_until_ms: now_ms - 1,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(target_owner, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(standby, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), target_owner, standby],
               status: :moving,
               target_owner: target_owner,
               transfer_id: "xfer-failover-1",
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.reassign_sessions_from(Node.self())

    assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
    assert assignment.owner == target_owner
    assert assignment.epoch == 2
    assert assignment.status == :active
    assert target_owner in assignment.replicas
    refute Map.has_key?(assignment, :target_owner)
    refute Map.has_key?(assignment, :transfer_id)
  end

  test "reassign_sessions_from falls back when a moving target owner is unavailable" do
    target_owner = :"routing-store-moving-unavailable@127.0.0.1"
    standby = :"routing-store-moving-fallback@127.0.0.1"

    session_id =
      "routing-store-moving-fallback-#{System.unique_integer([:positive, :monotonic])}"

    now_ms = System.system_time(:millisecond)

    Application.put_env(
      :starcite,
      :cluster_node_ids,
      [Node.self(), target_owner, standby]
    )

    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :ready,
               lease_until_ms: now_ms - 1,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(target_owner, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(standby, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), target_owner, standby],
               status: :moving,
               target_owner: target_owner,
               transfer_id: "xfer-failover-2",
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.reassign_sessions_from(Node.self())

    assert {:ok, assignment} = Store.get_assignment(session_id, favor: :consistency)
    assert assignment.owner == standby
    assert assignment.epoch == 2
    assert assignment.status == :active
    assert standby in assignment.replicas
    refute Map.has_key?(assignment, :target_owner)
    refute Map.has_key?(assignment, :transfer_id)
  end

  test "ensure_assignment balances owners across ready nodes" do
    peer_a = :"routing-store-balance-a@127.0.0.1"
    peer_b = :"routing-store-balance-b@127.0.0.1"
    nodes = [Node.self(), peer_a, peer_b]
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    Enum.each(nodes, fn node ->
      assert :ok =
               put_node_record(node, %{
                 status: :ready,
                 lease_until_ms: now_ms + 60_000,
                 updated_at_ms: now_ms
               })
    end)

    owner_counts =
      1..18
      |> Enum.map(fn i ->
        session_id =
          "routing-store-balanced-#{i}-#{System.unique_integer([:positive, :monotonic])}"

        assert {:ok, assignment} = Store.ensure_assignment(session_id)
        assert Enum.sort(assignment.replicas) == Enum.sort(nodes)
        assignment.owner
      end)
      |> Enum.frequencies()

    assert owner_counts == %{Node.self() => 6, peer_a => 6, peer_b => 6}
  end

  test "concurrent ensure_assignment claims stay evenly balanced across ready nodes" do
    peer_a = :"routing-store-concurrent-a@127.0.0.1"
    peer_b = :"routing-store-concurrent-b@127.0.0.1"
    nodes = [Node.self(), peer_a, peer_b]
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    Enum.each(nodes, fn node ->
      assert :ok =
               put_node_record(node, %{
                 status: :ready,
                 lease_until_ms: now_ms + 60_000,
                 updated_at_ms: now_ms
               })
    end)

    owner_counts =
      1..30
      |> Task.async_stream(
        fn i ->
          session_id =
            "routing-store-concurrent-balanced-#{i}-#{System.unique_integer([:positive, :monotonic])}"

          assert {:ok, assignment} = Store.ensure_assignment(session_id)
          assert Enum.sort(assignment.replicas) == Enum.sort(nodes)
          assignment.owner
        end,
        max_concurrency: 30,
        timeout: 5_000,
        ordered: false
      )
      |> Enum.map(fn {:ok, owner} -> owner end)
      |> Enum.frequencies()

    assert owner_counts == %{Node.self() => 10, peer_a => 10, peer_b => 10}
  end

  test "concurrent ensure_assignment never places ownership on a draining node" do
    peer_a = :"routing-store-draining-a@127.0.0.1"
    peer_b = :"routing-store-draining-b@127.0.0.1"
    peer_c = :"routing-store-draining-c@127.0.0.1"
    nodes = [Node.self(), peer_a, peer_b, peer_c]
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, nodes)
    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :draining,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    Enum.each([peer_a, peer_b, peer_c], fn node ->
      assert :ok =
               put_node_record(node, %{
                 status: :ready,
                 lease_until_ms: now_ms + 60_000,
                 updated_at_ms: now_ms
               })
    end)

    owner_counts =
      1..24
      |> Task.async_stream(
        fn i ->
          session_id =
            "routing-store-draining-balanced-#{i}-#{System.unique_integer([:positive, :monotonic])}"

          assert {:ok, assignment} = Store.ensure_assignment(session_id)
          refute assignment.owner == Node.self()
          assert Enum.sort(assignment.replicas) == Enum.sort([peer_a, peer_b, peer_c])
          assignment.owner
        end,
        max_concurrency: 24,
        timeout: 5_000,
        ordered: false
      )
      |> Enum.map(fn {:ok, owner} -> owner end)
      |> Enum.frequencies()

    assert owner_counts == %{peer_a => 8, peer_b => 8, peer_c => 8}
  end

  test "recovers local node readiness and claims after a local Khepri reset" do
    session_id = "routing-store-reset-recovery-#{System.unique_integer([:positive, :monotonic])}"
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self()])
    Application.put_env(:starcite, :routing_replication_factor, 1)
    TestHelper.reset()

    assert :ok = Store.renew_local_lease()
    assert Node.self() in Store.ready_nodes()

    assert :ok = :khepri_cluster.reset(Store.store_id(), 15_000)

    assert :ok = Store.renew_local_lease()
    assert Node.self() in Store.ready_nodes()

    assert {:ok, record} = Store.node_record(Node.self(), favor: :consistency)
    assert record.status == :ready
    assert record.lease_until_ms > now_ms

    assert {:ok, assignment} = Store.ensure_assignment(session_id)
    assert assignment.owner == Node.self()
    assert assignment.replicas == [Node.self()]
  end

  test "undrain_node rejects nodes with active or moving drain work" do
    peer = :"routing-store-undrain-peer@127.0.0.1"
    session_id = "routing-store-undrain-#{System.unique_integer([:positive, :monotonic])}"
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
    Application.put_env(:starcite, :routing_replication_factor, 2)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :draining,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(peer, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), peer],
               status: :moving,
               target_owner: peer,
               transfer_id: "xfer-undrain-1",
               updated_at_ms: now_ms
             })

    assert {:error, :node_still_draining} =
             Starcite.Operations.Maintenance.undrain_node(Node.self())
  end

  test "undrain_node allows a fully drained node to return ready" do
    now_ms = System.system_time(:millisecond)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self()])
    Application.put_env(:starcite, :routing_replication_factor, 1)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok = Starcite.Operations.Maintenance.undrain_node(Node.self())
    assert Store.node_status(Node.self()) == :ready
  end

  test "emits node state transition telemetry with the transition source" do
    handler_id = "routing-node-state-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :routing, :node_state],
        fn _event, measurements, metadata, pid ->
          send(pid, {:routing_node_state_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)

    assert :ok = Store.mark_node_draining(Node.self(), :maintenance)

    assert_receive {:routing_node_state_event, %{count: 1},
                    %{node: node_name, from: :ready, to: :draining, source: :maintenance}},
                   1_000

    assert node_name == Atom.to_string(Node.self())
  end

  test "emits transfer telemetry when drain transfers start and commit" do
    peer = :"routing-store-transfer-telemetry@127.0.0.1"

    session_id =
      "routing-store-transfer-telemetry-#{System.unique_integer([:positive, :monotonic])}"

    transfer_handler = "routing-transfer-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()
    now_ms = System.system_time(:millisecond)

    :ok =
      :telemetry.attach(
        transfer_handler,
        [:starcite, :routing, :transfer],
        fn _event, measurements, metadata, pid ->
          send(pid, {:routing_transfer_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(transfer_handler)
    end)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer])
    Application.put_env(:starcite, :routing_replication_factor, 2)
    TestHelper.reset()

    assert :ok = Store.mark_node_draining(Node.self())

    assert :ok =
             put_node_record(peer, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), peer],
               status: :active,
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.start_drain_transfers(Node.self())

    assert_receive {:routing_transfer_event, %{count: 1},
                    %{
                      session_id: ^session_id,
                      source_node: source_node,
                      target_node: target_node,
                      action: :started
                    }},
                   1_000

    assert source_node == Atom.to_string(Node.self())
    assert target_node == Atom.to_string(peer)

    assert {:ok, %{transfer_id: transfer_id}} =
             Store.get_assignment(session_id, favor: :consistency)

    assert {:ok, _assignment} = Store.commit_transfer(session_id, transfer_id)

    assert_receive {:routing_transfer_event, %{count: 1},
                    %{
                      session_id: ^session_id,
                      source_node: ^source_node,
                      target_node: ^target_node,
                      action: :committed
                    }},
                   1_000
  end

  test "emits failover telemetry when lease expiry reassigns ownership" do
    peer = :"routing-store-failover-telemetry@127.0.0.1"
    standby = :"routing-store-failover-target@127.0.0.1"

    session_id =
      "routing-store-failover-telemetry-#{System.unique_integer([:positive, :monotonic])}"

    failover_handler = "routing-failover-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()
    now_ms = System.system_time(:millisecond)

    :ok =
      :telemetry.attach(
        failover_handler,
        [:starcite, :routing, :failover],
        fn _event, measurements, metadata, pid ->
          send(pid, {:routing_failover_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(failover_handler)
    end)

    Application.put_env(:starcite, :cluster_node_ids, [Node.self(), peer, standby])
    Application.put_env(:starcite, :routing_replication_factor, 3)
    TestHelper.reset()

    assert :ok =
             put_node_record(Node.self(), %{
               status: :ready,
               lease_until_ms: now_ms - 1,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(peer, %{
               status: :drained,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_node_record(standby, %{
               status: :ready,
               lease_until_ms: now_ms + 60_000,
               updated_at_ms: now_ms
             })

    assert :ok =
             put_assignment(session_id, %{
               owner: Node.self(),
               epoch: 1,
               replicas: [Node.self(), peer, standby],
               status: :active,
               updated_at_ms: now_ms
             })

    assert {:ok, 1} = Store.reassign_sessions_from(Node.self())

    assert_receive {:routing_failover_event, %{count: 1},
                    %{
                      session_id: ^session_id,
                      source_node: source_node,
                      target_node: target_node,
                      reason: :lease_expired
                    }},
                   1_000

    assert source_node == Atom.to_string(Node.self())
    assert target_node == Atom.to_string(standby)
  end

  defp put_assignment(session_id, assignment) when is_binary(session_id) and is_map(assignment) do
    :khepri.put(Store.store_id(), [:sessions, session_id], assignment, khepri_opts())
  end

  defp put_node_record(node, record) when is_atom(node) and is_map(record) do
    :khepri.put(Store.store_id(), [:nodes, Atom.to_string(node)], record, khepri_opts())
  end

  defp khepri_opts do
    %{async: false, reply_from: :local, timeout: 5_000}
  end
end
