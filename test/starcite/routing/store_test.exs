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
