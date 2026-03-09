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
