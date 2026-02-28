defmodule Starcite.Runtime.RoutingTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.ReplicaRouter

  defmodule RoutingProbe do
    def local_ok, do: {:ok, :local}
    def remote_ok, do: {:ok, :remote}
  end

  describe "route_target/2" do
    test "returns local when node is in replica set" do
      self_node = :"node1@127.0.0.1"
      group_id = unique_group_id()

      assert {:local, ^self_node} =
               ReplicaRouter.route_target(group_id,
                 self: self_node,
                 replicas: [self_node, :"node2@127.0.0.1"],
                 ready_nodes: [self_node],
                 local_running: true
               )
    end

    test "prioritises ready replicas before fallbacks" do
      result =
        ReplicaRouter.route_target(13,
          self: :"observer@127.0.0.1",
          replicas: [
            :"node1@127.0.0.1",
            :"node2@127.0.0.1",
            :"node3@127.0.0.1"
          ],
          ready_nodes: [:"node3@127.0.0.1"]
        )

      assert {:remote, nodes} = result
      assert [:"node3@127.0.0.1", :"node1@127.0.0.1", :"node2@127.0.0.1"] == nodes
    end

    test "returns empty remote list when replica set empty" do
      assert {:remote, []} =
               ReplicaRouter.route_target(7,
                 self: :nonode@nohost,
                 replicas: [],
                 ready_nodes: []
               )
    end

    test "prefer_leader does not front-load a non-ready leader hint" do
      group_id = unique_group_id()
      hinted_leader = :"leader@127.0.0.1"
      ready_node = :"ready@127.0.0.1"

      seed_leader_hint(group_id, hinted_leader)

      assert {:remote, [^ready_node, ^hinted_leader]} =
               ReplicaRouter.route_target(group_id,
                 self: :"router@127.0.0.1",
                 replicas: [hinted_leader, ready_node],
                 ready_nodes: [ready_node],
                 prefer_leader: true,
                 local_running: false,
                 allow_local: false
               )
    end

    test "prefer_leader does not route local when local node is not ready" do
      group_id = unique_group_id()
      self_node = :"leader@127.0.0.1"
      ready_remote = :"ready@127.0.0.1"

      seed_leader_hint(group_id, self_node)

      assert {:remote, [^ready_remote, ^self_node]} =
               ReplicaRouter.route_target(group_id,
                 self: self_node,
                 replicas: [self_node, ready_remote],
                 ready_nodes: [ready_remote],
                 prefer_leader: true,
                 local_running: true
               )
    end
  end

  describe "call_on_replica/8 hot-path behavior" do
    test "does not emit routing telemetry for local execution" do
      group_id = unique_group_id()
      handler_id = attach_routing_handler()

      on_exit(fn -> :telemetry.detach(handler_id) end)

      assert {:ok, :local} =
               ReplicaRouter.call_on_replica(
                 group_id,
                 RoutingProbe,
                 :remote_ok,
                 [],
                 RoutingProbe,
                 :local_ok,
                 [],
                 self: Node.self(),
                 replicas: [Node.self()],
                 ready_nodes: [Node.self()],
                 local_running: true,
                 prefer_leader: false
               )

      refute_receive {:routing_event, _event, _measurements, _metadata}, 100
    end

    test "does not emit routing telemetry for remote fallback execution" do
      group_id = unique_group_id()
      handler_id = attach_routing_handler()

      on_exit(fn -> :telemetry.detach(handler_id) end)

      missing_a = :"missing-a@127.0.0.1"
      missing_b = :"missing-b@127.0.0.1"

      assert {:error, {:no_available_replicas, failures}} =
               ReplicaRouter.call_on_replica(
                 group_id,
                 RoutingProbe,
                 :remote_ok,
                 [],
                 RoutingProbe,
                 :local_ok,
                 [],
                 self: Node.self(),
                 replicas: [missing_a, missing_b],
                 ready_nodes: [missing_a],
                 local_running: false,
                 allow_local: false,
                 prefer_leader: true,
                 tenant_id: "acme"
               )

      assert length(failures) == 2
      refute_receive {:routing_event, _event, _measurements, _metadata}, 100
    end
  end

  defp unique_group_id do
    rem(
      System.unique_integer([:positive, :monotonic]),
      Starcite.DataPlane.RaftManager.num_groups()
    )
  end

  defp attach_routing_handler do
    handler_id = "routing-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:starcite, :routing, :decision],
          [:starcite, :routing, :result]
        ],
        fn event, measurements, metadata, pid ->
          send(pid, {:routing_event, event, measurements, metadata})
        end,
        test_pid
      )

    handler_id
  end

  defp seed_leader_hint(group_id, node) when is_integer(group_id) and is_atom(node) do
    _ =
      ReplicaRouter.route_target(group_id,
        self: node,
        replicas: [node],
        ready_nodes: [node]
      )

    :ets.insert(
      :starcite_replica_router_leader_cache,
      {group_id, node, System.monotonic_time(:millisecond)}
    )
  end
end
