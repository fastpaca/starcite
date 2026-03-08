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
                 prefer_leader: true
               )

      assert length(failures) == 2
      refute_receive {:routing_event, _event, _measurements, _metadata}, 100
    end

    test "emits routing telemetry for local execution when telemetry is enabled" do
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
                 prefer_leader: false,
                 telemetry_operation: :append_event
               )

      assert_receive_routing_decision(group_id, :local, false, :disabled, 1, 1)
      assert_receive_routing_result(group_id, :local, :ok, 0, 0, 0)
    end

    test "emits routing telemetry for remote fallback execution when telemetry is enabled" do
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
                 telemetry_operation: :append_event
               )

      assert length(failures) == 2
      assert_receive_routing_decision(group_id, :remote, true, :miss, 2, 1)
      assert_receive_routing_result(group_id, :remote, :no_candidates, 2, 1, 0)
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

  defp assert_receive_routing_decision(
         group_id,
         target,
         prefer_leader,
         leader_hint,
         replica_count,
         ready_count
       ) do
    deadline = System.monotonic_time(:millisecond) + 1_000

    do_assert_receive_routing_decision(
      group_id,
      target,
      prefer_leader,
      leader_hint,
      replica_count,
      ready_count,
      deadline
    )
  end

  defp do_assert_receive_routing_decision(
         group_id,
         target,
         prefer_leader,
         leader_hint,
         replica_count,
         ready_count,
         deadline
       ) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)
    node_name = Atom.to_string(Node.self())

    receive do
      {:routing_event, [:starcite, :routing, :decision],
       %{count: 1, replica_count: ^replica_count, ready_count: ^ready_count},
       %{
         node: ^node_name,
         group_id: ^group_id,
         target: ^target,
         prefer_leader: ^prefer_leader,
         leader_hint: ^leader_hint
       }} ->
        :ok

      {:routing_event, _event, _measurements, _metadata} ->
        do_assert_receive_routing_decision(
          group_id,
          target,
          prefer_leader,
          leader_hint,
          replica_count,
          ready_count,
          deadline
        )
    after
      remaining ->
        flunk(
          "timed out waiting for routing decision group_id=#{inspect(group_id)} target=#{inspect(target)}"
        )
    end
  end

  defp assert_receive_routing_result(
         group_id,
         path,
         outcome,
         attempts,
         retries,
         leader_redirects
       ) do
    deadline = System.monotonic_time(:millisecond) + 1_000

    do_assert_receive_routing_result(
      group_id,
      path,
      outcome,
      attempts,
      retries,
      leader_redirects,
      deadline
    )
  end

  defp do_assert_receive_routing_result(
         group_id,
         path,
         outcome,
         attempts,
         retries,
         leader_redirects,
         deadline
       ) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)
    node_name = Atom.to_string(Node.self())

    receive do
      {:routing_event, [:starcite, :routing, :result],
       %{count: 1, attempts: ^attempts, retries: ^retries, leader_redirects: ^leader_redirects},
       %{node: ^node_name, group_id: ^group_id, path: ^path, outcome: ^outcome}} ->
        :ok

      {:routing_event, _event, _measurements, _metadata} ->
        do_assert_receive_routing_result(
          group_id,
          path,
          outcome,
          attempts,
          retries,
          leader_redirects,
          deadline
        )
    after
      remaining ->
        flunk(
          "timed out waiting for routing result group_id=#{inspect(group_id)} path=#{inspect(path)} outcome=#{inspect(outcome)}"
        )
    end
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
