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

      assert {:local, ^self_node} =
               ReplicaRouter.route_target(42,
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

  describe "call_on_replica/8 telemetry" do
    test "emits routing decision and result for local execution" do
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

      events = collect_routing_events(group_id, 2)

      assert {[:starcite, :routing, :decision], decision_measurements, decision_metadata} =
               Enum.find(events, fn {event, _, _} -> event == [:starcite, :routing, :decision] end)

      assert decision_measurements.count == 1
      assert decision_metadata.target == :local
      assert decision_metadata.leader_hint == :disabled
      assert decision_metadata.prefer_leader == false

      assert {[:starcite, :routing, :result], result_measurements, result_metadata} =
               Enum.find(events, fn {event, _, _} -> event == [:starcite, :routing, :result] end)

      assert result_measurements.count == 1
      assert result_measurements.attempts == 1
      assert result_measurements.retries == 0
      assert result_measurements.leader_redirects == 0
      assert result_metadata.path == :local
      assert result_metadata.outcome == :ok
    end

    test "emits retry statistics for remote fallback execution" do
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

      events = collect_routing_events(group_id, 2)

      assert {[:starcite, :routing, :decision], _decision_measurements, decision_metadata} =
               Enum.find(events, fn {event, _, _} -> event == [:starcite, :routing, :decision] end)

      assert decision_metadata.target == :remote
      assert decision_metadata.prefer_leader == true
      assert decision_metadata.leader_hint in [:miss, :hit]

      assert {[:starcite, :routing, :result], result_measurements, result_metadata} =
               Enum.find(events, fn {event, _, _} -> event == [:starcite, :routing, :result] end)

      assert result_measurements.attempts == 2
      assert result_measurements.retries == 1
      assert result_measurements.leader_redirects >= 0
      assert result_metadata.path == :remote
      assert result_metadata.outcome == :no_candidates
    end
  end

  defp unique_group_id do
    System.unique_integer([:positive, :monotonic])
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

  defp collect_routing_events(group_id, count) when is_integer(count) and count > 0 do
    do_collect_routing_events(group_id, count, [])
  end

  defp do_collect_routing_events(_group_id, 0, acc), do: Enum.reverse(acc)

  defp do_collect_routing_events(group_id, remaining, acc) do
    receive do
      {:routing_event, event, measurements, %{group_id: ^group_id} = metadata} ->
        do_collect_routing_events(group_id, remaining - 1, [{event, measurements, metadata} | acc])

      {:routing_event, _event, _measurements, _metadata} ->
        do_collect_routing_events(group_id, remaining, acc)
    after
      1_000 ->
        flunk("timed out waiting for routing telemetry events")
    end
  end
end
