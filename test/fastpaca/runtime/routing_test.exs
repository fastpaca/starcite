defmodule Fastpaca.Runtime.RoutingTest do
  use ExUnit.Case, async: true

  alias Fastpaca.Runtime

  describe "route_target/2" do
    test "returns local when node is in replica set" do
      self_node = :"node1@127.0.0.1"

      assert {:local, ^self_node} =
               Runtime.route_target(42,
                 self: self_node,
                 replicas: [self_node, :"node2@127.0.0.1"],
                 ready_nodes: [self_node],
                 local_running: true
               )
    end

    test "prioritises ready replicas before fallbacks" do
      result =
        Runtime.route_target(13,
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
               Runtime.route_target(7,
                 self: :nonode@nohost,
                 replicas: [],
                 ready_nodes: []
               )
    end
  end
end
