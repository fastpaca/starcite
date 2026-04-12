defmodule Starcite.Routing.PolicyTest do
  use ExUnit.Case, async: true

  alias Starcite.Routing.Policy

  test "choose_claim_nodes allows degraded claims when ready nodes still satisfy quorum" do
    node_a = :"policy-node-a@127.0.0.1"
    node_b = :"policy-node-b@127.0.0.1"

    assert {:ok, [^node_a, ^node_b]} =
             Policy.choose_claim_nodes(%{}, [node_a, node_b], 3)
  end

  test "choose_claim_nodes rejects claims when ready nodes are below quorum" do
    node_a = :"policy-node-a@127.0.0.1"

    assert {:error, :no_ready_cluster_nodes} =
             Policy.choose_claim_nodes(%{}, [node_a], 3)
  end
end
