defmodule Starcite.Routing.TopologyTest do
  use ExUnit.Case, async: false

  alias Starcite.Routing.Topology

  @config_keys [:routing_replication_factor, :routing_node_ids]

  setup do
    original =
      Enum.into(@config_keys, %{}, fn key ->
        {key, Application.get_env(:starcite, key)}
      end)

    on_exit(fn ->
      Enum.each(@config_keys, fn key ->
        case Map.get(original, key) do
          nil -> Application.delete_env(:starcite, key)
          value -> Application.put_env(:starcite, key, value)
        end
      end)
    end)

    :ok
  end

  test "config! reads normalized routing-node config" do
    self_node = Node.self()

    Application.put_env(:starcite, :routing_replication_factor, 2)

    Application.put_env(
      :starcite,
      :routing_node_ids,
      [self_node, self_node, :"peer-a@cluster"]
    )

    assert %{replication_factor: 2, nodes: nodes} = Topology.config!()
    assert nodes == [self_node, :"peer-a@cluster"]
  end

  test "validate returns tagged error when replication factor exceeds routing-node count" do
    self_node = Node.self()

    Application.put_env(:starcite, :routing_replication_factor, 4)

    Application.put_env(
      :starcite,
      :routing_node_ids,
      [self_node, :"peer-a@cluster", :"peer-b@cluster"]
    )

    assert {:error, message} = Topology.validate()
    assert message =~ "routing_replication_factor=4 exceeds routing_node_ids=3"
  end

  test "validate returns tagged error when local node is missing from routing-node config" do
    Application.put_env(:starcite, :routing_replication_factor, 2)
    Application.put_env(:starcite, :routing_node_ids, [:"peer-a@cluster", :"peer-b@cluster"])

    assert {:error, message} = Topology.validate()
    assert message =~ "local node"
    assert message =~ "routing_node_ids"
  end
end
