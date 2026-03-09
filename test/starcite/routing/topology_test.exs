defmodule Starcite.Routing.WriteNodesTest do
  use ExUnit.Case, async: false

  alias Starcite.Routing.Topology

  @config_keys [:num_groups, :routing_replication_factor, :routing_node_ids]

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

  test "config! reads normalized static routing-node config" do
    Application.put_env(:starcite, :num_groups, 32)
    Application.put_env(:starcite, :routing_replication_factor, 2)
    Application.put_env(:starcite, :routing_node_ids, [:w1@cluster, :w1@cluster, :w2@cluster])

    assert %{
             num_groups: 32,
             replication_factor: 2,
             nodes: [:w1@cluster, :w2@cluster]
           } = Topology.config!()
  end

  test "validate returns tagged error when replication factor exceeds routing-node count" do
    Application.put_env(:starcite, :num_groups, 16)
    Application.put_env(:starcite, :routing_replication_factor, 4)
    Application.put_env(:starcite, :routing_node_ids, [:w1@cluster, :w2@cluster, :w3@cluster])

    assert {:error, message} = Topology.validate()
    assert message =~ "routing_replication_factor=4 exceeds routing_node_ids=3"
  end

  test "validate! raises for invalid num_groups" do
    Application.put_env(:starcite, :num_groups, 0)
    Application.put_env(:starcite, :routing_replication_factor, 1)
    Application.put_env(:starcite, :routing_node_ids, [:nonode@nohost])

    assert_raise ArgumentError, ~r/invalid value for :num_groups/, fn ->
      Topology.validate!()
    end
  end
end
