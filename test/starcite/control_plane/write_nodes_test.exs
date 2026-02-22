defmodule Starcite.ControlPlane.WriteNodesTest do
  use ExUnit.Case, async: false

  alias Starcite.ControlPlane.WriteNodes

  @config_keys [:num_groups, :write_replication_factor, :write_node_ids]

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

  test "config! reads normalized static write-node config" do
    Application.put_env(:starcite, :num_groups, 32)
    Application.put_env(:starcite, :write_replication_factor, 2)
    Application.put_env(:starcite, :write_node_ids, [:w1@cluster, :w1@cluster, :w2@cluster])

    assert %{
             num_groups: 32,
             replication_factor: 2,
             nodes: [:w1@cluster, :w2@cluster]
           } = WriteNodes.config!()
  end

  test "validate returns tagged error when replication factor exceeds write-node count" do
    Application.put_env(:starcite, :num_groups, 16)
    Application.put_env(:starcite, :write_replication_factor, 4)
    Application.put_env(:starcite, :write_node_ids, [:w1@cluster, :w2@cluster, :w3@cluster])

    assert {:error, message} = WriteNodes.validate()
    assert message =~ "write_replication_factor=4 exceeds write_node_ids=3"
  end

  test "validate! raises for invalid num_groups" do
    Application.put_env(:starcite, :num_groups, 0)
    Application.put_env(:starcite, :write_replication_factor, 1)
    Application.put_env(:starcite, :write_node_ids, [:nonode@nohost])

    assert_raise ArgumentError, ~r/invalid value for :num_groups/, fn ->
      WriteNodes.validate!()
    end
  end
end
