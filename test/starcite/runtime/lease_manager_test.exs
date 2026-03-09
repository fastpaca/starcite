defmodule Starcite.Routing.RaftManagerTest do
  use ExUnit.Case, async: false

  alias Starcite.Routing.LeaseManager

  @config_keys [:num_groups, :routing_replication_factor, :routing_node_ids, :raft_data_dir]

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

  test "replicas_for_group uses only configured routing nodes" do
    routing_nodes = [:"write-1@cluster", :"write-2@cluster", :"write-3@cluster"]

    Application.put_env(:starcite, :num_groups, 8)
    Application.put_env(:starcite, :routing_replication_factor, 2)
    Application.put_env(:starcite, :routing_node_ids, routing_nodes)

    Enum.each(0..7, fn group_id ->
      replicas = LeaseManager.replicas_for_group(group_id)
      assert length(replicas) == 2
      assert Enum.all?(replicas, &(&1 in routing_nodes))
    end)
  end

  test "group_for_session uses configured num_groups" do
    Application.put_env(:starcite, :num_groups, 16)

    group_id = LeaseManager.group_for_session("session-a")

    assert is_integer(group_id)
    assert group_id >= 0
    assert group_id < 16
  end

  test "validate_config! fails when replication factor exceeds routing nodes" do
    Application.put_env(:starcite, :num_groups, 16)
    Application.put_env(:starcite, :routing_replication_factor, 4)
    Application.put_env(:starcite, :routing_node_ids, [:w1@cluster, :w2@cluster, :w3@cluster])

    assert_raise ArgumentError, ~r/routing_replication_factor=4 exceeds routing_node_ids=3/, fn ->
      LeaseManager.validate_config!()
    end
  end

  test "ra_system_data_dir uses configured raft_data_dir root" do
    Application.put_env(:starcite, :raft_data_dir, "tmp/custom_raft")

    assert LeaseManager.raft_data_dir_root() == "tmp/custom_raft"
    assert LeaseManager.ra_system_data_dir() == Path.join("tmp/custom_raft", "ra_system")
  end
end
