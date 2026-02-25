defmodule Starcite.DataPlane.RaftManagerTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.RaftManager

  @config_keys [
    :num_groups,
    :write_replication_factor,
    :write_node_ids,
    :raft_data_dir,
    :raft_data_dir_required_prefix
  ]

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

  test "replicas_for_group uses only configured write nodes" do
    write_nodes = [:"write-1@cluster", :"write-2@cluster", :"write-3@cluster"]

    Application.put_env(:starcite, :num_groups, 8)
    Application.put_env(:starcite, :write_replication_factor, 2)
    Application.put_env(:starcite, :write_node_ids, write_nodes)

    Enum.each(0..7, fn group_id ->
      replicas = RaftManager.replicas_for_group(group_id)
      assert length(replicas) == 2
      assert Enum.all?(replicas, &(&1 in write_nodes))
    end)
  end

  test "group_for_session uses configured num_groups" do
    Application.put_env(:starcite, :num_groups, 16)

    group_id = RaftManager.group_for_session("session-a")

    assert is_integer(group_id)
    assert group_id >= 0
    assert group_id < 16
  end

  test "validate_config! fails when replication factor exceeds write nodes" do
    Application.put_env(:starcite, :num_groups, 16)
    Application.put_env(:starcite, :write_replication_factor, 4)
    Application.put_env(:starcite, :write_node_ids, [:w1@cluster, :w2@cluster, :w3@cluster])

    assert_raise ArgumentError, ~r/write_replication_factor=4 exceeds write_node_ids=3/, fn ->
      RaftManager.validate_config!()
    end
  end

  test "ra_system_data_dir uses configured raft_data_dir root" do
    Application.put_env(:starcite, :raft_data_dir, "tmp/custom_raft")

    assert RaftManager.raft_data_dir_root() == "tmp/custom_raft"
    assert RaftManager.ra_system_data_dir() == Path.join("tmp/custom_raft", "ra_system")
  end

  test "raft_data_dir_root fails on invalid empty path" do
    Application.put_env(:starcite, :raft_data_dir, "   ")

    assert_raise ArgumentError, ~r/invalid value for :raft_data_dir/, fn ->
      RaftManager.raft_data_dir_root()
    end
  end

  test "raft_data_dir_root enforces required prefix when configured" do
    Application.put_env(:starcite, :raft_data_dir_required_prefix, "/var/lib/starcite")
    Application.put_env(:starcite, :raft_data_dir, "/var/lib/starcite/raft")

    assert RaftManager.raft_data_dir_root() == "/var/lib/starcite/raft"
  end

  test "raft_data_dir_root rejects path outside required prefix" do
    Application.put_env(:starcite, :raft_data_dir_required_prefix, "/var/lib/starcite")
    Application.put_env(:starcite, :raft_data_dir, "/tmp/custom_raft")

    assert_raise ArgumentError, ~r/must be within/, fn ->
      RaftManager.raft_data_dir_root()
    end
  end

  test "raft_data_dir_root requires absolute path when required prefix is configured" do
    Application.put_env(:starcite, :raft_data_dir_required_prefix, "/var/lib/starcite")
    Application.put_env(:starcite, :raft_data_dir, "tmp/custom_raft")

    assert_raise ArgumentError, ~r/expected absolute path/, fn ->
      RaftManager.raft_data_dir_root()
    end
  end
end
