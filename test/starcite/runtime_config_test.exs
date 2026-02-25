defmodule Starcite.RuntimeConfigTest do
  use ExUnit.Case, async: false

  @raft_data_dir_env "STARCITE_RAFT_DATA_DIR"

  setup do
    original_raft_data_dir = System.get_env(@raft_data_dir_env)

    on_exit(fn ->
      restore_env(@raft_data_dir_env, original_raft_data_dir)
    end)

    :ok
  end

  test "runtime config pins :ra data and wal dirs to STARCITE_RAFT_DATA_DIR" do
    raft_data_dir = "tmp/runtime_config_raft"
    System.put_env(@raft_data_dir_env, raft_data_dir)

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :raft_data_dir) == raft_data_dir

    ra_config = Keyword.fetch!(config, :ra)
    expected_ra_system_dir = String.to_charlist(Path.join(raft_data_dir, "ra_system"))

    assert Keyword.fetch!(ra_config, :data_dir) == expected_ra_system_dir
    assert Keyword.fetch!(ra_config, :wal_data_dir) == expected_ra_system_dir
  end

  defp restore_env(env_name, nil), do: System.delete_env(env_name)
  defp restore_env(env_name, value), do: System.put_env(env_name, value)
end
