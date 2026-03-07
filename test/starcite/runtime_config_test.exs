defmodule Starcite.RuntimeConfigTest do
  use ExUnit.Case, async: false

  @raft_data_dir_env "STARCITE_RAFT_DATA_DIR"
  @ra_wal_data_dir_env "STARCITE_RA_WAL_DATA_DIR"
  @ra_wal_write_strategy_env "STARCITE_RA_WAL_WRITE_STRATEGY"
  @ra_wal_sync_method_env "STARCITE_RA_WAL_SYNC_METHOD"
  @enable_telemetry_env "STARCITE_ENABLE_TELEMETRY"
  @runtime_envs [
    @raft_data_dir_env,
    @ra_wal_data_dir_env,
    @ra_wal_write_strategy_env,
    @ra_wal_sync_method_env,
    @enable_telemetry_env
  ]

  setup do
    original_envs = Map.new(@runtime_envs, &{&1, System.get_env(&1)})

    on_exit(fn ->
      Enum.each(original_envs, fn {env_name, value} ->
        restore_env(env_name, value)
      end)
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

  test "runtime config applies explicit Ra WAL overrides" do
    raft_data_dir = "tmp/runtime_config_raft"
    wal_data_dir = "tmp/runtime_config_wal"

    System.put_env(@raft_data_dir_env, raft_data_dir)
    System.put_env(@ra_wal_data_dir_env, wal_data_dir)
    System.put_env(@ra_wal_write_strategy_env, "sync_after_notify")
    System.put_env(@ra_wal_sync_method_env, "none")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    ra_config = Keyword.fetch!(config, :ra)

    assert Keyword.fetch!(ra_config, :data_dir) ==
             String.to_charlist(Path.join(raft_data_dir, "ra_system"))

    assert Keyword.fetch!(ra_config, :wal_data_dir) == String.to_charlist(wal_data_dir)
    assert Keyword.fetch!(ra_config, :wal_write_strategy) == :sync_after_notify
    assert Keyword.fetch!(ra_config, :wal_sync_method) == :none
  end

  test "runtime config rejects invalid Ra WAL write strategy" do
    System.put_env(@ra_wal_write_strategy_env, "bad")

    assert_raise ArgumentError, ~r/invalid wal write strategy/, fn ->
      Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    end
  end

  test "runtime config rejects invalid Ra WAL sync method" do
    System.put_env(@ra_wal_sync_method_env, "bad")

    assert_raise ArgumentError, ~r/invalid wal sync method/, fn ->
      Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    end
  end

  test "telemetry flag enables PromEx and telemetry emission" do
    System.put_env(@enable_telemetry_env, "true")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :telemetry_enabled) == true

    prom_ex_config = Keyword.fetch!(starcite_config, Starcite.Observability.PromEx)
    assert Keyword.fetch!(prom_ex_config, :enabled) == true
  end

  test "telemetry defaults to enabled when no env override is set" do
    System.delete_env(@enable_telemetry_env)

    assert Application.get_env(:starcite, :telemetry_enabled) == true

    prom_ex_config = Application.get_env(:starcite, Starcite.Observability.PromEx, [])
    assert Keyword.fetch!(prom_ex_config, :enabled) == true
  end

  test "telemetry flag disables PromEx and telemetry emission" do
    System.put_env(@enable_telemetry_env, "false")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :telemetry_enabled) == false

    prom_ex_config = Keyword.fetch!(starcite_config, Starcite.Observability.PromEx)
    assert Keyword.fetch!(prom_ex_config, :enabled) == false
  end

  defp restore_env(env_name, nil), do: System.delete_env(env_name)
  defp restore_env(env_name, value), do: System.put_env(env_name, value)
end
