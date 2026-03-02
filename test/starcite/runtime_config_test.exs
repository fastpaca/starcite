defmodule Starcite.RuntimeConfigTest do
  use ExUnit.Case, async: false

  @raft_data_dir_env "STARCITE_RAFT_DATA_DIR"
  @enable_telemetry_env "STARCITE_ENABLE_TELEMETRY"
  @session_freeze_hydrate_grace_polls_env "STARCITE_SESSION_FREEZE_HYDRATE_GRACE_POLLS"

  setup do
    original_raft_data_dir = System.get_env(@raft_data_dir_env)
    original_enable_telemetry = System.get_env(@enable_telemetry_env)

    original_session_freeze_hydrate_grace_polls =
      System.get_env(@session_freeze_hydrate_grace_polls_env)

    on_exit(fn ->
      restore_env(@raft_data_dir_env, original_raft_data_dir)
      restore_env(@enable_telemetry_env, original_enable_telemetry)

      restore_env(
        @session_freeze_hydrate_grace_polls_env,
        original_session_freeze_hydrate_grace_polls
      )
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

  test "session freeze hydrate grace polls env overrides runtime config" do
    System.put_env(@session_freeze_hydrate_grace_polls_env, "4")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :session_freeze_hydrate_grace_polls) == 4
  end

  test "session freeze hydrate grace polls env rejects negative values" do
    System.put_env(@session_freeze_hydrate_grace_polls_env, "-1")

    assert_raise ArgumentError, fn ->
      Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    end
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
