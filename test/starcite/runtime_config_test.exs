defmodule Starcite.RuntimeConfigTest do
  use ExUnit.Case, async: false

  @routing_store_dir_env "STARCITE_ROUTING_STORE_DIR"
  @cluster_nodes_env "CLUSTER_NODES"
  @routing_replication_factor_env "STARCITE_ROUTING_REPLICATION_FACTOR"
  @enable_telemetry_env "STARCITE_ENABLE_TELEMETRY"
  @shutdown_drain_timeout_env "STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS"
  @jwks_hard_expiry_env "STARCITE_AUTH_JWKS_HARD_EXPIRY_MS"
  @producer_max_entries_env "STARCITE_PRODUCER_MAX_ENTRIES"
  @runtime_envs [
    @routing_store_dir_env,
    @cluster_nodes_env,
    @routing_replication_factor_env,
    @enable_telemetry_env,
    @shutdown_drain_timeout_env,
    @jwks_hard_expiry_env,
    @producer_max_entries_env
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

  test "runtime config applies STARCITE_ROUTING_STORE_DIR" do
    routing_store_dir = "tmp/runtime_config_routing_store"
    System.put_env(@routing_store_dir_env, routing_store_dir)

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :routing_store_dir) == routing_store_dir
  end

  test "runtime config applies CLUSTER_NODES and replication factor overrides" do
    System.put_env(@cluster_nodes_env, "node-a@host,node-b@host,node-c@host")
    System.put_env(@routing_replication_factor_env, "2")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :cluster_node_ids) == [
             :"node-a@host",
             :"node-b@host",
             :"node-c@host"
           ]

    assert Keyword.fetch!(starcite_config, :routing_replication_factor) == 2
  end

  test "runtime config ignores blank CLUSTER_NODES" do
    System.put_env(@cluster_nodes_env, "   ")
    System.put_env(@routing_replication_factor_env, "3")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    refute Keyword.has_key?(starcite_config, :cluster_node_ids)
  end

  test "runtime config loads cluster node ids from CLUSTER_NODES" do
    System.put_env(@cluster_nodes_env, "node-a@host,node-b@host,node-c@host")
    System.put_env(@routing_replication_factor_env, "3")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :cluster_node_ids) == [
             :"node-a@host",
             :"node-b@host",
             :"node-c@host"
           ]
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

  test "runtime config applies shutdown drain timeout override" do
    System.put_env(@shutdown_drain_timeout_env, "45000")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :shutdown_drain_timeout_ms) == 45_000
  end

  test "runtime config applies jwks hard expiry override" do
    System.put_env(@jwks_hard_expiry_env, "15000")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)
    auth_config = Keyword.fetch!(starcite_config, StarciteWeb.Auth)

    assert Keyword.fetch!(auth_config, :jwks_hard_expiry_ms) == 15_000
  end

  test "runtime config applies producer index retention overrides" do
    System.put_env(@producer_max_entries_env, "256")

    config = Config.Reader.read!("config/runtime.exs", env: :test, target: :host)
    starcite_config = Keyword.fetch!(config, :starcite)

    assert Keyword.fetch!(starcite_config, :producer_max_entries) == 256
  end

  defp restore_env(env_name, nil), do: System.delete_env(env_name)
  defp restore_env(env_name, value), do: System.put_env(env_name, value)
end
