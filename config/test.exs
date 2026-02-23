import Config

# Configure database for tests
config :starcite, Starcite.Repo,
  url: "ecto://postgres:postgres@localhost:5432/starcite_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :starcite, StarciteWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "j00A2bogYM9pz50icwpf23zy58245sY8GUZ67/P1CqXOIxD0kbwNBPR4vjbfl82k",
  server: false

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Raft test configuration
config :starcite,
  raft_data_dir: "tmp/test_raft",
  raft_flush_interval_ms: 100,
  write_replication_factor: 1,
  write_node_ids: [:nonode@nohost],
  emit_routing_telemetry: true,
  emit_raft_command_result_telemetry: true,
  archive_name: Starcite.Runtime.Archive,
  archive_adapter: Starcite.Archive.Adapter.Postgres,
  archive_adapter_opts: [],
  archive_flush_interval_ms: 3_600_000
