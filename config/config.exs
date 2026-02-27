# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

default_raft_data_dir = "priv/raft"
default_ra_system_data_dir = Path.join(default_raft_data_dir, "ra_system")

config :starcite,
  ecto_repos: [Starcite.Repo],
  num_groups: 256,
  write_replication_factor: 3,
  write_node_ids: [:nonode@nohost],
  raft_data_dir: default_raft_data_dir,
  # Periodic RA checkpoint hint so persistence progress does not depend on archiver acks.
  raft_checkpoint_interval_entries: 2_048,
  raft_flush_interval_ms: 5000,
  emit_routing_telemetry: false,
  emit_event_append_telemetry: false,
  emit_event_store_write_telemetry: false,
  emit_raft_command_result_telemetry: false,
  use_ra_pipeline: true,
  route_leader_probe_on_miss: false,
  route_leader_cache_ttl_ms: 10_000,
  archive_flush_interval_ms: 5_000,
  archive_name: Starcite.Archive,
  archive_adapter: Starcite.Archive.Adapter.S3,
  archive_adapter_opts: [],
  event_store_cache_chunk_size: 256,
  event_store_max_bytes: 2_147_483_648,
  event_store_capacity_check_interval: 4,
  session_store_ttl_ms: 21_600_000,
  session_store_purge_interval_ms: 60_000,
  session_store_compressed: true,
  session_store_touch_on_read: true,
  archive_read_cache_max_bytes: 536_870_912,
  archive_read_cache_reclaim_fraction: 0.25,
  archive_read_cache_compressed: true,
  pprof_port: nil,
  pprof_profile_timeout_ms: 60_000

config :ra,
  data_dir: String.to_charlist(default_ra_system_data_dir),
  wal_data_dir: String.to_charlist(default_ra_system_data_dir),
  wal_write_strategy: :o_sync,
  wal_sync_method: :datasync,
  wal_compute_checksums: true,
  segment_compute_checksums: true,
  low_priority_commands_flush_size: 1_024,
  wal_max_batch_size: 262_144,
  default_max_pipeline_count: 16_384,
  default_max_append_entries_rpc_batch_size: 512,
  server_message_queue_data: :off_heap,
  wal_min_heap_size: 1_024,
  server_min_heap_size: 1_024,
  wal_min_bin_vheap_size: 131_072,
  server_min_bin_vheap_size: 131_072

# Configures the endpoint
config :starcite, StarciteWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [json: StarciteWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Starcite.PubSub

config :starcite, StarciteWeb.Auth,
  issuer: "https://issuer.example",
  audience: "starcite-api",
  jwks_url: "http://localhost:4000/.well-known/jwks.json",
  jwt_leeway_seconds: 1,
  jwks_refresh_ms: :timer.seconds(60)

# Configures Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason
config :phoenix, :filter_parameters, ["password", "access_token"]

config :ex_aws,
  http_client: ExAws.Request.Req,
  json_codec: Jason

config :ex_aws, :req_opts, receive_timeout: 30_000

config :libcluster, topologies: []

config :starcite, Starcite.Observability.PromEx,
  metrics_server: :disabled,
  grafana_agent: :disabled

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
