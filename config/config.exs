# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :starcite,
  ecto_repos: [Starcite.Repo],
  routing_replication_factor: 3,
  cluster_node_ids: [:nonode@nohost],
  routing_store_id: :starcite_routing,
  routing_store_dir: "priv/khepri",
  shutdown_drain_timeout_ms: 30_000,
  telemetry_enabled: true,
  archive_flush_interval_ms: 5_000,
  archive_name: Starcite.Archive,
  event_archive_opts: [],
  event_store_cache_chunk_size: 256,
  event_store_max_bytes: 268_435_456,
  event_store_capacity_check_interval: 4,
  producer_max_entries: 1_024,
  session_store_ttl_ms: 21_600_000,
  session_store_purge_interval_ms: 60_000,
  session_store_compressed: true,
  session_store_touch_on_read: true,
  archive_read_cache_max_bytes: 67_108_864,
  archive_read_cache_reclaim_fraction: 0.25,
  archive_read_cache_compressed: true,
  ops_port: nil,
  pprof_port: nil,
  pprof_profile_timeout_ms: 60_000

# Configures the endpoint
config :starcite, StarciteWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Phoenix.Endpoint.Cowboy2Adapter,
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
  jwks_refresh_ms: :timer.seconds(60),
  jwks_hard_expiry_ms: :timer.seconds(60)

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
  enabled: true,
  metrics_server: :disabled,
  grafana_agent: :disabled

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
