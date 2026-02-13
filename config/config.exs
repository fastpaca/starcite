# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :starcite,
  ecto_repos: [Starcite.Repo],
  raft_data_dir: "priv/raft",
  raft_flush_interval_ms: 5000,
  archive_flush_interval_ms: 5_000,
  archive_name: Starcite.Archive,
  archive_adapter: Starcite.Archive.Adapter.Postgres,
  archive_adapter_opts: [],
  event_store_max_bytes: 2_147_483_648,
  archive_read_cache_max_bytes: 536_870_912,
  archive_read_cache_reclaim_fraction: 0.25

# Configures the endpoint
config :starcite, StarciteWeb.Endpoint,
  url: [host: "localhost"],
  adapter: Bandit.PhoenixAdapter,
  render_errors: [
    formats: [json: StarciteWeb.ErrorJSON],
    layout: false
  ],
  pubsub_server: Starcite.PubSub

# Configures Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :libcluster, topologies: []

config :starcite, Starcite.Observability.PromEx,
  metrics_server: :disabled,
  grafana_agent: :disabled

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
