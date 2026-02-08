import Config

# config/runtime.exs is executed for all environments, including
# during releases. It is executed after compilation and before the
# system starts, so it is typically used to load production configuration
# and secrets from environment variables or elsewhere. Do not define
# any compile-time configuration in here, as it won't be applied.
# The block below contains prod specific runtime configuration.

# ## Using releases
#
# If you use `mix release`, you need to explicitly enable the server
# by passing the PHX_SERVER=true when you start it:
#
#     PHX_SERVER=true bin/starcite start
#
# Alternatively, you can use `mix phx.gen.release` to generate a `bin/server`
# script that automatically sets the env var above.
if System.get_env("PHX_SERVER") do
  config :starcite, StarciteWeb.Endpoint, server: true
end

# Suppress Ra (Raft library) verbose logs
# Call :logger.set_application_level(:ra, :error) at runtime to filter Ra logs
# This is set in RaftTopology.init/1

# Cluster configuration (works in all environments, not just prod)
cluster_nodes = System.get_env("CLUSTER_NODES")
enabled_env? = fn value -> value not in [nil, "", "false", "0", "no", "off"] end

if cluster_nodes && cluster_nodes != "" do
  hosts =
    cluster_nodes
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(&String.to_atom/1)

  config :libcluster,
    topologies: [
      local: [
        strategy: Cluster.Strategy.Epmd,
        config: [
          hosts: hosts,
          connect: {:net_kernel, :connect_node, []},
          disconnect: {:erlang, :disconnect_node, []},
          list_nodes: {:erlang, :nodes, [:connected]}
        ]
      ]
    ]
end

if raft_dir = System.get_env("STARCITE_RAFT_DATA_DIR") do
  config :starcite, :raft_data_dir, raft_dir
end

# Configure Postgres Repo at runtime when archiver is enabled
if enabled_env?.(System.get_env("STARCITE_ARCHIVER_ENABLED")) do
  db_url = System.get_env("DATABASE_URL") || System.get_env("STARCITE_POSTGRES_URL")

  if db_url && db_url != "" do
    pool_size =
      System.get_env("DB_POOL_SIZE", "10")
      |> String.to_integer()

    config :starcite, Starcite.Repo,
      url: db_url,
      pool_size: pool_size,
      queue_target: 5000,
      queue_interval: 1000
  end
end

if config_env() == :prod do
  # Optional: Override slot log directory (for mounting NVMe, etc)
  if slot_log_dir = System.get_env("SLOT_LOG_DIR") do
    config :starcite, :slot_log_dir, slot_log_dir
  end

  # The secret key base is used to sign/encrypt cookies and other secrets.
  # A default value is used in config/dev.exs and config/test.exs but you
  # want to use a different value for prod and you most likely don't want
  # to check this value into version control, so we use an environment
  # variable instead.
  secret_key_base =
    System.get_env("SECRET_KEY_BASE") ||
      raise """
      environment variable SECRET_KEY_BASE is missing.
      You can generate one by calling: mix phx.gen.secret
      """

  host = System.get_env("PHX_HOST") || "example.com"
  port = String.to_integer(System.get_env("PORT") || "4000")

  # Cluster configuration
  # For production: DNS_CLUSTER_QUERY=starcite-headless.default.svc.cluster.local
  # For local dev: CLUSTER_NODES=node1@localhost,node2@localhost
  dns_query = System.get_env("DNS_CLUSTER_QUERY")
  cluster_nodes = System.get_env("CLUSTER_NODES")

  dns_poll_interval =
    System.get_env("DNS_POLL_INTERVAL_MS", "5000")
    |> String.to_integer()

  topologies =
    cond do
      dns_query && dns_query != "" ->
        [
          starcite_dns: [
            strategy: Cluster.Strategy.DNSPoll,
            config: [
              polling_interval: dns_poll_interval,
              query: dns_query,
              node_basename: System.get_env("DNS_CLUSTER_NODE_BASENAME", "starcite")
            ]
          ]
        ]

      cluster_nodes && cluster_nodes != "" ->
        hosts =
          cluster_nodes
          |> String.split(",")
          |> Enum.map(&String.trim/1)
          |> Enum.map(&String.to_atom/1)

        [
          local: [
            strategy: Cluster.Strategy.Epmd,
            config: [hosts: hosts]
          ]
        ]

      true ->
        []
    end

  config :libcluster, topologies: topologies

  config :starcite, StarciteWeb.Endpoint,
    url: [host: host, port: 443, scheme: "https"],
    http: [
      # Bind on all interfaces (IPv6 :: supports both IPv4 and IPv6 via dual-stack)
      ip: {0, 0, 0, 0, 0, 0, 0, 0},
      port: port
    ],
    secret_key_base: secret_key_base

  # ## SSL Support
  #
  # To get SSL working, you will need to add the `https` key
  # to your endpoint configuration:
  #
  #     config :starcite, StarciteWeb.Endpoint,
  #       https: [
  #         ...,
  #         port: 443,
  #         cipher_suite: :strong,
  #         keyfile: System.get_env("SOME_APP_SSL_KEY_PATH"),
  #         certfile: System.get_env("SOME_APP_SSL_CERT_PATH")
  #       ]
  #
  # The `cipher_suite` is set to `:strong` to support only the
  # latest and more secure SSL ciphers. This means old browsers
  # and clients may not be supported. You can set it to
  # `:compatible` for wider support.
  #
  # `:keyfile` and `:certfile` expect an absolute path to the key
  # and cert in disk or a relative path inside priv, for example
  # "priv/ssl/server.key". For all supported SSL configuration
  # options, see https://hexdocs.pm/plug/Plug.SSL.html#configure/1
  #
  # We also recommend setting `force_ssl` in your config/prod.exs,
  # ensuring no data is ever sent via http, always redirecting to https:
  #
  #     config :starcite, StarciteWeb.Endpoint,
  #       force_ssl: [hsts: true]
  #
  # Check `Plug.SSL` for all available options in `force_ssl`.

  # ## Configuring the mailer
  #
  # In production you need to configure the mailer to use a different adapter.
  # Here is an example configuration for Mailgun:
  #
  #     config :starcite, Starcite.Mailer,
  #       adapter: Swoosh.Adapters.Mailgun,
  #       api_key: System.get_env("MAILGUN_API_KEY"),
  #       domain: System.get_env("MAILGUN_DOMAIN")
  #
  # Most non-SMTP adapters require an API client. Swoosh supports Req, Hackney,
  # and Finch out-of-the-box. This configuration is typically done at
  # compile-time in your config/prod.exs:
  #
  #     config :swoosh, :api_client, Swoosh.ApiClient.Req
  #
  # See https://hexdocs.pm/swoosh/Swoosh.html#module-installation for details.
end
