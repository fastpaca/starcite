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

parse_positive_integer! = fn env_name, raw ->
  case Integer.parse(String.trim(raw)) do
    {value, ""} when value > 0 -> value
    _ -> raise ArgumentError, "invalid integer for #{env_name}: #{inspect(raw)}"
  end
end

parse_non_neg_integer! = fn env_name, raw ->
  case Integer.parse(String.trim(raw)) do
    {value, ""} when value >= 0 -> value
    _ -> raise ArgumentError, "invalid integer for #{env_name}: #{inspect(raw)}"
  end
end

parse_node_ids! = fn env_name, raw ->
  nodes =
    raw
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.map(&String.to_atom/1)
    |> Enum.uniq()

  case nodes do
    [] ->
      raise ArgumentError,
            "invalid node list for #{env_name}: #{inspect(raw)} (expected comma-separated node ids)"

    _ ->
      nodes
  end
end

parse_fraction! = fn env_name, raw ->
  case Float.parse(String.trim(raw)) do
    {value, ""} when value >= 0.01 and value <= 0.99 ->
      value

    _ ->
      raise ArgumentError,
            "invalid float for #{env_name}: #{inspect(raw)} (expected 0.01..0.99)"
  end
end

if write_node_ids = System.get_env("STARCITE_WRITE_NODE_IDS") do
  config :starcite, :write_node_ids, parse_node_ids!.("STARCITE_WRITE_NODE_IDS", write_node_ids)
end

if num_groups = System.get_env("STARCITE_NUM_GROUPS") do
  config :starcite, :num_groups, parse_positive_integer!.("STARCITE_NUM_GROUPS", num_groups)
end

if write_replication_factor = System.get_env("STARCITE_WRITE_REPLICATION_FACTOR") do
  config :starcite,
         :write_replication_factor,
         parse_positive_integer!.(
           "STARCITE_WRITE_REPLICATION_FACTOR",
           write_replication_factor
         )
end

parse_size_bytes! = fn env_name, raw ->
  case Regex.run(~r/^\s*(\d+)\s*([a-zA-Z]*)\s*$/, raw) do
    [_, amount_raw, unit_raw] ->
      amount = String.to_integer(amount_raw)

      multiplier =
        case String.upcase(unit_raw) do
          "" -> 1_048_576
          "B" -> 1
          "K" -> 1_024
          "KB" -> 1_024
          "KIB" -> 1_024
          "M" -> 1_048_576
          "MB" -> 1_048_576
          "MIB" -> 1_048_576
          "G" -> 1_073_741_824
          "GB" -> 1_073_741_824
          "GIB" -> 1_073_741_824
          "T" -> 1_099_511_627_776
          "TB" -> 1_099_511_627_776
          "TIB" -> 1_099_511_627_776
          _ -> :invalid
        end

      case {amount, multiplier} do
        {value, mult} when is_integer(value) and value > 0 and is_integer(mult) ->
          value * mult

        _ ->
          raise ArgumentError,
                "invalid size for #{env_name}: #{inspect(raw)} (examples: 256MB, 4G, 1024M)"
      end

    _ ->
      raise ArgumentError,
            "invalid size for #{env_name}: #{inspect(raw)} (examples: 256MB, 4G, 1024M)"
  end
end

required_non_empty_env! = fn env_name ->
  case System.get_env(env_name) do
    value when is_binary(value) and value != "" ->
      value

    _ ->
      raise ArgumentError, "missing required environment variable #{env_name}"
  end
end

put_env_opt = fn opts, key, env_name, parser ->
  case System.get_env(env_name) do
    nil ->
      opts

    raw ->
      value = String.trim(raw)

      if value == "" do
        opts
      else
        Keyword.put(opts, key, parser.(value))
      end
  end
end

archive_adapter =
  case System.get_env("STARCITE_ARCHIVE_ADAPTER") do
    nil ->
      Application.get_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.S3)

    raw ->
      case raw |> String.trim() |> String.downcase() do
        "s3" ->
          Starcite.Archive.Adapter.S3

        "postgres" ->
          Starcite.Archive.Adapter.Postgres

        other ->
          raise ArgumentError,
                "invalid STARCITE_ARCHIVE_ADAPTER: #{inspect(other)} (expected s3|postgres)"
      end
  end

config :starcite, :archive_adapter, archive_adapter

archive_adapter_opts =
  case archive_adapter do
    Starcite.Archive.Adapter.S3 ->
      base_opts =
        Application.get_env(:starcite, :archive_adapter_opts, [])
        |> Keyword.new()

      s3_opts =
        []
        |> put_env_opt.(:bucket, "STARCITE_S3_BUCKET", & &1)
        |> put_env_opt.(:prefix, "STARCITE_S3_PREFIX", & &1)
        |> put_env_opt.(:region, "STARCITE_S3_REGION", & &1)
        |> put_env_opt.(:access_key_id, "STARCITE_S3_ACCESS_KEY_ID", & &1)
        |> put_env_opt.(:secret_access_key, "STARCITE_S3_SECRET_ACCESS_KEY", & &1)
        |> put_env_opt.(:security_token, "STARCITE_S3_SESSION_TOKEN", & &1)
        |> put_env_opt.(:endpoint, "STARCITE_S3_ENDPOINT", & &1)
        |> put_env_opt.(:max_write_retries, "STARCITE_S3_MAX_WRITE_RETRIES", fn value ->
          parse_positive_integer!.("STARCITE_S3_MAX_WRITE_RETRIES", value)
        end)
        |> put_env_opt.(:path_style, "STARCITE_S3_PATH_STYLE", fn value ->
          Starcite.Env.parse_bool!(value, "STARCITE_S3_PATH_STYLE")
        end)

      resolved = Keyword.merge(base_opts, s3_opts)
      config :starcite, :archive_adapter_opts, resolved
      resolved

    Starcite.Archive.Adapter.Postgres ->
      opts = Application.get_env(:starcite, :archive_adapter_opts, [])
      config :starcite, :archive_adapter_opts, opts
      opts
  end

if archive_flush_interval = System.get_env("STARCITE_ARCHIVE_FLUSH_INTERVAL_MS") do
  config :starcite,
         :archive_flush_interval_ms,
         parse_positive_integer!.("STARCITE_ARCHIVE_FLUSH_INTERVAL_MS", archive_flush_interval)
end

if event_store_max_size = System.get_env("STARCITE_EVENT_STORE_MAX_SIZE") do
  config :starcite,
         :event_store_max_bytes,
         parse_size_bytes!.("STARCITE_EVENT_STORE_MAX_SIZE", event_store_max_size)
end

if archive_read_cache_max_size = System.get_env("STARCITE_ARCHIVE_READ_CACHE_MAX_SIZE") do
  config :starcite,
         :archive_read_cache_max_bytes,
         parse_size_bytes!.("STARCITE_ARCHIVE_READ_CACHE_MAX_SIZE", archive_read_cache_max_size)
end

if archive_read_cache_reclaim_fraction =
     System.get_env("STARCITE_ARCHIVE_READ_CACHE_RECLAIM_FRACTION") do
  config :starcite,
         :archive_read_cache_reclaim_fraction,
         parse_fraction!.(
           "STARCITE_ARCHIVE_READ_CACHE_RECLAIM_FRACTION",
           archive_read_cache_reclaim_fraction
         )
end

auth_mode =
  case System.get_env("STARCITE_AUTH_MODE", "none") |> String.downcase() |> String.trim() do
    "none" ->
      :none

    "jwt" ->
      :jwt

    other ->
      raise ArgumentError, "invalid STARCITE_AUTH_MODE: #{inspect(other)} (expected none|jwt)"
  end

jwt_leeway_seconds =
  case System.get_env("STARCITE_AUTH_JWT_LEEWAY_SECONDS") do
    nil -> 30
    raw -> parse_non_neg_integer!.("STARCITE_AUTH_JWT_LEEWAY_SECONDS", raw)
  end

jwks_refresh_ms =
  case System.get_env("STARCITE_AUTH_JWKS_REFRESH_MS") do
    nil -> 60_000
    raw -> parse_positive_integer!.("STARCITE_AUTH_JWKS_REFRESH_MS", raw)
  end

principal_token_salt =
  case System.get_env("STARCITE_AUTH_PRINCIPAL_TOKEN_SALT") do
    nil -> "principal-token-v1"
    raw -> String.trim(raw)
  end

if principal_token_salt == "" do
  raise ArgumentError, "invalid STARCITE_AUTH_PRINCIPAL_TOKEN_SALT: cannot be empty"
end

# Default expiry for issued principal tokens when request omits `ttl_seconds`.
# Configurable via STARCITE_AUTH_PRINCIPAL_TOKEN_DEFAULT_TTL_SECONDS.
principal_token_default_ttl_seconds =
  case System.get_env("STARCITE_AUTH_PRINCIPAL_TOKEN_DEFAULT_TTL_SECONDS") do
    nil -> 600
    raw -> parse_positive_integer!.("STARCITE_AUTH_PRINCIPAL_TOKEN_DEFAULT_TTL_SECONDS", raw)
  end

# Hard upper bound for issued principal token expiry, even when caller supplies `ttl_seconds`.
# Configurable via STARCITE_AUTH_PRINCIPAL_TOKEN_MAX_TTL_SECONDS.
principal_token_max_ttl_seconds =
  case System.get_env("STARCITE_AUTH_PRINCIPAL_TOKEN_MAX_TTL_SECONDS") do
    nil -> 900
    raw -> parse_positive_integer!.("STARCITE_AUTH_PRINCIPAL_TOKEN_MAX_TTL_SECONDS", raw)
  end

if principal_token_default_ttl_seconds > principal_token_max_ttl_seconds do
  raise ArgumentError,
        "invalid principal token ttl settings: STARCITE_AUTH_PRINCIPAL_TOKEN_DEFAULT_TTL_SECONDS exceeds STARCITE_AUTH_PRINCIPAL_TOKEN_MAX_TTL_SECONDS"
end

auth_config =
  case auth_mode do
    :none ->
      [
        mode: :none,
        jwt_leeway_seconds: jwt_leeway_seconds,
        jwks_refresh_ms: jwks_refresh_ms,
        principal_token_salt: principal_token_salt,
        principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
        principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
      ]

    :jwt ->
      [
        mode: :jwt,
        issuer: required_non_empty_env!.("STARCITE_AUTH_JWT_ISSUER"),
        audience: required_non_empty_env!.("STARCITE_AUTH_JWT_AUDIENCE"),
        jwks_url: required_non_empty_env!.("STARCITE_AUTH_JWKS_URL"),
        jwt_leeway_seconds: jwt_leeway_seconds,
        jwks_refresh_ms: jwks_refresh_ms,
        principal_token_salt: principal_token_salt,
        principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
        principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
      ]
  end

config :starcite, StarciteWeb.Auth, auth_config

db_url = System.get_env("DATABASE_URL") || System.get_env("STARCITE_POSTGRES_URL")
repo_url = db_url || Keyword.get(Application.get_env(:starcite, Starcite.Repo, []), :url)

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

if config_env() == :prod do
  if archive_adapter == Starcite.Archive.Adapter.Postgres and repo_url in [nil, ""] do
    raise """
    environment variable DATABASE_URL or STARCITE_POSTGRES_URL is missing.
    Starcite requires a configured Postgres archive database in postgres mode.
    """
  end

  if archive_adapter == Starcite.Archive.Adapter.S3 do
    bucket = Keyword.get(archive_adapter_opts, :bucket)

    if bucket in [nil, ""] do
      raise """
      environment variable STARCITE_S3_BUCKET is missing.
      Starcite requires a configured S3 bucket in s3 mode.
      """
    end
  end

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
