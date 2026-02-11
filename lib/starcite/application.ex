defmodule Starcite.Application do
  @moduledoc false

  use Application
  require Cachex.Spec

  @archive_read_cache :starcite_archive_read_cache
  @archive_read_cache_default_ttl_ms :timer.minutes(10)
  @archive_read_cache_default_cleanup_interval_ms :timer.minutes(1)
  @archive_read_cache_default_compressed true

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])

    children =
      [
        # PromEx metrics
        Starcite.Observability.PromEx,
        # Cache fronting archive reads
        archive_read_cache_spec(),
        # Ecto Repo for archive storage (only if archiving is enabled)
        repo_spec(),
        # PubSub before runtime
        pubsub_spec(),
        # DNS / clustering
        dns_cluster_spec(),
        # Runtime (Raft etc.)
        Starcite.Runtime.Supervisor
      ]
      |> Enum.concat(cluster_children(topologies))
      |> Enum.reject(&is_nil/1)
      |> Enum.concat([
        # Endpoint last
        StarciteWeb.Endpoint
      ])

    opts = [strategy: :one_for_one, name: Starcite.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp repo_spec do
    if archive_enabled?() do
      Starcite.Repo
    else
      nil
    end
  end

  defp archive_read_cache_spec do
    options = [
      name: @archive_read_cache,
      compressed: archive_read_cache_compressed?(),
      expiration:
        Cachex.Spec.expiration(
          default: archive_read_cache_ttl_ms(),
          interval: archive_read_cache_cleanup_interval_ms()
        )
    ]

    options =
      case archive_read_cache_max_entries() do
        nil -> options
        max_entries -> Keyword.put(options, :limit, max_entries)
      end

    {Cachex, options}
  end

  defp archive_enabled? do
    from_env =
      case System.get_env("STARCITE_ARCHIVER_ENABLED") do
        nil -> false
        val -> val not in ["", "false", "0", "no", "off"]
      end

    Application.get_env(:starcite, :archive_enabled, false) || from_env
  end

  defp pubsub_spec do
    case Application.get_env(:starcite, :pubsub_adapter, :local) do
      :redis ->
        {Phoenix.PubSub,
         name: Starcite.PubSub,
         adapter: Phoenix.PubSub.Redis,
         host: System.get_env("REDIS_HOST", "localhost"),
         port: String.to_integer(System.get_env("REDIS_PORT", "6379")),
         node_name: System.get_env("NODE_NAME") || "starcite_#{:rand.uniform(1000)}"}

      _ ->
        {Phoenix.PubSub, name: Starcite.PubSub}
    end
  end

  defp cluster_children([]), do: []

  defp cluster_children(topologies) do
    [
      {Cluster.Supervisor, [topologies, [name: Starcite.ClusterSupervisor]]}
    ]
  end

  defp dns_cluster_spec do
    query = Application.get_env(:starcite, :dns_cluster_query) || :ignore

    if Code.ensure_loaded?(DNSCluster) and query != :ignore do
      {DNSCluster, query: query}
    else
      nil
    end
  end

  defp archive_read_cache_ttl_ms do
    env_int_or_default(
      "STARCITE_ARCHIVE_READ_CACHE_TTL_MS",
      Application.get_env(
        :starcite,
        :archive_read_cache_ttl_ms,
        @archive_read_cache_default_ttl_ms
      ),
      1
    )
  end

  defp archive_read_cache_cleanup_interval_ms do
    env_int_or_default(
      "STARCITE_ARCHIVE_READ_CACHE_CLEANUP_INTERVAL_MS",
      Application.get_env(
        :starcite,
        :archive_read_cache_cleanup_interval_ms,
        @archive_read_cache_default_cleanup_interval_ms
      ),
      1
    )
  end

  defp archive_read_cache_max_entries do
    env_int_or_default(
      "STARCITE_ARCHIVE_READ_CACHE_MAX_ENTRIES",
      Application.get_env(:starcite, :archive_read_cache_max_entries),
      1
    )
  end

  defp archive_read_cache_compressed? do
    env_bool_or_default(
      "STARCITE_ARCHIVE_READ_CACHE_COMPRESSED",
      Application.get_env(
        :starcite,
        :archive_read_cache_compressed,
        @archive_read_cache_default_compressed
      )
    )
  end

  defp env_int_or_default(env_key, default, min) when is_binary(env_key) and is_integer(min) do
    case System.get_env(env_key) do
      nil -> validate_int!(default, env_key, min)
      raw -> parse_int!(raw, env_key, min)
    end
  end

  defp validate_int!(nil, _env_key, _min), do: nil

  defp validate_int!(value, _env_key, min) when is_integer(value) and value >= min,
    do: value

  defp validate_int!(value, env_key, min) do
    raise ArgumentError,
          "invalid default integer for #{env_key}: #{inspect(value)} (expected >= #{min})"
  end

  defp parse_int!(raw, env_key, min) when is_binary(raw) do
    case Integer.parse(raw) do
      {value, ""} when value >= min ->
        value

      _ ->
        raise ArgumentError,
              "invalid integer for #{env_key}: #{inspect(raw)} (expected >= #{min})"
    end
  end

  defp env_bool_or_default(env_key, default) when is_binary(env_key) do
    case System.get_env(env_key) do
      nil -> validate_bool!(default, env_key)
      raw -> parse_bool!(raw, env_key)
    end
  end

  defp validate_bool!(value, _env_key) when is_boolean(value), do: value

  defp validate_bool!(value, env_key) do
    raise ArgumentError,
          "invalid default boolean for #{env_key}: #{inspect(value)} (expected true/false)"
  end

  defp parse_bool!(raw, env_key) when is_binary(raw) do
    normalized = String.downcase(String.trim(raw))

    case normalized do
      "1" -> true
      "true" -> true
      "yes" -> true
      "on" -> true
      "0" -> false
      "false" -> false
      "no" -> false
      "off" -> false
      _ -> raise ArgumentError, "invalid boolean for #{env_key}: #{inspect(raw)}"
    end
  end
end
