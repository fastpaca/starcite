defmodule Starcite.Application do
  @moduledoc false

  use Application
  @archive_read_cache :starcite_archive_read_cache
  @archive_read_cache_default_compressed true

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])

    children =
      [
        # PromEx metrics
        Starcite.Observability.PromEx,
        # Cache used by Runtime.EventStore for flushed event reads
        archive_read_cache_spec(),
        # Ecto Repo for Postgres archive mode
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
    if Starcite.Archive.Store.adapter() == Starcite.Archive.Adapter.Postgres do
      Starcite.Repo
    else
      nil
    end
  end

  defp archive_read_cache_spec do
    options = [
      name: @archive_read_cache,
      compressed: archive_read_cache_compressed?()
    ]

    {Cachex, options}
  end

  defp pubsub_spec do
    {Phoenix.PubSub, name: Starcite.PubSub}
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
