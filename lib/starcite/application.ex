defmodule Starcite.Application do
  @moduledoc false

  use Application
  @archive_read_cache :starcite_archive_read_cache

  @impl true
  def start(_type, _args) do
    :ok = Starcite.ControlPlane.WriteNodes.validate!()
    topologies = Application.get_env(:libcluster, :topologies, [])

    children =
      [
        # PromEx metrics
        Starcite.Observability.PromEx,
        # Cache used by DataPlane.EventStore for flushed event reads
        archive_read_cache_spec(),
        # Ecto Repo for Postgres archive mode
        repo_spec(),
        # PubSub before runtime
        pubsub_spec(),
        # DNS / clustering
        dns_cluster_spec(),
        # Data plane (Raft, observer, archive)
        Starcite.DataPlane.Supervisor
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
    case Application.get_env(:starcite, :archive_read_cache_compressed) do
      value when is_boolean(value) ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :archive_read_cache_compressed: #{inspect(value)} (expected true/false)"
    end
  end
end
