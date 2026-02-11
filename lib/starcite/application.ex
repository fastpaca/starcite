defmodule Starcite.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    topologies = Application.get_env(:libcluster, :topologies, [])

    children =
      [
        # PromEx metrics
        Starcite.Observability.PromEx,
        # Ecto Repo for archive storage
        Starcite.Repo,
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
end
