defmodule Starcite.Application do
  @moduledoc false

  use Application
  @archive_read_cache :starcite_archive_read_cache

  @impl true
  def start(_type, _args) do
    pprof_port = pprof_port()
    :ok = maybe_prepare_pprof(pprof_port)
    :ok = Starcite.ControlPlane.WriteNodes.validate!()
    topologies = Application.get_env(:libcluster, :topologies, [])

    children =
      [
        # PromEx metrics
        prom_ex_spec(),
        # Cache used by DataPlane.EventStore for flushed event reads
        archive_read_cache_spec(),
        # Ecto Repo for Postgres archive mode
        repo_spec(),
        # PubSub before runtime
        pubsub_spec(),
        # DNS / clustering
        dns_cluster_spec(),
        # Control-plane liveness and routing intent
        Starcite.ControlPlane.Supervisor,
        # Data-plane runtime (Raft bootstrap, archive, event store)
        Starcite.DataPlane.Supervisor
      ]
      |> Enum.concat(pprof_children(pprof_port))
      |> Enum.concat(cluster_children(topologies))
      |> Enum.reject(&is_nil/1)
      |> Enum.concat([
        # Endpoint last
        StarciteWeb.Endpoint
      ])

    opts = [strategy: :one_for_one, name: Starcite.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp prom_ex_spec do
    prom_ex_opts = Application.get_env(:starcite, Starcite.Observability.PromEx, [])
    metrics_server = Keyword.get(prom_ex_opts, :metrics_server, :disabled)
    grafana_agent = Keyword.get(prom_ex_opts, :grafana_agent, :disabled)

    if metrics_server == :disabled and grafana_agent == :disabled do
      nil
    else
      Starcite.Observability.PromEx
    end
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

  defp pprof_port do
    case Application.get_env(:starcite, :pprof_port) do
      nil ->
        nil

      port when is_integer(port) and port > 0 ->
        port

      value ->
        raise ArgumentError,
              "invalid value for :pprof_port: #{inspect(value)} (expected positive integer or nil)"
    end
  end

  defp maybe_prepare_pprof(nil), do: :ok

  defp maybe_prepare_pprof(_port) do
    case Application.load(:pprof) do
      :ok ->
        :ok

      {:error, {:already_loaded, :pprof}} ->
        :ok

      {:error, reason} ->
        raise "failed to load pprof: #{inspect(reason)}"
    end

    case Application.ensure_all_started(:plug_cowboy) do
      {:ok, _apps} ->
        :ok

      {:error, reason} ->
        raise "failed to start plug_cowboy for pprof: #{inspect(reason)}"
    end
  end

  defp pprof_children(nil), do: []

  defp pprof_children(port) do
    [
      {Pprof.Servers.Profile, []},
      {Plug.Cowboy, scheme: :http, plug: Starcite.Pprof.Router, port: port, compress: true}
    ]
  end
end
