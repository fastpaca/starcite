defmodule Starcite.Application do
  @moduledoc false

  use Application
  @archive_read_cache :starcite_archive_read_cache

  @impl true
  def start(_type, _args) do
    ops_port = ops_port()
    :ok = maybe_prepare_ops_server(ops_port)
    :ok = Starcite.Routing.Topology.validate!()
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
        Starcite.Routing.Supervisor,
        # Data-plane runtime (owners, archive, event store)
        Starcite.DataPlane.Supervisor
      ]
      |> Enum.concat(ops_children(ops_port))
      |> Enum.concat(cluster_children(topologies))
      |> Enum.reject(&is_nil/1)
      |> Enum.concat([
        # Endpoint last
        {StarciteWeb.Endpoint, adapter: Phoenix.Endpoint.Cowboy2Adapter}
      ])

    opts = [strategy: :one_for_one, name: Starcite.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @impl true
  def prep_stop(state) do
    maybe_drain_before_stop()
    state
  end

  defp prom_ex_spec do
    prom_ex_opts = Application.get_env(:starcite, Starcite.Observability.PromEx, [])
    enabled = Keyword.get(prom_ex_opts, :enabled, true)

    if enabled == true, do: Starcite.Observability.PromEx, else: nil
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

  defp ops_port do
    case Application.get_env(:starcite, :ops_port, Application.get_env(:starcite, :pprof_port)) do
      nil ->
        nil

      port when is_integer(port) and port > 0 ->
        port

      value ->
        raise ArgumentError,
              "invalid value for :ops_port: #{inspect(value)} (expected positive integer or nil)"
    end
  end

  defp maybe_prepare_ops_server(nil), do: :ok

  defp maybe_prepare_ops_server(_port) do
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
        raise "failed to start plug_cowboy for ops server: #{inspect(reason)}"
    end
  end

  defp ops_children(nil), do: []

  defp ops_children(port) do
    [
      {Pprof.Servers.Profile, []},
      {Plug.Cowboy, scheme: :http, plug: StarciteWeb.OpsRouter, port: port, compress: true}
    ]
  end

  defp maybe_drain_before_stop do
    if length(Starcite.Routing.Topology.nodes()) > 1 do
      _ = Starcite.Operations.drain_node(Node.self())
      _ = Starcite.Operations.wait_local_drained(drain_timeout_ms())
    end

    :ok
  end

  defp drain_timeout_ms do
    case Application.get_env(:starcite, :shutdown_drain_timeout_ms, 5_000) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :shutdown_drain_timeout_ms: #{inspect(value)} (expected positive integer)"
    end
  end
end
