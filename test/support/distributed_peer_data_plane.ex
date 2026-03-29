defmodule Starcite.TestSupport.DistributedPeerDataPlane do
  @moduledoc false

  alias Starcite.DataPlane.{EventStore, SessionQuorum, SessionStore}
  alias Starcite.Routing.{Store, Watcher}
  alias Starcite.Storage.EventArchive

  @runtime_registry Starcite.DataPlane.SessionRuntimeRegistry
  @runtime_supervisor Starcite.DataPlane.SessionRuntimeSupervisor

  def start do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        :ok

      nil ->
        ref = make_ref()
        caller = self()

        _pid =
          spawn(fn ->
            run(caller, ref)
          end)

        receive do
          {^ref, :ok} -> :ok
          {^ref, {:error, reason}} -> {:error, reason}
        after
          15_000 -> {:error, :start_timeout}
        end
    end
  end

  def stop do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        ref = make_ref()
        send(pid, {:stop, self(), ref})

        receive do
          {^ref, :ok} -> :ok
        after
          15_000 -> {:error, :stop_timeout}
        end

      nil ->
        :ok
    end
  end

  def clear do
    :ok = SessionQuorum.clear()
    :ok = SessionStore.clear()
    :ok = EventStore.clear()
    :ok
  end

  def ready? do
    Enum.all?(
      [
        @runtime_registry,
        @runtime_supervisor,
        Starcite.PubSub,
        Store,
        Watcher,
        EventArchive,
        EventStore
      ],
      fn name ->
        is_pid(Process.whereis(name))
      end
    ) and is_pid(Process.whereis(:starcite_session_store))
  end

  defp run(caller, ref) when is_pid(caller) do
    Process.flag(:trap_exit, true)
    true = Process.register(self(), __MODULE__)

    :ok = start_runtime()
    send(caller, {ref, :ok})
    loop()
  end

  defp loop do
    receive do
      {:stop, caller, ref} when is_pid(caller) ->
        :ok = stop_runtime()
        send(caller, {ref, :ok})

      {:EXIT, _pid, _reason} ->
        loop()
    end
  end

  defp start_runtime do
    {:ok, _apps} = Application.ensure_all_started(:elixir)
    {:ok, _apps} = Application.ensure_all_started(:telemetry)
    {:ok, _apps} = Application.ensure_all_started(:cachex)
    {:ok, _apps} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _apps} = Application.ensure_all_started(:khepri)
    :ok = start_process({Registry, keys: :unique, name: @runtime_registry})
    :ok = start_process({DynamicSupervisor, strategy: :one_for_one, name: @runtime_supervisor})
    :ok = start_process({Phoenix.PubSub, name: Starcite.PubSub})
    :ok = start_process({Store, []})
    :ok = start_process({Watcher, []})
    :ok = start_process({SessionStore, []})
    :ok = start_process({EventArchive, event_archive_opts()})
    :ok = start_process({EventStore, []})
    :ok
  end

  defp stop_runtime do
    reset_store_membership()
    :ok = stop_process(EventStore)
    :ok = stop_process(EventArchive)
    :ok = stop_process(:starcite_session_store)
    :ok = stop_process(Watcher)
    :ok = stop_process(Store)
    :ok = stop_process(Starcite.PubSub)
    :ok = stop_process(@runtime_supervisor)
    :ok = stop_process(@runtime_registry)
    :ok
  end

  defp start_process({module, _opts} = child)
       when is_atom(module) do
    case start_child(child) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      other -> raise "failed to start peer child #{inspect(module)}: #{inspect(other)}"
    end
  end

  defp start_child({Registry, opts}), do: Registry.start_link(opts)
  defp start_child({DynamicSupervisor, opts}), do: DynamicSupervisor.start_link(opts)
  defp start_child({Phoenix.PubSub, opts}), do: Phoenix.PubSub.Supervisor.start_link(opts)
  defp start_child({Store, opts}), do: Store.start_link(opts)
  defp start_child({Watcher, opts}), do: Watcher.start_link(opts)
  defp start_child({SessionStore, opts}), do: SessionStore.start_link(opts)
  defp start_child({EventArchive, opts}), do: EventArchive.start_link(opts)
  defp start_child({EventStore, opts}), do: EventStore.start_link(opts)

  defp event_archive_opts do
    Application.get_env(:starcite, :event_archive_opts, [])
  end

  defp stop_process(name) when is_atom(name) do
    case Process.whereis(name) do
      pid when is_pid(pid) ->
        GenServer.stop(pid, :normal, 5_000)

      nil ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp reset_store_membership do
    if Store.running?() do
      case :khepri_cluster.reset(Store.store_id(), 15_000) do
        :ok -> :ok
        {:error, _reason} -> :ok
      end
    else
      :ok
    end
  end
end
