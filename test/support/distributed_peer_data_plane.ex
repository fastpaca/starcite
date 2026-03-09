defmodule Starcite.TestSupport.DistributedPeerDataPlane do
  @moduledoc false

  alias Starcite.DataPlane.{EventStore, SessionOwners, SessionStore}

  @registry Starcite.DataPlane.SessionOwnerRegistry
  @supervisor Starcite.DataPlane.SessionOwnerSupervisor

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
          5_000 -> {:error, :start_timeout}
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
          5_000 -> {:error, :stop_timeout}
        end

      nil ->
        :ok
    end
  end

  def clear do
    :ok = SessionOwners.clear()
    :ok = SessionStore.clear()
    :ok = EventStore.clear()
    :ok
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
    {:ok, _apps} = Application.ensure_all_started(:cachex)
    {:ok, _apps} = Application.ensure_all_started(:ra)
    :ok = start_process({Registry, keys: :unique, name: @registry})
    :ok = start_process({DynamicSupervisor, strategy: :one_for_one, name: @supervisor})
    :ok = start_process({SessionStore, []})
    :ok = start_process({EventStore, []})
    clear()
    :ok
  end

  defp stop_runtime do
    clear()
    :ok = stop_process(EventStore)
    :ok = stop_process(:starcite_session_store)
    :ok = stop_process(@supervisor)
    :ok = stop_process(@registry)
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
  defp start_child({SessionStore, opts}), do: SessionStore.start_link(opts)
  defp start_child({EventStore, opts}), do: EventStore.start_link(opts)

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
end
