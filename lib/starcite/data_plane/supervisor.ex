defmodule Starcite.DataPlane.Supervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children =
      [
        # Session log registry (one log process per session replica)
        {Registry, keys: :unique, name: Starcite.DataPlane.SessionLogRegistry},
        # Dynamic supervisor for session log processes
        {DynamicSupervisor,
         strategy: :one_for_one, name: Starcite.DataPlane.SessionLogSupervisor},
        # Stable owner for Cachex-backed session store (control-plane/lifecycle use)
        {Starcite.DataPlane.SessionStore, []},
        # Stable owner for ETS event mirror table
        {Starcite.DataPlane.EventStore, []},
        {Starcite.Storage.EventArchive, event_archive_opts()},
        {Starcite.Archive,
         [
           name: archive_name(),
           flush_interval_ms: archive_interval()
         ]}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp archive_interval do
    case Application.get_env(:starcite, :archive_flush_interval_ms, 5_000) do
      val when is_integer(val) and val > 0 ->
        val

      val ->
        raise ArgumentError,
              "invalid value for archive_flush_interval_ms: #{inspect(val)} (expected positive integer)"
    end
  end

  defp event_archive_opts, do: Application.get_env(:starcite, :event_archive_opts, [])

  defp archive_name, do: Application.get_env(:starcite, :archive_name, Starcite.Archive)
end
