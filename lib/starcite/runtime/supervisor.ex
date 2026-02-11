defmodule Starcite.Runtime.Supervisor do
  use Supervisor

  alias Starcite.Archive.Store

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children =
      [
        # Stable owner for ETS event mirror table
        {Starcite.Runtime.EventStore, []},
        # Task.Supervisor for async Raft group startup
        {Task.Supervisor, name: Starcite.RaftTaskSupervisor},
        # Topology coordinator (uses Erlang distribution, not Presence)
        Starcite.Runtime.RaftTopology,
        {Starcite.Archive,
         [
           name: archive_name(),
           flush_interval_ms: archive_interval(),
           adapter: Store.adapter(),
           adapter_opts: archive_adapter_opts()
         ]}
      ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp archive_interval do
    case Application.get_env(:starcite, :archive_flush_interval_ms) ||
           System.get_env("STARCITE_ARCHIVE_FLUSH_INTERVAL_MS") do
      nil -> 5_000
      val when is_integer(val) -> val
      val when is_binary(val) -> String.to_integer(val)
    end
  end

  defp archive_adapter_opts, do: Application.get_env(:starcite, :archive_adapter_opts, [])

  defp archive_name, do: Application.get_env(:starcite, :archive_name, Starcite.Archive)
end
