defmodule Starcite.Runtime.Supervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    archive_children =
      if archive_enabled?() do
        [
          {Starcite.Archive,
           [
             flush_interval_ms: archive_interval(),
             adapter: Starcite.Archive.Adapter.Postgres,
             adapter_opts: []
           ]}
        ]
      else
        []
      end

    children =
      [
        # Stable owner for ETS event mirror table
        {Starcite.Runtime.EventStore, []},
        # Task.Supervisor for async Raft group startup
        {Task.Supervisor, name: Starcite.RaftTaskSupervisor},
        # Topology coordinator (uses Erlang distribution, not Presence)
        Starcite.Runtime.RaftTopology
      ] ++ archive_children

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

  defp archive_enabled? do
    from_env =
      case System.get_env("STARCITE_ARCHIVER_ENABLED") do
        nil -> false
        val -> val not in ["", "false", "0", "no", "off"]
      end

    Application.get_env(:starcite, :archive_enabled, false) || from_env
  end
end
