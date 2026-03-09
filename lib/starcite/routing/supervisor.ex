defmodule Starcite.Routing.Supervisor do
  use Supervisor

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [
      # Task supervisor used by control-plane Raft bootstrap/recovery work.
      {Task.Supervisor, name: Starcite.RaftTaskSupervisor},
      # Control-plane Raft bootstrap/lifecycle coordinator.
      Starcite.Routing.LeaseBootstrap,
      Starcite.Routing.Observer
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
