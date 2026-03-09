defmodule Starcite.Routing.Supervisor do
  use Supervisor

  alias Starcite.Routing.{Store, Watcher}

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_arg) do
    children = [Store, Watcher]
    Supervisor.init(children, strategy: :one_for_one)
  end
end
