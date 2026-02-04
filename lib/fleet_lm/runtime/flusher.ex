defmodule FleetLM.Runtime.Flusher do
  @moduledoc """
  Disabled Postgres flusher (no-op).

  In FleetLM, all state lives in Raft. This module exists for compatibility
  but does nothing. Future versions may flush to Postgres for analytics.
  """

  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init([]) do
    {:ok, %{}}
  end
end
