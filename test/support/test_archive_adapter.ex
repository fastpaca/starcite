defmodule FleetLM.Archive.TestAdapter do
  @moduledoc false
  @behaviour FleetLM.Archive.Adapter

  use GenServer

  @impl true
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def write_events(rows) when is_list(rows), do: {:ok, length(rows)}
end
