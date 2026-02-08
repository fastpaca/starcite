defmodule FleetLM.Archive.IdempotentTestAdapter do
  @moduledoc """
  Test adapter for archive idempotency tests.

  Tracks all write attempts and stores them for inspection,
  allowing tests to verify idempotency behavior.
  """
  @behaviour FleetLM.Archive.Adapter

  use GenServer

  @impl true
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{writes: []}}
  end

  @impl true
  def write_events(rows) when is_list(rows) do
    GenServer.call(__MODULE__, {:write, rows})
    {:ok, length(rows)}
  end

  def get_writes do
    GenServer.call(__MODULE__, :get_writes)
  end

  def clear_writes do
    GenServer.call(__MODULE__, :clear_writes)
  end

  @impl true
  def handle_call({:write, rows}, _from, state) do
    new_writes = state.writes ++ rows
    {:reply, :ok, %{state | writes: new_writes}}
  end

  @impl true
  def handle_call(:get_writes, _from, state) do
    {:reply, state.writes, state}
  end

  @impl true
  def handle_call(:clear_writes, _from, state) do
    {:reply, :ok, %{state | writes: []}}
  end
end
