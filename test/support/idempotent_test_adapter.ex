defmodule Starcite.Archive.IdempotentTestAdapter do
  @moduledoc """
  Test adapter for archive idempotency tests.

  Tracks all write attempts and stores them for inspection,
  allowing tests to verify idempotency behavior.
  """
  @behaviour Starcite.Archive.Adapter

  use GenServer

  @impl true
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{writes: [], write_batches: [], reads: []}}
  end

  @impl true
  def write_events(rows) when is_list(rows) do
    GenServer.call(__MODULE__, {:write, rows})
    {:ok, length(rows)}
  end

  @impl true
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and is_integer(from_seq) and is_integer(to_seq) do
    GenServer.call(__MODULE__, {:read, session_id, from_seq, to_seq})
  end

  def get_writes do
    GenServer.call(__MODULE__, :get_writes)
  end

  def clear_writes do
    GenServer.call(__MODULE__, :clear_writes)
  end

  def get_write_batches do
    GenServer.call(__MODULE__, :get_write_batches)
  end

  def get_reads do
    GenServer.call(__MODULE__, :get_reads)
  end

  @impl true
  def handle_call({:write, rows}, _from, state) do
    new_writes = state.writes ++ rows
    new_batches = state.write_batches ++ [rows]
    {:reply, :ok, %{state | writes: new_writes, write_batches: new_batches}}
  end

  @impl true
  def handle_call({:read, session_id, from_seq, to_seq}, _from, state) do
    rows =
      state.writes
      |> Enum.filter(fn row ->
        row.session_id == session_id and row.seq >= from_seq and row.seq <= to_seq
      end)
      |> Enum.sort_by(& &1.seq)
      |> Enum.uniq_by(& &1.seq)
      |> Enum.map(&to_event_map/1)

    reads = state.reads ++ [{session_id, from_seq, to_seq}]
    {:reply, {:ok, rows}, %{state | reads: reads}}
  end

  @impl true
  def handle_call(:get_writes, _from, state) do
    {:reply, state.writes, state}
  end

  @impl true
  def handle_call(:clear_writes, _from, state) do
    {:reply, :ok, %{state | writes: [], write_batches: [], reads: []}}
  end

  @impl true
  def handle_call(:get_write_batches, _from, state) do
    {:reply, state.write_batches, state}
  end

  @impl true
  def handle_call(:get_reads, _from, state) do
    {:reply, state.reads, state}
  end

  defp to_event_map(row) do
    %{
      seq: row.seq,
      type: row.type,
      payload: row.payload,
      actor: row.actor,
      source: row.source,
      metadata: row.metadata,
      refs: row.refs,
      idempotency_key: row.idempotency_key,
      inserted_at: row.inserted_at
    }
  end
end
