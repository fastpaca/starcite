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
    {:ok, %{writes: [], write_batches: [], reads: [], sessions: %{}}}
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

  @impl true
  def upsert_session(session) when is_map(session) do
    GenServer.call(__MODULE__, {:upsert_session, session})
  end

  @impl true
  def list_sessions(query_opts) when is_map(query_opts) do
    GenServer.call(__MODULE__, {:list_sessions, query_opts})
  end

  @impl true
  def list_sessions_by_ids(ids, query_opts) when is_list(ids) and is_map(query_opts) do
    GenServer.call(__MODULE__, {:list_sessions_by_ids, ids, query_opts})
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

  def get_sessions do
    GenServer.call(__MODULE__, :get_sessions)
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
  def handle_call({:upsert_session, %{id: id} = session}, _from, state)
      when is_binary(id) and id != "" do
    normalized = normalize_session(session)
    {:reply, :ok, %{state | sessions: Map.put(state.sessions, id, normalized)}}
  end

  @impl true
  def handle_call({:upsert_session, _session}, _from, state) do
    {:reply, {:error, :invalid_session}, state}
  end

  @impl true
  def handle_call({:list_sessions, query_opts}, _from, state) do
    {:reply, {:ok, session_page(Map.values(state.sessions), query_opts)}, state}
  end

  @impl true
  def handle_call({:list_sessions_by_ids, ids, query_opts}, _from, state) do
    filtered =
      ids
      |> Enum.uniq()
      |> Enum.map(&Map.get(state.sessions, &1))
      |> Enum.reject(&is_nil/1)

    {:reply, {:ok, session_page(filtered, query_opts)}, state}
  end

  @impl true
  def handle_call(:get_writes, _from, state) do
    {:reply, state.writes, state}
  end

  @impl true
  def handle_call(:clear_writes, _from, state) do
    {:reply, :ok, %{state | writes: [], write_batches: [], reads: [], sessions: %{}}}
  end

  @impl true
  def handle_call(:get_write_batches, _from, state) do
    {:reply, state.write_batches, state}
  end

  @impl true
  def handle_call(:get_reads, _from, state) do
    {:reply, state.reads, state}
  end

  @impl true
  def handle_call(:get_sessions, _from, state) do
    {:reply, state.sessions, state}
  end

  defp to_event_map(row) do
    %{
      seq: row.seq,
      type: row.type,
      payload: row.payload,
      actor: row.actor,
      producer_id: row.producer_id,
      producer_seq: row.producer_seq,
      source: row.source,
      metadata: row.metadata,
      refs: row.refs,
      idempotency_key: row.idempotency_key,
      inserted_at: row.inserted_at
    }
  end

  defp normalize_session(%{id: id, title: title, metadata: metadata, created_at: created_at}) do
    %{
      id: id,
      title: title,
      metadata: metadata,
      created_at: created_at
    }
  end

  defp session_page(sessions, query_opts) do
    metadata_filters = query_opts |> Map.get(:metadata, %{}) |> normalize_metadata_filters()
    cursor = Map.get(query_opts, :cursor)
    limit = Map.get(query_opts, :limit, 100)

    sorted = sessions |> Enum.sort_by(& &1.id)

    filtered =
      sorted
      |> Enum.filter(fn session ->
        (is_nil(cursor) or session.id > cursor) and
          metadata_match?(session.metadata || %{}, metadata_filters)
      end)

    page = Enum.take(filtered, limit)

    %{
      sessions: page,
      next_cursor:
        if(length(page) == limit and length(filtered) > limit, do: List.last(page).id, else: nil)
    }
  end

  defp normalize_metadata_filters(filters) when is_map(filters) do
    Enum.reduce(filters, %{}, fn
      {key, value}, acc when is_binary(key) and not is_map(value) and not is_list(value) ->
        Map.put(acc, key, value)

      _, acc ->
        acc
    end)
  end

  defp metadata_match?(_metadata, filters) when map_size(filters) == 0, do: true

  defp metadata_match?(metadata, filters) when is_map(metadata) and is_map(filters) do
    Enum.all?(filters, fn {key, expected} ->
      Map.get(metadata, key) == expected
    end)
  end
end
