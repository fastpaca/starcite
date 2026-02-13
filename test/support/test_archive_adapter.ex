defmodule Starcite.Archive.TestAdapter do
  @moduledoc false
  @behaviour Starcite.Archive.Adapter

  use GenServer

  @impl true
  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def write_events(rows) when is_list(rows), do: {:ok, length(rows)}

  @impl true
  def read_events(_session_id, _from_seq, _to_seq), do: {:ok, []}

  @impl true
  def upsert_session(_session), do: :ok

  @impl true
  def list_sessions(_query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}

  @impl true
  def list_sessions_by_ids(_ids, _query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}
end
