defmodule Starcite.Archive.StoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.{IdempotentTestAdapter, Store, TestAdapter}

  defmodule FailingReadAdapter do
    @behaviour Starcite.Archive.Adapter

    @impl true
    def start_link(_opts), do: {:ok, self()}

    @impl true
    def write_events(rows) when is_list(rows), do: {:ok, length(rows)}

    @impl true
    def read_events(_session_id, _from_seq, _to_seq), do: {:error, :db_down}

    @impl true
    def upsert_session(_session), do: :ok

    @impl true
    def list_sessions(_query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}

    @impl true
    def list_sessions_by_ids(_ids, _query_opts), do: {:ok, %{sessions: [], next_cursor: nil}}
  end

  setup do
    if pid = Process.whereis(IdempotentTestAdapter) do
      GenServer.stop(pid)
    end

    start_supervised!({IdempotentTestAdapter, []})
    :ok = IdempotentTestAdapter.clear_writes()

    previous_adapter = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, IdempotentTestAdapter)

    on_exit(fn ->
      if previous_adapter do
        Application.put_env(:starcite, :archive_adapter, previous_adapter)
      else
        Application.delete_env(:starcite, :archive_adapter)
      end
    end)

    :ok
  end

  test "reads through adapter each time" do
    session_id = "ses-store-#{System.unique_integer([:positive, :monotonic])}"
    rows = build_rows(session_id, 1..3)

    assert {:ok, 3} = Store.write_events(rows)

    assert {:ok, first} = Store.read_events(session_id, 1, 2)
    assert Enum.map(first, & &1.seq) == [1, 2]

    assert {:ok, second} = Store.read_events(session_id, 1, 2)
    assert second == first

    assert IdempotentTestAdapter.get_reads() == [{session_id, 1, 2}, {session_id, 1, 2}]
  end

  test "adapter selection is explicit per read call" do
    session_id = "ses-store-adapter-#{System.unique_integer([:positive, :monotonic])}"
    rows = build_rows(session_id, 1..1)

    assert {:ok, 1} = Store.write_events(rows)

    assert {:ok, hot} = Store.read_events(IdempotentTestAdapter, session_id, 1, 1)
    assert Enum.map(hot, & &1.seq) == [1]

    assert {:ok, []} = Store.read_events(TestAdapter, session_id, 1, 1)
  end

  test "returns persistence read failures without normalization" do
    session_id = "ses-store-fail-#{System.unique_integer([:positive, :monotonic])}"

    assert {:error, :db_down} =
             Store.read_events(FailingReadAdapter, session_id, 1, 10)
  end

  test "upserts and lists session catalog rows" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             Store.upsert_session(%{
               id: "ses-a",
               title: "A",
               metadata: %{"tenant_id" => "acme", "user_id" => "u1"},
               created_at: now
             })

    assert :ok =
             Store.upsert_session(%{
               id: "ses-b",
               title: "B",
               metadata: %{"tenant_id" => "acme", "user_id" => "u2"},
               created_at: now
             })

    assert {:ok, %{sessions: sessions, next_cursor: nil}} =
             Store.list_sessions(%{limit: 10, metadata: %{"tenant_id" => "acme"}})

    assert Enum.map(sessions, & &1.id) == ["ses-a", "ses-b"]

    assert {:ok, %{sessions: [session], next_cursor: nil}} =
             Store.list_sessions_by_ids(["ses-b"], %{limit: 10, metadata: %{}})

    assert session.id == "ses-b"
  end

  defp build_rows(session_id, seqs) do
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{text: "m#{seq}"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: seq,
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end
end
