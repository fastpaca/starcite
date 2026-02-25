defmodule Starcite.DataPlane.EventStoreTest do
  use ExUnit.Case, async: false

  defmodule FailingReadAdapter do
    @behaviour Starcite.Archive.Adapter

    use GenServer

    @impl true
    def start_link(_opts), do: GenServer.start_link(__MODULE__, %{})

    @impl true
    def init(state), do: {:ok, state}

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

  alias Starcite.Archive.IdempotentTestAdapter
  alias Starcite.DataPlane.EventStore

  setup do
    EventStore.clear()

    on_exit(fn ->
      EventStore.clear()
    end)

    :ok
  end

  test "get_event reads archived cache entries when pending ETS is empty" do
    session_id = "ses-cache-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    :ok =
      EventStore.cache_archived_events(session_id, [
        %{
          seq: 1,
          type: "content",
          payload: %{text: "from-cache"},
          actor: "agent:test",
          producer_id: "writer:test",
          producer_seq: 1,
          source: nil,
          metadata: %{},
          refs: %{},
          idempotency_key: nil,
          inserted_at: inserted_at
        }
      ])

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "from-cache"}
    assert :error = EventStore.get_event(session_id, 2)
  end

  test "read_archived_events caches persistence reads and avoids repeated adapter calls" do
    setup_idempotent_archive_adapter()

    session_id = "ses-archive-cache-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    rows =
      for seq <- 1..3 do
        %{
          session_id: session_id,
          seq: seq,
          type: "content",
          payload: %{n: seq},
          actor: "agent:test",
          producer_id: "writer:test",
          producer_seq: seq,
          source: nil,
          metadata: %{},
          refs: %{},
          idempotency_key: nil,
          inserted_at: inserted_at
        }
      end

    assert {:ok, 3} = IdempotentTestAdapter.write_events(rows)

    assert {:ok, first} = EventStore.read_archived_events(session_id, 1, 3)
    assert Enum.map(first, & &1.seq) == [1, 2, 3]
    assert IdempotentTestAdapter.get_reads() == [{session_id, 1, 3}]

    assert {:ok, second} = EventStore.read_archived_events(session_id, 1, 3)
    assert second == first
    assert IdempotentTestAdapter.get_reads() == [{session_id, 1, 3}]
  end

  test "read_archived_events fills only missing cache gaps from persistence" do
    setup_idempotent_archive_adapter()

    session_id = "ses-archive-gaps-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    rows = archived_rows(session_id, inserted_at, 1..6)
    assert {:ok, 6} = IdempotentTestAdapter.write_events(rows)

    cached_subset =
      rows
      |> Enum.filter(&(&1.seq in [1, 2, 5]))
      |> Enum.map(&Map.delete(&1, :session_id))

    :ok = EventStore.cache_archived_events(session_id, cached_subset)

    assert {:ok, events} = EventStore.read_archived_events(session_id, 1, 6)
    assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5, 6]
    assert IdempotentTestAdapter.get_reads() == [{session_id, 3, 4}, {session_id, 6, 6}]

    assert {:ok, again} = EventStore.read_archived_events(session_id, 1, 6)
    assert Enum.map(again, & &1.seq) == [1, 2, 3, 4, 5, 6]
    assert IdempotentTestAdapter.get_reads() == [{session_id, 3, 4}, {session_id, 6, 6}]
  end

  test "read_archived_events returns adapter read failures without normalization" do
    setup_archive_adapter(FailingReadAdapter)
    session_id = "ses-archive-read-fail-#{System.unique_integer([:positive, :monotonic])}"

    assert {:error, :db_down} = EventStore.read_archived_events(session_id, 1, 3)
  end

  test "reclaims archived cache before applying write backpressure" do
    cache_session_id = "ses-cache-pressure-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    :ok =
      EventStore.cache_archived_events(
        cache_session_id,
        archived_events_only(inserted_at, 1..256)
      )

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    pending_session_id = "ses-pending-pressure-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             EventStore.put_event(pending_session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               producer_id: "writer:test",
               producer_seq: 1,
               source: nil,
               metadata: %{},
               refs: %{},
               idempotency_key: nil,
               inserted_at: inserted_at
             })
  end

  test "stores and fetches events by {session_id, seq}" do
    session_id = "ses-store-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    :ok =
      EventStore.put_event(session_id, %{
        seq: 1,
        type: "content",
        payload: %{text: "hello"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        source: "agent",
        metadata: %{trace_id: "t-1"},
        refs: %{},
        idempotency_key: "k-1",
        inserted_at: inserted_at
      })

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.seq == 1
    assert event.payload == %{text: "hello"}
    assert event.inserted_at == inserted_at

    assert :error = EventStore.get_event(session_id, 2)
    assert EventStore.size() == 1
  end

  test "reads ordered ranges from cursor with limit" do
    session_id = "ses-range-#{System.unique_integer([:positive, :monotonic])}"

    for seq <- 1..5 do
      :ok =
        EventStore.put_event(session_id, %{
          seq: seq,
          type: "content",
          payload: %{n: seq},
          actor: "agent:test",
          producer_id: "writer:test",
          producer_seq: seq,
          inserted_at: NaiveDateTime.utc_now()
        })
    end

    events = EventStore.from_cursor(session_id, 2, 2)
    assert Enum.map(events, & &1.seq) == [3, 4]
  end

  test "deletes entries below floor sequence per session" do
    session_id = "ses-evict-#{System.unique_integer([:positive, :monotonic])}"
    other_session = "ses-other-#{System.unique_integer([:positive, :monotonic])}"

    for seq <- 1..4 do
      :ok =
        EventStore.put_event(session_id, %{
          seq: seq,
          type: "content",
          payload: %{n: seq},
          actor: "agent:test",
          producer_id: "writer:test",
          producer_seq: seq,
          inserted_at: NaiveDateTime.utc_now()
        })
    end

    :ok =
      EventStore.put_event(other_session, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    assert 2 = EventStore.delete_below(session_id, 3)
    assert EventStore.session_size(session_id) == 2
    assert EventStore.session_size(other_session) == 1

    remaining = EventStore.from_cursor(session_id, 0, 10)
    assert Enum.map(remaining, & &1.seq) == [3, 4]
  end

  test "tracks max sequence per session and returns indexed session ids" do
    session_a = "ses-index-a-#{System.unique_integer([:positive, :monotonic])}"
    session_b = "ses-index-b-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      EventStore.put_event(session_a, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_a, %{
        seq: 2,
        type: "content",
        payload: %{n: 2},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 2,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_b, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    assert {:ok, 2} = EventStore.max_seq(session_a)
    assert {:ok, 1} = EventStore.max_seq(session_b)
    assert :error = EventStore.max_seq("missing-session")

    session_ids = EventStore.session_ids() |> Enum.sort()
    assert session_ids == Enum.sort([session_a, session_b])
  end

  test "removes session index when all events are evicted" do
    session_id = "ses-index-clean-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      EventStore.put_event(session_id, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_id, %{
        seq: 2,
        type: "content",
        payload: %{n: 2},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 2,
        inserted_at: NaiveDateTime.utc_now()
      })

    assert {:ok, 2} = EventStore.max_seq(session_id)
    assert 2 = EventStore.delete_below(session_id, 3)
    assert EventStore.session_size(session_id) == 0
    assert :error = EventStore.max_seq(session_id)
    refute session_id in EventStore.session_ids()
  end

  test "accepts committed writes while under memory pressure" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = "ses-cap-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 2,
               type: "content",
               payload: %{n: 2},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    assert EventStore.size() == 2
  end

  test "accepts committed writes even with tiny byte limit" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    with_env(:starcite, :event_store_max_bytes, 1)
    session_id = "ses-size-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: NaiveDateTime.utc_now()
             })

    with_env(:starcite, :event_store_max_bytes, 4_294_967_296)

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 2,
               type: "content",
               payload: %{n: 2},
               actor: "agent:test",
               inserted_at: NaiveDateTime.utc_now()
             })
  end

  defp with_env(app, key, value) do
    previous = Application.get_env(app, key)
    Application.put_env(app, key, value)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(app, key)
      else
        Application.put_env(app, key, previous)
      end
    end)
  end

  defp setup_idempotent_archive_adapter do
    setup_archive_adapter(IdempotentTestAdapter)

    if pid = Process.whereis(IdempotentTestAdapter) do
      GenServer.stop(pid)
    end

    start_supervised!({IdempotentTestAdapter, []})
    :ok = IdempotentTestAdapter.clear_writes()
  end

  defp setup_archive_adapter(adapter_mod) when is_atom(adapter_mod) do
    previous_adapter = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, adapter_mod)

    on_exit(fn ->
      if previous_adapter do
        Application.put_env(:starcite, :archive_adapter, previous_adapter)
      else
        Application.delete_env(:starcite, :archive_adapter)
      end
    end)
  end

  defp archived_rows(session_id, inserted_at, seqs) do
    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{n: seq},
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

  defp archived_events_only(inserted_at, seqs) do
    Enum.map(seqs, fn seq ->
      %{
        seq: seq,
        type: "content",
        payload: %{n: seq},
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
