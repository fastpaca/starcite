defmodule Starcite.DataPlane.EventStoreTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Starcite.Repo
  alias Starcite.DataPlane.{EventStore, SessionStore}
  alias Starcite.Session
  alias Starcite.Session.Header
  alias Starcite.Storage.{EventArchive, SessionCatalog}

  setup do
    ensure_repo_sandbox()
    EventStore.clear()
    SessionStore.clear()

    on_exit(fn ->
      EventStore.clear()
      SessionStore.clear()
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
          tenant_id: "acme",
          source: nil,
          metadata: %{},
          refs: %{},
          idempotency_key: nil,
          inserted_at: inserted_at
        }
      end

    assert {:ok, 3} = EventArchive.write_events(rows)
    :ok = cache_session_tenant(session_id, "acme")

    assert {:ok, first} = EventStore.read_archived_events(session_id, 1, 3)
    assert Enum.map(first, & &1.seq) == [1, 2, 3]

    assert {3, _} =
             Repo.delete_all(from(event in "events", where: event.session_id == ^session_id))

    assert {:ok, second} = EventStore.read_archived_events(session_id, 1, 3)
    assert second == first
  end

  test "read_archived_events fills only missing cache gaps from persistence" do
    session_id = "ses-archive-gaps-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    rows = archived_rows(session_id, inserted_at, 1..6)
    assert {:ok, 6} = EventArchive.write_events(rows)
    :ok = cache_session_tenant(session_id, "acme")

    cached_subset =
      rows
      |> Enum.filter(&(&1.seq in [1, 2, 5]))
      |> Enum.map(&Map.delete(&1, :session_id))

    :ok = EventStore.cache_archived_events(session_id, cached_subset)
    assert {3, _} = delete_archived_rows(session_id, [1, 2, 5])

    assert {:ok, events} = EventStore.read_archived_events(session_id, 1, 6)
    assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5, 6]

    assert {3, _} = delete_archived_rows(session_id, [3, 4, 6])

    assert {:ok, again} = EventStore.read_archived_events(session_id, 1, 6)
    assert Enum.map(again, & &1.seq) == [1, 2, 3, 4, 5, 6]
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
             EventStore.put_event(pending_session_id, "acme", %{
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
      EventStore.put_event(session_id, "acme", %{
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
        EventStore.put_event(session_id, "acme", %{
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
        EventStore.put_event(session_id, "acme", %{
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
      EventStore.put_event(other_session, "acme", %{
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
      EventStore.put_event(session_a, "acme", %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_a, "acme", %{
        seq: 2,
        type: "content",
        payload: %{n: 2},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 2,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_b, "acme", %{
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
      EventStore.put_event(session_id, "acme", %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_id, "acme", %{
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
             EventStore.put_event(session_id, "acme", %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    assert :ok =
             EventStore.put_event(session_id, "acme", %{
               seq: 2,
               type: "content",
               payload: %{n: 2},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    assert EventStore.size() == 2
  end

  test "emits backpressure telemetry from deferred capacity checks" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = "ses-cap-telemetry-#{System.unique_integer([:positive, :monotonic])}"
    handler_id = attach_event_store_handler()

    on_exit(fn -> :telemetry.detach(handler_id) end)

    inserted_at = NaiveDateTime.utc_now()

    assert :ok =
             EventStore.put_event(session_id, "acme", %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    assert :ok =
             EventStore.put_event(session_id, "acme", %{
               seq: 2,
               type: "content",
               payload: %{n: 2},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    assert EventStore.size() == 2

    assert_receive {:event_store_backpressure, measurements, metadata}, 1_000
    assert measurements.count == 1
    assert metadata.session_id == session_id
    assert metadata.tenant_id == "acme"
    assert metadata.reason == :memory_limit
  end

  test "accepts committed writes even with tiny byte limit" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    with_env(:starcite, :event_store_max_bytes, 1)
    session_id = "ses-size-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             EventStore.put_event(session_id, "acme", %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: NaiveDateTime.utc_now()
             })

    with_env(:starcite, :event_store_max_bytes, 4_294_967_296)

    assert :ok =
             EventStore.put_event(session_id, "acme", %{
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

  defp attach_event_store_handler do
    handler_id = "event-store-backpressure-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :event_store, :backpressure],
        fn _event, measurements, metadata, pid ->
          send(pid, {:event_store_backpressure, measurements, metadata})
        end,
        test_pid
      )

    handler_id
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
        tenant_id: "acme",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end

  defp cache_session_tenant(session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: tenant_id))

    Session.new(session_id, tenant_id: tenant_id)
    |> SessionStore.put_session()
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
        tenant_id: "acme",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end

  defp delete_archived_rows(session_id, seqs)
       when is_binary(session_id) and session_id != "" and is_list(seqs) do
    Repo.delete_all(
      from(event in "events",
        where: event.session_id == ^session_id and event.seq in ^seqs
      )
    )
  end

  defp ensure_repo_sandbox do
    if Process.whereis(Repo) == nil do
      _pid = start_supervised!(Repo)
      :ok
    end

    case Ecto.Adapters.SQL.Sandbox.checkout(Repo) do
      :ok -> :ok
      {:already, _owner} -> :ok
    end

    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    :ok
  end
end
