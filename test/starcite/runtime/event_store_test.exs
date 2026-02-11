defmodule Starcite.Runtime.EventStoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime.EventStore

  setup do
    EventStore.clear()

    on_exit(fn ->
      EventStore.clear()
    end)

    :ok
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
          inserted_at: NaiveDateTime.utc_now()
        })
    end

    :ok =
      EventStore.put_event(other_session, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
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
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_a, %{
        seq: 2,
        type: "content",
        payload: %{n: 2},
        actor: "agent:test",
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_b, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
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
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_id, %{
        seq: 2,
        type: "content",
        payload: %{n: 2},
        actor: "agent:test",
        inserted_at: NaiveDateTime.utc_now()
      })

    assert {:ok, 2} = EventStore.max_seq(session_id)
    assert 2 = EventStore.delete_below(session_id, 3)
    assert EventStore.session_size(session_id) == 0
    assert :error = EventStore.max_seq(session_id)
    refute session_id in EventStore.session_ids()
  end

  test "enforces memory-based backpressure limit" do
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

    with_env(:starcite, :event_store_max_size, "#{EventStore.memory_bytes()}B")

    assert {:error, :event_store_backpressure} =
             EventStore.put_event(session_id, %{
               seq: 2,
               type: "content",
               payload: %{n: 2},
               actor: "agent:test",
               inserted_at: inserted_at
             })

    assert EventStore.size() == 1
  end

  test "parses MB and unit-suffixed size settings" do
    with_env(:starcite, :event_store_max_size, "1B")
    session_id = "ses-size-#{System.unique_integer([:positive, :monotonic])}"

    assert {:error, :event_store_backpressure} =
             EventStore.put_event(session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: NaiveDateTime.utc_now()
             })

    with_env(:starcite, :event_store_max_size, "4G")

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
               actor: "agent:test",
               inserted_at: NaiveDateTime.utc_now()
             })
  end

  test "skips memory limit when event store capacity check is disabled" do
    session_id = "ses-cap-off-#{System.unique_integer([:positive, :monotonic])}"
    with_env(:starcite, :event_store_max_size, "1B")
    with_env(:starcite, :event_store_capacity_check, false)

    assert :ok =
             EventStore.put_event(session_id, %{
               seq: 1,
               type: "content",
               payload: %{n: 1},
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
end
