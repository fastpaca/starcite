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
end
