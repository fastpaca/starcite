defmodule Starcite.Runtime.PayloadStoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime.PayloadStore

  setup do
    PayloadStore.clear()

    on_exit(fn ->
      PayloadStore.clear()
    end)

    :ok
  end

  test "stores and fetches events by {session_id, seq}" do
    session_id = "ses-store-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    :ok =
      PayloadStore.put_event(session_id, %{
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

    assert {:ok, event} = PayloadStore.get_event(session_id, 1)
    assert event.seq == 1
    assert event.payload == %{text: "hello"}
    assert event.inserted_at == inserted_at

    assert :error = PayloadStore.get_event(session_id, 2)
    assert PayloadStore.size() == 1
  end

  test "reads ordered ranges from cursor with limit" do
    session_id = "ses-range-#{System.unique_integer([:positive, :monotonic])}"

    for seq <- 1..5 do
      :ok =
        PayloadStore.put_event(session_id, %{
          seq: seq,
          type: "content",
          payload: %{n: seq},
          actor: "agent:test",
          inserted_at: NaiveDateTime.utc_now()
        })
    end

    events = PayloadStore.from_cursor(session_id, 2, 2)
    assert Enum.map(events, & &1.seq) == [3, 4]
  end

  test "deletes entries below floor sequence per session" do
    session_id = "ses-evict-#{System.unique_integer([:positive, :monotonic])}"
    other_session = "ses-other-#{System.unique_integer([:positive, :monotonic])}"

    for seq <- 1..4 do
      :ok =
        PayloadStore.put_event(session_id, %{
          seq: seq,
          type: "content",
          payload: %{n: seq},
          actor: "agent:test",
          inserted_at: NaiveDateTime.utc_now()
        })
    end

    :ok =
      PayloadStore.put_event(other_session, %{
        seq: 1,
        type: "content",
        payload: %{n: 1},
        actor: "agent:test",
        inserted_at: NaiveDateTime.utc_now()
      })

    assert 2 = PayloadStore.delete_below(session_id, 3)
    assert PayloadStore.session_size(session_id) == 2
    assert PayloadStore.session_size(other_session) == 1

    remaining = PayloadStore.from_cursor(session_id, 0, 10)
    assert Enum.map(remaining, & &1.seq) == [3, 4]
  end
end
