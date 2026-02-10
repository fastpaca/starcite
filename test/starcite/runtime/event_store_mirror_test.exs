defmodule Starcite.Runtime.EventStoreMirrorTest do
  use ExUnit.Case, async: false

  alias Phoenix.PubSub
  alias Starcite.Runtime
  alias Starcite.Runtime.{CursorUpdate, EventStore}

  setup do
    Starcite.Runtime.TestHelper.reset()
    EventStore.clear()

    on_exit(fn ->
      EventStore.clear()
    end)

    :ok
  end

  test "mirrors committed payloads into ETS" do
    session_id = "ses-dual-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.create_session(id: session_id)

    {:ok, %{seq: 1, deduped: false}} =
      Runtime.append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test",
        idempotency_key: "idem-1"
      })

    {:ok, %{seq: 1, deduped: true}} =
      Runtime.append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test",
        idempotency_key: "idem-1"
      })

    assert {:ok, stored} = EventStore.get_event(session_id, 1)
    assert stored.payload == %{text: "one"}
    assert EventStore.session_size(session_id) == 1
  end

  test "publishes payload-free cursor updates on the cursor topic" do
    session_id = "ses-cursor-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = Runtime.create_session(id: session_id)

    :ok = PubSub.subscribe(Starcite.PubSub, "session:#{session_id}")
    :ok = PubSub.subscribe(Starcite.PubSub, CursorUpdate.topic(session_id))
    :ok = PubSub.subscribe(Starcite.PubSub, CursorUpdate.global_topic())

    {:ok, %{seq: 1}} =
      Runtime.append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test",
        source: "agent"
      })

    assert_receive {:event, event}, 1_000
    assert event.seq == 1

    assert_receive {:cursor_update, update}, 1_000
    assert update.version == 1
    assert update.session_id == session_id
    assert update.seq == 1
    assert update.last_seq == 1
    assert update.type == "state"
    assert update.actor == "agent:test"
    assert update.source == "agent"
    refute Map.has_key?(update, :payload)

    assert_receive {:cursor_update, global_update}, 1_000
    assert global_update.session_id == session_id
    assert global_update.seq == 1
  end
end
