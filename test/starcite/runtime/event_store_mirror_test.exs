defmodule Starcite.DataPlane.EventStoreMirrorTest do
  use ExUnit.Case, async: false

  alias Phoenix.PubSub
  alias Starcite.WritePath
  alias Starcite.DataPlane.{CursorUpdate, EventStore}

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
    {:ok, _} = WritePath.create_session(id: session_id)

    {:ok, %{seq: 1, deduped: false}} =
      WritePath.append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1
      })

    {:ok, %{seq: 1, deduped: true}} =
      WritePath.append_event(session_id, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1
      })

    assert {:ok, stored} = EventStore.get_event(session_id, 1)
    assert stored.payload == %{text: "one"}
    assert EventStore.session_size(session_id) == 1
  end

  test "publishes cursor updates with the committed event payload" do
    session_id = "ses-cursor-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = WritePath.create_session(id: session_id)

    :ok = PubSub.subscribe(Starcite.PubSub, CursorUpdate.topic(session_id))

    {:ok, %{seq: 1}} =
      WritePath.append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test",
        source: "agent",
        producer_id: "writer:test",
        producer_seq: 1
      })

    assert_receive {:cursor_update, update}, 1_000
    assert update.version == 1
    assert update.session_id == session_id
    assert update.seq == 1
    assert update.last_seq == 1
    assert update.type == "state"
    assert update.actor == "agent:test"
    assert update.source == "agent"

    assert %{seq: 1, type: "state", payload: %{state: "running"}, actor: "agent:test"} =
             update.event
  end
end
