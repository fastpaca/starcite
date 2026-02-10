defmodule Starcite.Runtime.RaftFSMEventStoreTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime.{EventStore, RaftFSM}

  setup do
    EventStore.clear()

    on_exit(fn ->
      EventStore.clear()
    end)

    :ok
  end

  test "append_event mirrors appended events to ETS" do
    session_id = unique_session_id()

    state = seeded_state(session_id)

    {_, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("one"), []}, state)

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "append_event dedupe does not create extra event-store entries" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {next_state, {:reply, {:ok, %{seq: 1, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, 0, session_id, event_payload("one", idempotency_key: "idem-1"), []},
        state
      )

    {_, {:reply, {:ok, %{seq: 1, deduped: true}}}} =
      RaftFSM.apply(
        nil,
        {:append_event, 0, session_id, event_payload("one", idempotency_key: "idem-1"), []},
        next_state
      )

    assert EventStore.session_size(session_id) == 1
    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "ack_archived updates archived sequence without evicting ETS mirrored events" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("one"), []}, state)

    {state, {:reply, {:ok, %{seq: 2}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("two"), []}, state)

    {state, {:reply, {:ok, %{seq: 3}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("three"), []}, state)

    assert {:ok, _} = EventStore.get_event(session_id, 1)
    assert {:ok, _} = EventStore.get_event(session_id, 2)
    assert {:ok, _} = EventStore.get_event(session_id, 3)

    {next_state, {:reply, {:ok, %{archived_seq: 2, trimmed: _trimmed}}}} =
      RaftFSM.apply(nil, {:ack_archived, 0, session_id, 2}, state)

    assert {:ok, session} = RaftFSM.query_session(next_state, 0, session_id)
    assert session.archived_seq == 2

    assert {:ok, event_one} = EventStore.get_event(session_id, 1)
    assert event_one.payload == %{text: "one"}
    assert {:ok, event_two} = EventStore.get_event(session_id, 2)
    assert event_two.payload == %{text: "two"}
    assert {:ok, event} = EventStore.get_event(session_id, 3)
    assert event.payload == %{text: "three"}
  end

  defp seeded_state(session_id) do
    state = RaftFSM.init(%{group_id: 0})

    {seeded, {:reply, {:ok, _session}}} =
      RaftFSM.apply(nil, {:create_session, 0, session_id, nil, %{}}, state)

    seeded
  end

  defp event_payload(text, opts \\ []) do
    %{
      type: "content",
      payload: %{text: text},
      actor: "agent:test"
    }
    |> Map.merge(Enum.into(opts, %{}))
  end

  defp unique_session_id do
    "ses-fsm-#{System.unique_integer([:positive, :monotonic])}"
  end
end
