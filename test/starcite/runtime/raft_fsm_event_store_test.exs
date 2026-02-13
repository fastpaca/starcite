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
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), []}, state)

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "append_event dedupe does not create extra event-store entries" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {next_state, {:reply, {:ok, %{seq: 1, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", idempotency_key: "idem-1"), []},
        state
      )

    {_, {:reply, {:ok, %{seq: 1, deduped: true}}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", idempotency_key: "idem-1"), []},
        next_state
      )

    assert EventStore.session_size(session_id) == 1
    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "ack_archived updates archived sequence and evicts archived ETS entries" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), []}, state)

    {state, {:reply, {:ok, %{seq: 2}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("two"), []}, state)

    {state, {:reply, {:ok, %{seq: 3}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("three"), []}, state)

    assert {:ok, _} = EventStore.get_event(session_id, 1)
    assert {:ok, _} = EventStore.get_event(session_id, 2)
    assert {:ok, _} = EventStore.get_event(session_id, 3)

    {next_state, {:reply, {:ok, %{archived_seq: 2, trimmed: 2}}}} =
      RaftFSM.apply(nil, {:ack_archived, session_id, 2}, state)

    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.archived_seq == 2

    assert :error = EventStore.get_event(session_id, 1)
    assert :error = EventStore.get_event(session_id, 2)
    assert {:ok, event} = EventStore.get_event(session_id, 3)
    assert event.payload == %{text: "three"}
  end

  test "ack_archived emits release_cursor effect when raft meta index is available" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), []}, state)

    {_, {:reply, {:ok, %{archived_seq: 1, trimmed: 1}}}, effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, session_id, 1}, state)

    assert [{:release_cursor, 42, %RaftFSM{}}] = effects
  end

  test "ack_archived does not emit release_cursor when archive cursor does not advance" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), []}, state)

    {state, {:reply, {:ok, %{archived_seq: 1, trimmed: 1}}}, _effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, session_id, 1}, state)

    {_, {:reply, {:ok, %{archived_seq: 1, trimmed: 0}}}} =
      RaftFSM.apply(%{index: 43}, {:ack_archived, session_id, 1}, state)
  end

  test "append_event rejects writes under event-store backpressure without advancing session" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), []}, state)

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    {next_state, {:reply, {:error, :event_store_backpressure}}} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("two"), []}, state)

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  defp seeded_state(session_id) do
    state = RaftFSM.init(%{group_id: 0})

    {seeded, {:reply, {:ok, _session}}} =
      RaftFSM.apply(nil, {:create_session, session_id, nil, %{}}, state)

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
