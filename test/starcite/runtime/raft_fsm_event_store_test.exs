defmodule Starcite.DataPlane.RaftFSMEventStoreTest do
  use ExUnit.Case, async: false

  alias Starcite.DataPlane.EventStore
  alias Starcite.DataPlane.RaftFSM

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
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "append_event dedupe does not create extra event-store entries" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {next_state, {:reply, {:ok, %{seq: 1, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {_, {:reply, {:ok, %{seq: 1, deduped: true}}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        next_state
      )

    assert EventStore.session_size(session_id) == 1
    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "append_event allows independent producers in the same session" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id,
         event_payload("one", producer_id: "writer-a", producer_seq: 1), nil},
        state
      )

    {_, {:reply, {:ok, %{seq: 2, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id,
         event_payload("two", producer_id: "writer-b", producer_seq: 1), nil},
        state
      )

    assert EventStore.session_size(session_id) == 2
    assert {:ok, one} = EventStore.get_event(session_id, 1)
    assert {:ok, two} = EventStore.get_event(session_id, 2)
    assert one.payload == %{text: "one"}
    assert two.payload == %{text: "two"}
  end

  test "append_events mirrors appended events to ETS" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {_, {:reply, {:ok, %{results: [first, second], last_seq: 2}}}, effects} =
      RaftFSM.apply(
        nil,
        {:append_events, session_id,
         [event_payload("one", producer_seq: 1), event_payload("two", producer_seq: 2)], nil},
        state
      )

    assert first == %{seq: 1, last_seq: 1, deduped: false}
    assert second == %{seq: 2, last_seq: 2, deduped: false}
    assert length(effects) == 2

    assert {:ok, one} = EventStore.get_event(session_id, 1)
    assert {:ok, two} = EventStore.get_event(session_id, 2)
    assert one.payload == %{text: "one"}
    assert two.payload == %{text: "two"}
  end

  test "append_events is all-or-nothing on producer sequence conflict" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {next_state, {:reply, {:error, {:producer_seq_conflict, "writer:test", 3, 4}}}} =
      RaftFSM.apply(
        nil,
        {:append_events, session_id,
         [event_payload("two", producer_seq: 2), event_payload("bad", producer_seq: 4)], nil},
        state
      )

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  test "append_event producer sequence conflict does not mutate state" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {next_state, {:reply, {:error, {:producer_seq_conflict, "writer:test", 2, 3}}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("bad", producer_seq: 3), nil},
        state
      )

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  test "append_event producer replay conflict does not mutate state" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {next_state, {:reply, {:error, :producer_replay_conflict}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("changed", producer_seq: 1), nil},
        state
      )

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  test "ack_archived updates archived sequence and evicts archived ETS entries" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {state, {:reply, {:ok, %{seq: 2}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("two", producer_seq: 2), nil},
        state
      )

    {state, {:reply, {:ok, %{seq: 3}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("three", producer_seq: 3), nil},
        state
      )

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
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {_, {:reply, {:ok, %{archived_seq: 1, trimmed: 1}}}, effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, session_id, 1}, state)

    assert [{:release_cursor, 42, %RaftFSM{}}] = effects
  end

  test "ack_archived does not emit release_cursor when archive cursor does not advance" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {state, {:reply, {:ok, %{archived_seq: 1, trimmed: 1}}}, _effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, session_id, 1}, state)

    {_, {:reply, {:ok, %{archived_seq: 1, trimmed: 0}}}} =
      RaftFSM.apply(%{index: 43}, {:ack_archived, session_id, 1}, state)
  end

  test "append_event rejects writes under event-store backpressure without advancing session" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), nil}, state)

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    {next_state, {:reply, {:error, :event_store_backpressure}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("two", producer_seq: 2), nil},
        state
      )

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  test "append_events rejects writes under event-store backpressure without partial writes" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), nil}, state)

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    {next_state, {:reply, {:error, :event_store_backpressure}}} =
      RaftFSM.apply(
        nil,
        {:append_events, session_id,
         [event_payload("two", producer_seq: 2), event_payload("three", producer_seq: 3)], nil},
        state
      )

    assert next_state == state
    assert EventStore.session_size(session_id) == 1
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 1
  end

  defp seeded_state(session_id) do
    state = RaftFSM.init(%{group_id: 0})

    {seeded, {:reply, {:ok, _session}}} =
      RaftFSM.apply(
        nil,
        {:create_session, session_id, nil,
         %Starcite.Auth.Principal{tenant_id: "acme", id: "user-1", type: :user}, %{}},
        state
      )

    seeded
  end

  defp event_payload(text, opts \\ []) do
    %{
      type: "content",
      payload: %{text: text},
      actor: "agent:test",
      producer_id: Keyword.get(opts, :producer_id, "writer:test"),
      producer_seq: Keyword.get(opts, :producer_seq, 1)
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
