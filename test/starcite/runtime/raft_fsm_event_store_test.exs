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

  test "declares raft machine version mapping" do
    assert RaftFSM.version() == 1
    assert RaftFSM.which_module(0) == RaftFSM
    assert RaftFSM.which_module(1) == RaftFSM
    assert_raise ArgumentError, fn -> RaftFSM.which_module(99) end
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

    {next_state, {:reply, {:ok, %{seq: 1, deduped: false, epoch: epoch_one} = reply_one}},
     _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    assert is_integer(epoch_one) and epoch_one >= 0
    assert reply_one.cursor == %{epoch: epoch_one, seq: 1}
    assert reply_one.committed_cursor == %{epoch: epoch_one, seq: 0}

    {_, {:reply, {:ok, %{seq: 1, deduped: true, epoch: ^epoch_one} = deduped_reply}}} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        next_state
      )

    assert deduped_reply.cursor == %{epoch: epoch_one, seq: 1}
    assert deduped_reply.committed_cursor == %{epoch: epoch_one, seq: 0}

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

    assert %{seq: 1, last_seq: 1, deduped: false, epoch: first_epoch} = first
    assert first.cursor == %{epoch: first_epoch, seq: 1}
    assert first.committed_cursor == %{epoch: first_epoch, seq: 0}
    assert %{seq: 2, last_seq: 2, deduped: false, epoch: second_epoch} = second
    assert second.cursor == %{epoch: second_epoch, seq: 2}
    assert second.committed_cursor == %{epoch: second_epoch, seq: 0}
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

  test "append_event preserves deterministic state transition under event-store backpressure" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), nil}, state)

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    {next_state, {:reply, {:ok, %{seq: 2, deduped: false}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("two", producer_seq: 2), nil},
        state
      )

    refute next_state == state
    assert EventStore.session_size(session_id) == 2
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 2
    assert {:ok, event} = EventStore.get_event(session_id, 2)
    assert event.payload == %{text: "two"}
  end

  test "append_events preserves deterministic state transition under event-store backpressure" do
    with_env(:starcite, :event_store_capacity_check_interval, 1)
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, session_id, event_payload("one"), nil}, state)

    with_env(:starcite, :event_store_max_bytes, EventStore.memory_bytes())

    {next_state, {:reply, {:ok, %{results: [_second, _third], last_seq: 3}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_events, session_id,
         [event_payload("two", producer_seq: 2), event_payload("three", producer_seq: 3)], nil},
        state
      )

    refute next_state == state
    assert EventStore.session_size(session_id) == 3
    assert {:ok, session} = RaftFSM.query_session(next_state, session_id)
    assert session.last_seq == 3
    assert {:ok, two} = EventStore.get_event(session_id, 2)
    assert {:ok, three} = EventStore.get_event(session_id, 3)
    assert two.payload == %{text: "two"}
    assert three.payload == %{text: "three"}
  end

  defp seeded_state(session_id) do
    state = RaftFSM.init(%{group_id: 0})

    seed_session(state, session_id)
  end

  defp seed_session(state, session_id) do
    {seeded, {:reply, {:ok, _session}}} =
      RaftFSM.apply(
        nil,
        {:create_session, session_id, nil,
         %Starcite.Auth.Principal{tenant_id: "acme", id: "user-1", type: :user}, "acme", %{}},
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
