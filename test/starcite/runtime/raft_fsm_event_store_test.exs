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

  test "ack_archived evicts drained sessions and hydrate resets producer cursors" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {state,
     {:reply,
      {:ok, %{applied: [%{session_id: ^session_id, archived_seq: 1, trimmed: 1}], failed: []}}}} =
      RaftFSM.apply(nil, {:ack_archived, [{session_id, 1}]}, state)

    assert {:error, :session_not_found} = RaftFSM.query_session(state, session_id)

    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    hydrated_session =
      hydrated_session(session_id, inserted_at, "Hydrated", %{"source" => "archive"})

    {state, {:reply, {:ok, :hydrated}}} =
      RaftFSM.apply(
        nil,
        {:hydrate_session, hydrated_session},
        state
      )

    assert {:ok, hydrated} = RaftFSM.query_session(state, session_id)
    assert hydrated.last_seq == 1
    assert hydrated.archived_seq == 1
    assert hydrated.producer_cursors == %{}

    {_, {:reply, {:ok, :already_hot}}} =
      RaftFSM.apply(nil, {:hydrate_session, hydrated_session}, state)
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

  test "append_events accepts legacy opts tuple shape" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {_, {:reply, {:ok, %{results: [first], last_seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_events, session_id, [event_payload("one", producer_seq: 1)], [expected_seq: 0]},
        state
      )

    assert first == %{seq: 1, last_seq: 1, deduped: false}
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

    {next_state,
     {:reply,
      {:ok, %{applied: [%{session_id: ^session_id, archived_seq: 2, trimmed: 2}], failed: []}}}} =
      RaftFSM.apply(nil, {:ack_archived, [{session_id, 2}]}, state)

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

    {_,
     {:reply,
      {:ok, %{applied: [%{session_id: ^session_id, archived_seq: 1, trimmed: 1}], failed: []}}},
     effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, [{session_id, 1}]}, state)

    assert [{:release_cursor, 42, %RaftFSM{}}] = effects
  end

  test "ack_archived evicts drained session after cursor advances" do
    session_id = unique_session_id()
    state = seeded_state(session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {state,
     {:reply,
      {:ok, %{applied: [%{session_id: ^session_id, archived_seq: 1, trimmed: 1}], failed: []}}},
     _effects} =
      RaftFSM.apply(%{index: 42}, {:ack_archived, [{session_id, 1}]}, state)

    {_,
     {:reply,
      {:ok, %{applied: [], failed: [%{session_id: ^session_id, reason: :session_not_found}]}}}} =
      RaftFSM.apply(%{index: 43}, {:ack_archived, [{session_id, 1}]}, state)
  end

  test "ack_archived batch is best effort across sessions" do
    first_session_id = unique_session_id()
    second_session_id = unique_session_id()
    missing_session_id = unique_session_id()
    state = seeded_state(first_session_id) |> seed_session(second_session_id)

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, first_session_id, event_payload("one", producer_seq: 1), nil},
        state
      )

    {state, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(
        nil,
        {:append_event, second_session_id, event_payload("two", producer_seq: 1), nil},
        state
      )

    {next_state, {:reply, {:ok, %{applied: applied, failed: failed}}}} =
      RaftFSM.apply(
        nil,
        {:ack_archived, [{first_session_id, 1}, {missing_session_id, 1}, {second_session_id, 1}]},
        state
      )

    assert Enum.sort_by(applied, & &1.session_id) ==
             Enum.sort_by(
               [
                 %{session_id: first_session_id, archived_seq: 1, trimmed: 1},
                 %{session_id: second_session_id, archived_seq: 1, trimmed: 1}
               ],
               & &1.session_id
             )

    assert failed == [%{session_id: missing_session_id, reason: :session_not_found}]
    assert {:error, :session_not_found} = RaftFSM.query_session(next_state, first_session_id)
    assert {:error, :session_not_found} = RaftFSM.query_session(next_state, second_session_id)
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

  defp hydrated_session(session_id, inserted_at, title, metadata) do
    %Starcite.Session{
      id: session_id,
      title: title,
      creator_principal: %Starcite.Auth.Principal{
        tenant_id: "acme",
        id: "user-1",
        type: :user
      },
      tenant_id: "acme",
      metadata: metadata,
      last_seq: 1,
      archived_seq: 1,
      inserted_at: inserted_at,
      retention: %{tail_keep: 1000, producer_max_entries: 10_000},
      producer_cursors: %{}
    }
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
