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
