defmodule Starcite.Runtime.RaftFSMPayloadPlaneTest do
  use ExUnit.Case, async: false

  alias Starcite.Runtime.{EventStore, RaftFSM}

  setup do
    EventStore.clear()
    old_mode = Application.get_env(:starcite, :payload_plane, :legacy)

    on_exit(fn ->
      Application.put_env(:starcite, :payload_plane, old_mode)
      EventStore.clear()
    end)

    :ok
  end

  test "legacy mode does not mirror appended events to ETS" do
    Application.put_env(:starcite, :payload_plane, :legacy)
    session_id = unique_session_id()

    state = seeded_state(session_id)

    {_, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("one"), []}, state)

    assert EventStore.size() == 0
    assert :error = EventStore.get_event(session_id, 1)
  end

  test "dual_write mode mirrors appended events to ETS" do
    Application.put_env(:starcite, :payload_plane, :dual_write)
    session_id = unique_session_id()

    state = seeded_state(session_id)

    {_, {:reply, {:ok, %{seq: 1}}}, _effects} =
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("one"), []}, state)

    assert {:ok, event} = EventStore.get_event(session_id, 1)
    assert event.payload == %{text: "one"}
  end

  test "invalid mode raises from apply/3" do
    Application.put_env(:starcite, :payload_plane, :unsupported)
    session_id = unique_session_id()

    state = seeded_state(session_id)

    assert_raise ArgumentError, fn ->
      RaftFSM.apply(nil, {:append_event, 0, session_id, event_payload("one"), []}, state)
    end
  end

  defp seeded_state(session_id) do
    state = RaftFSM.init(%{group_id: 0})

    {seeded, {:reply, {:ok, _session}}} =
      RaftFSM.apply(nil, {:create_session, 0, session_id, nil, %{}}, state)

    seeded
  end

  defp event_payload(text) do
    %{
      type: "content",
      payload: %{text: text},
      actor: "agent:test"
    }
  end

  defp unique_session_id do
    "ses-fsm-#{System.unique_integer([:positive, :monotonic])}"
  end
end
