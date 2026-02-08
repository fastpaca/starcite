defmodule FleetLM.SessionTest do
  use ExUnit.Case, async: true

  alias FleetLM.Session
  alias FleetLM.Session.EventLog

  describe "Session.new/2" do
    test "creates a session with default values" do
      session = Session.new("ses-1")

      assert session.id == "ses-1"
      assert session.last_seq == 0
      assert session.archived_seq == 0
      assert session.metadata == %{}
      assert EventLog.entries(session.event_log) == []
    end
  end

  describe "Session.append_event/2" do
    test "appends events and increments sequence" do
      session = Session.new("ses-1")

      {:appended, session, event1} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "hello"},
          actor: "agent:1"
        })

      {:appended, session, event2} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "world"},
          actor: "agent:1"
        })

      assert event1.seq == 1
      assert event2.seq == 2
      assert session.last_seq == 2
    end

    test "dedupes same idempotency key and payload" do
      session = Session.new("ses-1")

      input = %{
        type: "state",
        payload: %{"state" => "running"},
        actor: "agent:1",
        idempotency_key: "k1"
      }

      {:appended, session, event} = Session.append_event(session, input)
      {:deduped, session_after_dedupe, seq} = Session.append_event(session, input)

      assert seq == event.seq
      assert session_after_dedupe.last_seq == session.last_seq
    end

    test "rejects idempotency key reuse with different payload" do
      session = Session.new("ses-1")

      {:appended, session, _event} =
        Session.append_event(session, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          idempotency_key: "k1"
        })

      assert {:error, :idempotency_conflict} =
               Session.append_event(session, %{
                 type: "state",
                 payload: %{"state" => "completed"},
                 actor: "agent:1",
                 idempotency_key: "k1"
               })
    end
  end

  describe "Session.events_from_cursor/3" do
    test "returns events strictly after cursor in seq order" do
      session = Session.new("ses-1")

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "one"},
          actor: "agent:1"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "two"},
          actor: "agent:1"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "three"},
          actor: "agent:1"
        })

      events = Session.events_from_cursor(session, 1, 100)
      assert Enum.map(events, & &1.seq) == [2, 3]
    end
  end

  describe "Session.persist_ack/2" do
    test "trims acknowledged range while retaining bounded tail" do
      session = Session.new("ses-arch", tail_keep: 2)

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 1}, actor: "a"})

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 2}, actor: "a"})

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 3}, actor: "a"})

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 4}, actor: "a"})

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 5}, actor: "a"})

      {session, trimmed} = Session.persist_ack(session, 3)
      kept = session.event_log |> EventLog.entries() |> Enum.map(& &1.seq)

      assert session.archived_seq == 3
      assert Enum.sort(kept) == [2, 3, 4, 5]
      assert trimmed == 1
    end
  end

  describe "Session.to_map/1" do
    test "converts session to API map" do
      session = Session.new("ses-1", title: "Draft", metadata: %{workflow: "contract"})
      map = Session.to_map(session)

      assert map.id == "ses-1"
      assert map.title == "Draft"
      assert map.metadata == %{workflow: "contract"}
      assert map.last_seq == 0
      assert is_binary(map.created_at)
      assert is_binary(map.updated_at)
    end
  end
end
