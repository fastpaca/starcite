defmodule Starcite.SessionTest do
  use ExUnit.Case, async: true

  alias Starcite.Session

  describe "Session.new/2" do
    test "creates a session with default values" do
      session = Session.new("ses-1")

      assert session.id == "ses-1"
      assert session.last_seq == 0
      assert session.archived_seq == 0
      assert session.metadata == %{}
      assert Session.tail_size(session) == 0
    end
  end

  describe "Session.append_event/2" do
    test "appends events and increments sequence" do
      session = Session.new("ses-1")

      {:appended, session, event1} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "hello"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, session, event2} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"text" => "world"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 2
        })

      assert event1.seq == 1
      assert event2.seq == 2
      assert session.last_seq == 2
    end

    test "dedupes same producer sequence and payload" do
      session = Session.new("ses-1")

      input = %{
        type: "state",
        payload: %{"state" => "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      {:appended, session, event} = Session.append_event(session, input)
      {:deduped, session_after_dedupe, seq} = Session.append_event(session, input)

      assert seq == event.seq
      assert session_after_dedupe.last_seq == session.last_seq
    end

    test "rejects producer sequence replay with different payload" do
      session = Session.new("ses-1")

      {:appended, session, _event} =
        Session.append_event(session, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:error, :producer_replay_conflict} =
               Session.append_event(session, %{
                 type: "state",
                 payload: %{"state" => "completed"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 1
               })
    end

    test "rejects producer sequence gap" do
      session = Session.new("ses-1")

      assert {:error, {:producer_seq_conflict, "writer-1", 1, 2}} =
               Session.append_event(session, %{
                 type: "state",
                 payload: %{"state" => "running"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 2
               })
    end

    test "bounds producer cursor index size and allows seq reset after eviction" do
      session = Session.new("ses-bounded-producers", producer_max_entries: 2)

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "p1",
          producer_seq: 1
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "p2",
          producer_seq: 1
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          producer_id: "p3",
          producer_seq: 1
        })

      retained = session.producer_cursors |> Map.keys() |> Enum.sort()
      assert retained == ["p2", "p3"]

      assert {:appended, session, replayed} =
               Session.append_event(session, %{
                 type: "content",
                 payload: %{"n" => 4},
                 actor: "a",
                 producer_id: "p1",
                 producer_seq: 1
               })

      assert replayed.seq == 4
      assert session.last_seq == 4
    end
  end

  describe "Session.persist_ack/2" do
    test "advances archived cursor and updates tail retention accounting" do
      session = Session.new("ses-arch", tail_keep: 2)

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 2
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 3
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 4},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 4
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 5},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 5
        })

      {session, trimmed} = Session.persist_ack(session, 3)

      assert session.archived_seq == 3
      assert trimmed == 1
      assert Session.tail_size(session) == 2
    end

    test "does not advance archived cursor beyond last sequence" do
      session = Session.new("ses-arch-clamp", tail_keep: 2)

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 2
        })

      {session, _trimmed} = Session.persist_ack(session, 10)

      assert session.archived_seq == 2
      assert Session.tail_size(session) == 0
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
