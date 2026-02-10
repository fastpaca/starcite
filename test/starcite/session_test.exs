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

  describe "Session.persist_ack/2" do
    test "advances archived cursor and updates tail retention accounting" do
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

      assert session.archived_seq == 3
      assert trimmed == 1
      assert Session.tail_size(session) == 4
    end

    test "prunes idempotency keys below retained tail floor" do
      session = Session.new("ses-arch-idem", tail_keep: 2)

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          idempotency_key: "k1"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          idempotency_key: "k2"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          idempotency_key: "k3"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 4},
          actor: "a",
          idempotency_key: "k4"
        })

      {:appended, session, _} =
        Session.append_event(session, %{
          type: "content",
          payload: %{"n" => 5},
          actor: "a",
          idempotency_key: "k5"
        })

      {session, _trimmed} = Session.persist_ack(session, 3)
      retained_keys = session.idempotency_index |> Map.keys() |> Enum.sort()

      assert retained_keys == ["k2", "k3", "k4", "k5"]

      assert {:appended, session, replayed} =
               Session.append_event(session, %{
                 type: "content",
                 payload: %{"n" => 10},
                 actor: "a",
                 idempotency_key: "k1"
               })

      assert replayed.seq == 6
      assert session.idempotency_index["k1"].seq == 6
    end

    test "does not advance archived cursor beyond last sequence" do
      session = Session.new("ses-arch-clamp", tail_keep: 2)

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 1}, actor: "a"})

      {:appended, session, _} =
        Session.append_event(session, %{type: "content", payload: %{"n" => 2}, actor: "a"})

      {session, _trimmed} = Session.persist_ack(session, 10)

      assert session.archived_seq == 2
      assert Session.tail_size(session) == 2
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
