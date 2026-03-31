defmodule Starcite.SessionTest do
  use ExUnit.Case, async: true

  alias Starcite.Session
  alias Starcite.Session.Header

  describe "Session.new/2" do
    test "creates a session with default values" do
      session = Session.new("ses-1")

      assert session.id == "ses-1"
      assert session.last_seq == 0
      assert session.archived_seq == 0
      assert Session.tail_size(session) == 0
    end
  end

  describe "Session.append_event/3" do
    test "appends events and increments sequence" do
      session = Session.new("ses-1")
      producer_cursors = %{}

      {:appended, session, producer_cursors, event1} =
        Session.append_event(session, producer_cursors, %{
          type: "content",
          payload: %{"text" => "hello"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, session, _producer_cursors, event2} =
        Session.append_event(session, producer_cursors, %{
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
      producer_cursors = %{}

      input = %{
        type: "state",
        payload: %{"state" => "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      {:appended, session, producer_cursors, event} =
        Session.append_event(session, producer_cursors, input)

      {:deduped, session_after_dedupe, _producer_cursors, seq} =
        Session.append_event(session, producer_cursors, input)

      assert seq == event.seq
      assert session_after_dedupe.last_seq == session.last_seq
    end

    test "rejects producer sequence replay with different payload" do
      session = Session.new("ses-1")

      {:appended, session, producer_cursors, _event} =
        Session.append_event(session, %{}, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:error, :producer_replay_conflict} =
               Session.append_event(session, producer_cursors, %{
                 type: "state",
                 payload: %{"state" => "completed"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 1
               })
    end

    test "tracks first seen producer sequence and then enforces continuity" do
      session = Session.new("ses-1")

      {:appended, session, producer_cursors, _event} =
        Session.append_event(session, %{}, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 2
        })

      assert {:error, {:producer_seq_conflict, "writer-1", 3, 5}} =
               Session.append_event(session, producer_cursors, %{
                 type: "state",
                 payload: %{"state" => "running"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 5
               })
    end

    test "bounds producer cursor index size and allows seq reset after eviction" do
      previous = Application.get_env(:starcite, :producer_max_entries)
      Application.put_env(:starcite, :producer_max_entries, 2)

      on_exit(fn ->
        if previous == nil do
          Application.delete_env(:starcite, :producer_max_entries)
        else
          Application.put_env(:starcite, :producer_max_entries, previous)
        end
      end)

      session = Session.new("ses-bounded-producers")

      {:appended, session, producer_cursors, _} =
        Session.append_event(session, %{}, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "p1",
          producer_seq: 1
        })

      {:appended, session, producer_cursors, _} =
        Session.append_event(session, producer_cursors, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "p2",
          producer_seq: 1
        })

      {:appended, session, producer_cursors, _} =
        Session.append_event(session, producer_cursors, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          producer_id: "p3",
          producer_seq: 1
        })

      retained = producer_cursors |> Map.keys() |> Enum.sort()
      assert retained == ["p2", "p3"]

      assert {:appended, session, _producer_cursors, replayed} =
               Session.append_event(session, producer_cursors, %{
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
      previous = Application.get_env(:starcite, :tail_keep)
      Application.put_env(:starcite, :tail_keep, 2)

      on_exit(fn ->
        if previous == nil do
          Application.delete_env(:starcite, :tail_keep)
        else
          Application.put_env(:starcite, :tail_keep, previous)
        end
      end)

      session =
        Enum.reduce(1..5, Session.new("ses-arch"), fn seq, current ->
          {:appended, updated, _producer_cursors, _event} =
            Session.append_event(current, %{}, %{
              type: "content",
              payload: %{"n" => seq},
              actor: "a",
              producer_id: "writer-1",
              producer_seq: seq
            })

          updated
        end)

      {session, trimmed} = Session.persist_ack(session, 3)

      assert session.archived_seq == 3
      assert trimmed == 1
      assert Session.tail_size(session) == 2
    end

    test "does not advance archived cursor beyond last sequence" do
      previous = Application.get_env(:starcite, :tail_keep)
      Application.put_env(:starcite, :tail_keep, 2)

      on_exit(fn ->
        if previous == nil do
          Application.delete_env(:starcite, :tail_keep)
        else
          Application.put_env(:starcite, :tail_keep, previous)
        end
      end)

      session =
        Enum.reduce(1..2, Session.new("ses-arch-clamp"), fn seq, current ->
          {:appended, updated, _producer_cursors, _event} =
            Session.append_event(current, %{}, %{
              type: "content",
              payload: %{"n" => seq},
              actor: "a",
              producer_id: "writer-1",
              producer_seq: seq
            })

          updated
        end)

      {session, _trimmed} = Session.persist_ack(session, 10)

      assert session.archived_seq == 2
      assert Session.tail_size(session) == 0
    end
  end

  describe "Session.to_map/2" do
    test "converts session plus header to an API map" do
      header = Header.new("ses-1", title: "Draft", metadata: %{workflow: "contract"})
      session = Session.new_from_header(header)
      map = Session.to_map(session, header)

      assert map.id == "ses-1"
      assert map.title == "Draft"
      assert map.metadata == %{workflow: "contract"}
      assert map.last_seq == 0
      assert is_binary(map.created_at)
      assert is_binary(map.updated_at)
    end
  end
end
