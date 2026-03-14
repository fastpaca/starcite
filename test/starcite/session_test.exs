defmodule Starcite.SessionTest do
  use ExUnit.Case, async: false

  alias Starcite.Session
  alias Starcite.Session.{Header, WriteState}

  describe "Session.new/2" do
    test "creates a session with default values" do
      session = Session.new("ses-1")

      assert session.id == "ses-1"
      assert session.last_seq == 0
      assert session.archived_seq == 0
      assert Session.tail_size(session) == 0
    end

    test "rejects per-session retention overrides" do
      assert_raise ArgumentError, "per-session tail_keep is no longer supported", fn ->
        Session.new("ses-1", tail_keep: 10)
      end

      assert_raise ArgumentError, "per-session producer_max_entries is no longer supported", fn ->
        Session.new("ses-1", producer_max_entries: 10)
      end
    end
  end

  describe "WriteState.append_event/2" do
    test "appends events and increments sequence" do
      write_state = WriteState.new(Session.new("ses-1"))

      {:appended, write_state, event1} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"text" => "hello"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, write_state, event2} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"text" => "world"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 2
        })

      assert event1.seq == 1
      assert event2.seq == 2
      assert WriteState.session(write_state).last_seq == 2
    end

    test "dedupes same producer sequence and payload" do
      write_state = WriteState.new(Session.new("ses-1"))

      input = %{
        type: "state",
        payload: %{"state" => "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      {:appended, write_state, event} = WriteState.append_event(write_state, input)
      {:deduped, write_state_after_dedupe, seq} = WriteState.append_event(write_state, input)

      assert seq == event.seq

      assert WriteState.session(write_state_after_dedupe).last_seq ==
               WriteState.session(write_state).last_seq
    end

    test "rejects producer sequence replay with different payload" do
      write_state = WriteState.new(Session.new("ses-1"))

      {:appended, write_state, _event} =
        WriteState.append_event(write_state, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:error, :producer_replay_conflict} =
               WriteState.append_event(write_state, %{
                 type: "state",
                 payload: %{"state" => "completed"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 1
               })
    end

    test "tracks first seen producer sequence and then enforces continuity" do
      write_state = WriteState.new(Session.new("ses-1"))

      {:appended, write_state, _event} =
        WriteState.append_event(write_state, %{
          type: "state",
          payload: %{"state" => "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 2
        })

      assert {:error, {:producer_seq_conflict, "writer-1", 3, 5}} =
               WriteState.append_event(write_state, %{
                 type: "state",
                 payload: %{"state" => "running"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 5
               })
    end

    test "bounds producer cursor index size and allows seq reset after eviction" do
      with_env(:starcite, :producer_max_entries, 2)
      write_state = WriteState.new(Session.new("ses-bounded-producers"))

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "p1",
          producer_seq: 1
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "p2",
          producer_seq: 1
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          producer_id: "p3",
          producer_seq: 1
        })

      retained = write_state.producer_cursors |> Map.keys() |> Enum.sort()
      assert retained == ["p2", "p3"]

      assert {:appended, write_state, replayed} =
               WriteState.append_event(write_state, %{
                 type: "content",
                 payload: %{"n" => 4},
                 actor: "a",
                 producer_id: "p1",
                 producer_seq: 1
               })

      assert replayed.seq == 4
      assert WriteState.session(write_state).last_seq == 4
    end
  end

  describe "WriteState.persist_ack/2" do
    test "advances archived cursor and updates tail retention accounting" do
      with_env(:starcite, :tail_keep, 2)
      write_state = WriteState.new(Session.new("ses-arch"))

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 2
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 3},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 3
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 4},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 4
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 5},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 5
        })

      {write_state, trimmed} = WriteState.persist_ack(write_state, 3)
      session = WriteState.session(write_state)

      assert session.archived_seq == 3
      assert trimmed == 1
      assert Session.tail_size(session) == 2
    end

    test "does not advance archived cursor beyond last sequence" do
      with_env(:starcite, :tail_keep, 2)
      write_state = WriteState.new(Session.new("ses-arch-clamp"))

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 1},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 1
        })

      {:appended, write_state, _} =
        WriteState.append_event(write_state, %{
          type: "content",
          payload: %{"n" => 2},
          actor: "a",
          producer_id: "writer-1",
          producer_seq: 2
        })

      {write_state, _trimmed} = WriteState.persist_ack(write_state, 10)
      session = WriteState.session(write_state)

      assert session.archived_seq == 2
      assert Session.tail_size(session) == 0
    end
  end

  describe "Session.to_map/2" do
    test "converts session and header to API map" do
      session = Session.new("ses-1", tenant_id: "acme")

      header =
        Header.new("ses-1", tenant_id: "acme", title: "Draft", metadata: %{workflow: "contract"})

      map = Session.to_map(session, header)

      assert map.id == "ses-1"
      assert map.title == "Draft"
      assert map.metadata == %{workflow: "contract"}
      assert map.last_seq == 0
      assert is_binary(map.created_at)
      assert is_binary(map.updated_at)
    end
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
