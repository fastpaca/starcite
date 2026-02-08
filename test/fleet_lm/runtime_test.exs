defmodule FleetLM.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias FleetLM.Runtime
  alias FleetLM.Runtime.RaftManager

  setup do
    FleetLM.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{:rand.uniform(999_999_999)}"
  end

  describe "create/get session" do
    test "creates a new session with generated id" do
      {:ok, session} = Runtime.create_session(title: "Draft", metadata: %{workflow: "legal"})

      assert is_binary(session.id)
      assert String.starts_with?(session.id, "ses_")
      assert session.title == "Draft"
      assert session.metadata == %{workflow: "legal"}
      assert session.last_seq == 0
    end

    test "creates with caller-provided id and rejects duplicates" do
      id = unique_id("ses")

      {:ok, session} = Runtime.create_session(id: id, title: "Draft")
      assert session.id == id

      assert {:error, :session_exists} = Runtime.create_session(id: id)
    end

    test "gets existing session" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      {:ok, session} = Runtime.get_session(id)
      assert session.id == id
      assert session.last_seq == 0
    end

    test "returns not found for missing session" do
      assert {:error, :session_not_found} = Runtime.get_session("missing")
    end
  end

  describe "append_event/3" do
    test "appends events with monotonic seq" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      {:ok, r1} =
        Runtime.append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      {:ok, r2} =
        Runtime.append_event(id, %{
          type: "content",
          payload: %{text: "two"},
          actor: "agent:1"
        })

      assert r1.seq == 1
      assert r2.seq == 2
      assert r2.last_seq == 2
      refute r2.deduped
    end

    test "guards on expected_seq" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      {:ok, _} =
        Runtime.append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:error, {:expected_seq_conflict, 1}} =
               Runtime.append_event(
                 id,
                 %{type: "content", payload: %{text: "two"}, actor: "agent:1"},
                 expected_seq: 0
               )
    end

    test "dedupes when idempotency key repeats with same payload" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      event = %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:1",
        idempotency_key: "k1"
      }

      {:ok, first} = Runtime.append_event(id, event)
      {:ok, second} = Runtime.append_event(id, event)

      assert first.seq == second.seq
      assert second.deduped
      assert second.last_seq == 1
    end

    test "errors on idempotency conflict" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      {:ok, _} =
        Runtime.append_event(id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:1",
          idempotency_key: "k1"
        })

      assert {:error, :idempotency_conflict} =
               Runtime.append_event(id, %{
                 type: "state",
                 payload: %{state: "completed"},
                 actor: "agent:1",
                 idempotency_key: "k1"
               })
    end
  end

  describe "get_events_from_cursor/3" do
    test "returns events strictly after cursor" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          Runtime.append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      {:ok, events} = Runtime.get_events_from_cursor(id, 2, 100)
      assert Enum.map(events, & &1.seq) == [3, 4, 5]
    end
  end

  describe "Raft failover and recovery" do
    test "recovers state after server crash and restart" do
      id = unique_id("ses-failover")

      capture_log(fn ->
        {:ok, _} = Runtime.create_session(id: id)

        {:ok, first} =
          Runtime.append_event(id, %{
            type: "content",
            payload: %{text: "before crash"},
            actor: "agent:1"
          })

        assert first.seq == 1

        group_id = RaftManager.group_for_session(id)
        server_id = RaftManager.server_id(group_id)
        pid = Process.whereis(server_id)

        ref = Process.monitor(pid)
        Process.exit(pid, :kill)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          2_000 -> flunk("Raft process did not die")
        end

        assert RaftManager.start_group(group_id) in [:ok, {:error, :cluster_not_formed}]

        eventually(
          fn ->
            assert Process.whereis(server_id)
          end,
          timeout: 3_000
        )

        {:ok, second} =
          Runtime.append_event(id, %{
            type: "content",
            payload: %{text: "after crash"},
            actor: "agent:1"
          })

        assert second.seq >= 2

        {:ok, events} = Runtime.get_events_from_cursor(id, 0, 100)
        texts = Enum.map(events, &Map.get(&1.payload, :text))

        assert "before crash" in texts
        assert "after crash" in texts
      end)
    end
  end

  describe "Concurrent access" do
    test "multiple sessions can append concurrently" do
      ids =
        for i <- 1..10 do
          id = "ses-concurrent-#{i}"
          {:ok, _} = Runtime.create_session(id: id)
          id
        end

      tasks =
        Enum.map(ids, fn id ->
          Task.async(fn ->
            Runtime.append_event(id, %{
              type: "content",
              payload: %{text: "hello"},
              actor: "agent:1"
            })
          end)
        end)

      results = Task.await_many(tasks, 5_000)

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    try do
      fun.()
    rescue
      _ ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          fun.()
        end
    end
  end
end
