defmodule Starcite.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Starcite.Auth.Principal
  alias Starcite.ControlPlane.WriteNodes
  alias Starcite.{ReadPath, WritePath}
  alias Starcite.Archive.Store
  alias Starcite.DataPlane.{EventStore, RaftAccess, RaftBootstrap, SessionStore}
  alias Starcite.DataPlane.RaftManager
  alias Starcite.Session
  alias Starcite.Repo

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{:rand.uniform(999_999_999)}"
  end

  describe "create/get session" do
    test "creates a new session with generated id" do
      {:ok, session} = WritePath.create_session(title: "Draft", metadata: %{workflow: "legal"})

      assert is_binary(session.id)
      assert String.starts_with?(session.id, "ses_")
      assert session.title == "Draft"
      assert session.metadata == %{workflow: "legal"}
      assert session.last_seq == 0
    end

    test "creates with caller-provided id and rejects duplicates" do
      id = unique_id("ses")

      {:ok, session} = WritePath.create_session(id: id, title: "Draft")
      assert session.id == id

      assert {:error, :session_exists} = WritePath.create_session(id: id)
    end

    test "gets existing session" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(id)
      {:ok, session} = RaftAccess.query_session(server_id, id)
      assert session.id == id
      assert session.last_seq == 0
    end

    test "returns not found for missing session" do
      {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started("missing")
      assert {:error, :session_not_found} = RaftAccess.query_session(server_id, "missing")
    end

    test "create_session warms session store for immediate reads" do
      id = unique_id("ses")
      principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}
      {:ok, _session} = WritePath.create_session(id: id, creator_principal: principal)
      assert {:ok, loaded} = SessionStore.get_session(id)
      assert loaded.id == id
      assert loaded.creator_principal == principal
    end

    test "auth lookup returns session store hit without raft/archive read-through" do
      id = unique_id("ses")
      principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}
      session = Session.new(id, creator_principal: principal, metadata: %{"source" => "cache"})
      assert :ok = SessionStore.put_session(session)

      assert {:ok, loaded} = ReadPath.get_session(id)
      assert loaded.id == id
      assert loaded.creator_principal == principal
      assert loaded.metadata["source"] == "cache"
    end
  end

  describe "append_event/3" do
    test "appends events with monotonic seq" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, r1} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      {:ok, r2} =
        append_event(id, %{
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
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:error, {:expected_seq_conflict, 0, 1}} =
               append_event(
                 id,
                 %{type: "content", payload: %{text: "two"}, actor: "agent:1"},
                 expected_seq: 0
               )
    end

    test "dedupes when producer sequence repeats with same payload" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      event = %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:1",
        producer_id: "writer-1",
        producer_seq: 1
      }

      {:ok, first} = append_event(id, event)
      {:ok, second} = append_event(id, event)

      assert first.seq == second.seq
      assert second.deduped
      assert second.last_seq == 1
    end

    test "errors on producer replay conflict" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:1",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:error, :producer_replay_conflict} =
               append_event(id, %{
                 type: "state",
                 payload: %{state: "completed"},
                 actor: "agent:1",
                 producer_id: "writer-1",
                 producer_seq: 1
               })
    end
  end

  describe "append_events/3" do
    test "appends a batch with contiguous sequence numbers" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, reply} =
        WritePath.append_events(id, [
          %{
            type: "content",
            payload: %{text: "one"},
            actor: "agent:1",
            producer_id: "writer:test",
            producer_seq: 1
          },
          %{
            type: "content",
            payload: %{text: "two"},
            actor: "agent:1",
            producer_id: "writer:test",
            producer_seq: 2
          }
        ])

      assert reply.last_seq == 2
      assert Enum.map(reply.results, & &1.seq) == [1, 2]
      assert Enum.map(reply.results, & &1.last_seq) == [1, 2]
      refute Enum.any?(reply.results, & &1.deduped)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 10)
      assert Enum.map(events, & &1.seq) == [1, 2]
      assert Enum.map(events, & &1.payload) == [%{text: "one"}, %{text: "two"}]
    end

    test "guards on expected_seq for a batch append" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:error, {:expected_seq_conflict, 0, 1}} =
               WritePath.append_events(
                 id,
                 [
                   %{
                     type: "content",
                     payload: %{text: "two"},
                     actor: "agent:1",
                     producer_id: "writer:test",
                     producer_seq: 2
                   }
                 ],
                 expected_seq: 0
               )
    end
  end

  describe "get_events_from_cursor/3" do
    test "returns events strictly after cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      {:ok, events} = ReadPath.get_events_from_cursor(id, 2, 100)
      assert Enum.map(events, & &1.seq) == [3, 4, 5]
    end

    test "returns only unarchived events after archive cutover compacts ETS" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 3, trimmed: 3}], failed: []}} =
               WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 3, 100)
      assert Enum.map(events, & &1.seq) == [4, 5]
    end

    test "returns hot events even when session metadata is unavailable" do
      missing_id = unique_id("missing")

      :ok =
        EventStore.put_event(missing_id, "acme", %{
          seq: 1,
          type: "content",
          payload: %{text: "rogue"},
          actor: "agent:1",
          producer_id: "writer:test",
          producer_seq: 1,
          source: nil,
          metadata: %{},
          refs: %{},
          idempotency_key: nil,
          inserted_at: NaiveDateTime.utc_now()
        })

      assert {:ok, events} = ReadPath.get_events_from_cursor(missing_id, 0, 100)
      assert Enum.map(events, & &1.seq) == [1]
    end

    test "returns ordered events across archive cold + ETS hot boundary" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      cold_rows = EventStore.from_cursor(id, 0, 3)
      insert_cold_rows(id, cold_rows)

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 3, trimmed: 3}], failed: []}} =
               WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
      assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5]
    end

    test "respects limit across archive cold + ETS hot boundary" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..5 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      cold_rows = EventStore.from_cursor(id, 0, 3)
      insert_cold_rows(id, cold_rows)

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 3, trimmed: 3}], failed: []}} =
               WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 2, 2)
      assert Enum.map(events, & &1.seq) == [3, 4]
    end
  end

  describe "ack_archived/1" do
    test "is idempotent for the same archive cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..4 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 2, trimmed: 2}], failed: []}} =
               WritePath.ack_archived(id, 2)

      assert EventStore.session_size(id) == 2

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 2, trimmed: 0}], failed: []}} =
               WritePath.ack_archived(id, 2)

      assert EventStore.session_size(id) == 2
    end

    test "clamps archive cursor to last_seq and compacts the full hot tail" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..3 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 3, trimmed: 3}], failed: []}} =
               WritePath.ack_archived(id, 10_000)

      assert EventStore.session_size(id) == 0
    end

    test "coalesces duplicate sessions to the highest archived cursor in one batch" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      for n <- 1..3 do
        {:ok, _} =
          append_event(id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:1"
          })
      end

      assert {:ok, %{applied: [%{session_id: ^id, archived_seq: 3, trimmed: 3}], failed: []}} =
               WritePath.ack_archived([{id, 1}, {id, 3}, {id, 2}])

      assert EventStore.session_size(id) == 0
    end

    test "is best effort for missing sessions within a batch" do
      first_id = unique_id("ses")
      second_id = unique_id("ses")
      missing_id = unique_id("ses-missing")

      {:ok, _} = WritePath.create_session(id: first_id)
      {:ok, _} = WritePath.create_session(id: second_id)

      {:ok, _} =
        append_event(first_id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      {:ok, _} =
        append_event(second_id, %{
          type: "content",
          payload: %{text: "two"},
          actor: "agent:1"
        })

      assert {:ok, %{applied: applied, failed: failed}} =
               WritePath.ack_archived([{first_id, 1}, {missing_id, 1}, {second_id, 1}])

      assert Enum.sort_by(applied, & &1.session_id) == [
               %{session_id: first_id, archived_seq: 1, trimmed: 1},
               %{session_id: second_id, archived_seq: 1, trimmed: 1}
             ]

      assert failed == [%{session_id: missing_id, reason: :session_not_found}]
      assert EventStore.session_size(first_id) == 0
      assert EventStore.session_size(second_id) == 0
    end
  end

  describe "Raft failover and recovery" do
    test "runtime reconcile emits local raft leader count telemetry" do
      handler_id = "raft-role-count-#{System.unique_integer([:positive, :monotonic])}"
      group_role_handler_id = "raft-group-role-#{System.unique_integer([:positive, :monotonic])}"
      test_pid = self()

      :ok =
        :telemetry.attach(
          handler_id,
          [:starcite, :raft, :role_count],
          fn _event, measurements, metadata, pid ->
            send(pid, {:raft_role_count_event, measurements, metadata})
          end,
          test_pid
        )

      :ok =
        :telemetry.attach(
          group_role_handler_id,
          [:starcite, :raft, :group_role],
          fn _event, measurements, metadata, pid ->
            send(pid, {:raft_group_role_event, measurements, metadata})
          end,
          test_pid
        )

      on_exit(fn ->
        :telemetry.detach(handler_id)
        :telemetry.detach(group_role_handler_id)
      end)

      eventually(
        fn ->
          assert RaftBootstrap.ready?()
        end,
        timeout: 10_000
      )

      send(RaftBootstrap, :runtime_reconcile)

      assert_receive_raft_role_count(:leader, WriteNodes.num_groups(), Node.self())
      assert_receive_raft_group_role(0, :leader, 1, Node.self())
      assert_receive_raft_group_role(0, :follower, 0, Node.self())
    end

    test "starting a multi-replica local group does not trigger a local election" do
      previous_num_groups = Application.get_env(:starcite, :num_groups)
      previous_replication_factor = Application.get_env(:starcite, :write_replication_factor)
      previous_write_node_ids = Application.get_env(:starcite, :write_node_ids)

      on_exit(fn ->
        restore_env(:num_groups, previous_num_groups)
        restore_env(:write_replication_factor, previous_replication_factor)
        restore_env(:write_node_ids, previous_write_node_ids)
        Starcite.Runtime.TestHelper.reset()
      end)

      Application.put_env(:starcite, :num_groups, 1)
      Application.put_env(:starcite, :write_replication_factor, 3)

      Application.put_env(:starcite, :write_node_ids, [
        Node.self(),
        :"peer-a@127.0.0.1",
        :"peer-b@127.0.0.1"
      ])

      Starcite.Runtime.TestHelper.reset()

      assert_no_trigger_election(fn ->
        assert :ok = RaftManager.start_group(0)
      end)
    end

    test "recovers state after server crash and restart" do
      id = unique_id("ses-failover")

      capture_log(fn ->
        {:ok, _} = WritePath.create_session(id: id)

        {:ok, first} =
          append_event(id, %{
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

        assert :ok = RaftManager.start_group(group_id)

        eventually(
          fn ->
            assert Process.whereis(server_id)
          end,
          timeout: 3_000
        )

        {:ok, second} =
          append_event(id, %{
            type: "content",
            payload: %{text: "after crash"},
            actor: "agent:1"
          })

        assert second.seq >= 2

        {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
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
          {:ok, _} = WritePath.create_session(id: id)
          id
        end

      tasks =
        Enum.map(ids, fn id ->
          Task.async(fn ->
            append_event(id, %{
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

  defp append_event(id, event, opts \\ [])
       when is_binary(id) and is_map(event) and is_list(opts) do
    producer_id = Map.get(event, :producer_id, "writer:test")

    enriched_event =
      event
      |> Map.put_new(:producer_id, producer_id)
      |> Map.put_new_lazy(:producer_seq, fn -> next_producer_seq(id, producer_id) end)

    WritePath.append_event(id, enriched_event, opts)
  end

  defp next_producer_seq(session_id, producer_id)
       when is_binary(session_id) and is_binary(producer_id) do
    counters = Process.get(:producer_seq_counters, %{})
    key = {session_id, producer_id}
    seq = Map.get(counters, key, 0) + 1
    Process.put(:producer_seq_counters, Map.put(counters, key, seq))
    seq
  end

  defp assert_no_trigger_election(fun) when is_function(fun, 0) do
    parent = self()

    {:ok, _tracer} =
      :dbg.tracer(
        :process,
        {fn msg, _state ->
           send(parent, {:dbg, msg})
           {:ok, nil}
         end, nil}
      )

    {:ok, _} = :dbg.p(self(), [:call])
    {:ok, matches} = :dbg.tpl(:ra, :trigger_election, :x)

    assert Enum.any?(matches, fn
             {:matched, _node, matched} when is_integer(matched) and matched >= 1 -> true
             _ -> false
           end)

    try do
      fun.()

      refute_receive(
        {:dbg, {:trace, _pid, :call, {:ra, :trigger_election, _args}}},
        200,
        "unexpected :ra.trigger_election/2 during local recovery"
      )
    after
      :dbg.stop()
    end
  end

  defp assert_receive_raft_role_count(role, groups, node_name)
       when role in [:leader, :follower, :candidate, :other, :down] and
              is_integer(groups) and groups >= 0 and is_atom(node_name) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_role_count(role, groups, Atom.to_string(node_name), deadline)
  end

  defp assert_receive_raft_group_role(group_id, role, present, node_name)
       when is_integer(group_id) and group_id >= 0 and role in [:leader, :follower, :candidate, :other, :down] and
              present in [0, 1] and is_atom(node_name) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_raft_group_role(group_id, role, present, Atom.to_string(node_name), deadline)
  end

  defp do_assert_receive_raft_role_count(role, groups, node_name, deadline)
       when is_binary(node_name) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_role_count_event, %{groups: ^groups}, %{node: ^node_name, role: ^role}} ->
        :ok

      {:raft_role_count_event, _measurements, _metadata} ->
        do_assert_receive_raft_role_count(role, groups, node_name, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft role count telemetry role=#{inspect(role)} groups=#{inspect(groups)} node=#{inspect(node_name)}"
        )
    end
  end

  defp do_assert_receive_raft_group_role(group_id, role, present, node_name, deadline)
       when is_binary(node_name) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:raft_group_role_event, %{present: ^present},
       %{node: ^node_name, group_id: ^group_id, role: ^role}} ->
        :ok

      {:raft_group_role_event, _measurements, _metadata} ->
        do_assert_receive_raft_group_role(group_id, role, present, node_name, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for raft group role telemetry group_id=#{inspect(group_id)} role=#{inspect(role)} present=#{inspect(present)} node=#{inspect(node_name)}"
        )
    end
  end

  defp restore_env(key, nil) when is_atom(key), do: Application.delete_env(:starcite, key)
  defp restore_env(key, value) when is_atom(key), do: Application.put_env(:starcite, key, value)

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

  defp insert_cold_rows(session_id, events) when is_binary(session_id) and is_list(events) do
    base_rows =
      Enum.map(events, fn event ->
        %{
          session_id: session_id,
          seq: event.seq,
          type: event.type,
          payload: event.payload,
          actor: event.actor,
          producer_id: event.producer_id,
          producer_seq: event.producer_seq,
          tenant_id: event.tenant_id,
          source: event.source,
          metadata: event.metadata,
          refs: event.refs,
          idempotency_key: event.idempotency_key,
          inserted_at: as_datetime(event.inserted_at)
        }
      end)

    case Store.adapter() do
      Starcite.Archive.Adapter.Postgres ->
        ensure_repo_sandbox()

        rows =
          if events_table_has_tenant_id?() do
            Enum.zip(base_rows, events)
            |> Enum.map(fn {row, event} -> Map.put(row, :tenant_id, event_tenant_id!(event)) end)
          else
            base_rows
          end

        {count, _} =
          Repo.insert_all(
            "events",
            rows,
            on_conflict: :nothing,
            conflict_target: [:session_id, :seq]
          )

        assert count == length(rows)

      _other ->
        rows =
          Enum.zip(base_rows, events)
          |> Enum.map(fn {row, event} -> Map.put(row, :tenant_id, event_tenant_id!(event)) end)

        assert {:ok, inserted} = Store.write_events(rows)
        assert inserted == length(rows)
    end
  end

  defp event_tenant_id!(%{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_tenant_id!(event) when is_map(event) do
    raise ArgumentError,
          "event row missing tenant_id: #{inspect(Map.take(event, [:seq, :producer_id, :producer_seq]))}"
  end

  defp events_table_has_tenant_id? do
    result =
      Ecto.Adapters.SQL.query!(
        Repo,
        "SELECT 1 FROM information_schema.columns WHERE table_name = 'events' AND column_name = 'tenant_id' LIMIT 1",
        []
      )

    result.num_rows > 0
  end

  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp as_datetime(%DateTime{} = value), do: value

  defp ensure_repo_sandbox do
    if Process.whereis(Repo) == nil do
      _pid = start_supervised!(Repo)
      :ok
    end

    checked_out? =
      case Ecto.Adapters.SQL.Sandbox.checkout(Repo) do
        :ok -> true
        {:already, _owner} -> false
      end

    if checked_out? do
      on_exit(fn ->
        Ecto.Adapters.SQL.Sandbox.checkin(Repo)
      end)
    end

    :ok
  end
end
