defmodule Starcite.RuntimeTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Phoenix.PubSub
  alias Starcite.Archive.Store
  alias Starcite.Auth.Principal
  alias Starcite.{ReadPath, WritePath}
  alias Starcite.DataPlane.{EventStore, RaftAccess, SessionDiscovery, SessionStore}
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

    test "create_session publishes service discovery update" do
      id = unique_id("ses-discovery")
      :ok = PubSub.subscribe(Starcite.PubSub, SessionDiscovery.topic())

      {:ok, _session} = WritePath.create_session(id: id, tenant_id: "acme")

      assert_receive {:session_discovery, update}, 1_000
      assert update.version == 1
      assert update.kind == :session_created
      assert update.state == :active
      assert update.session_id == id
      assert update.tenant_id == "acme"
      assert is_binary(update.occurred_at)
      assert String.ends_with?(update.occurred_at, "Z")
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

    @tag :requires_postgres
    test "hydrates frozen sessions from archive and retries append once" do
      :ok = setup_repo_shared_sandbox()
      :ok = PubSub.subscribe(Starcite.PubSub, SessionDiscovery.topic())

      id = unique_id("ses-hydrate")
      {:ok, _} = WritePath.create_session(id: id)

      assert_receive {:session_discovery, %{kind: :session_created, session_id: ^id}}, 1_000

      {:ok, _reply} =
        append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:1"
        })

      assert {:ok, %{archived_seq: 1, trimmed: 1}} = WritePath.ack_archived(id, 1)

      {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(id)
      {:ok, session} = RaftAccess.query_session(server_id, id)

      assert :ok = Store.upsert_session(archive_session_snapshot(session))

      assert {:ok, {:reply, {:ok, %{id: ^id}}}, _leader} =
               :ra.process_command(
                 {server_id, Node.self()},
                 {:freeze_session, id, session.last_seq, session.archived_seq,
                  session.last_progress_poll},
                 2_000
               )

      assert {:error, :session_not_found} = RaftAccess.query_session(server_id, id)

      assert_receive {:session_discovery, frozen_update}, 1_000
      assert frozen_update.kind == :session_frozen
      assert frozen_update.state == :frozen
      assert frozen_update.session_id == id

      assert {:ok, append_reply} =
               append_event(id, %{
                 type: "content",
                 payload: %{text: "two"},
                 actor: "agent:1"
               })

      assert append_reply.seq == 2
      assert append_reply.last_seq == 2

      assert {:ok, rehydrated_session} = RaftAccess.query_session(server_id, id)
      assert rehydrated_session.last_seq == 2
      assert rehydrated_session.archived_seq == 1

      assert_receive {:session_discovery, hydrated_update}, 1_000
      assert hydrated_update.kind == :session_hydrated
      assert hydrated_update.state == :active
      assert hydrated_update.session_id == id
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

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

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

    @tag :requires_postgres
    test "returns ordered events across Postgres cold + ETS hot boundary" do
      :ok = setup_repo_shared_sandbox()

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

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 0, 100)
      assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5]
    end

    @tag :requires_postgres
    test "respects limit across Postgres cold + ETS hot boundary" do
      :ok = setup_repo_shared_sandbox()

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

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 3)

      {:ok, events} = ReadPath.get_events_from_cursor(id, 2, 2)
      assert Enum.map(events, & &1.seq) == [3, 4]
    end
  end

  describe "ack_archived/2" do
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

      assert {:ok, %{archived_seq: 2, trimmed: 2}} = WritePath.ack_archived(id, 2)
      assert EventStore.session_size(id) == 2

      assert {:ok, %{archived_seq: 2, trimmed: 0}} = WritePath.ack_archived(id, 2)
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

      assert {:ok, %{archived_seq: 3, trimmed: 3}} = WritePath.ack_archived(id, 10_000)
      assert EventStore.session_size(id) == 0
    end
  end

  describe "Raft failover and recovery" do
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
    include_tenant_id? = events_table_has_tenant_id?()

    rows =
      Enum.map(events, fn event ->
        %{
          session_id: session_id,
          seq: event.seq,
          type: event.type,
          payload: event.payload,
          actor: event.actor,
          producer_id: event.producer_id,
          producer_seq: event.producer_seq,
          source: event.source,
          metadata: event.metadata,
          refs: event.refs,
          idempotency_key: event.idempotency_key,
          inserted_at: as_datetime(event.inserted_at)
        }
        |> maybe_put_tenant_id(event, include_tenant_id?)
      end)

    {count, _} =
      Repo.insert_all(
        "events",
        rows,
        on_conflict: :nothing,
        conflict_target: [:session_id, :seq]
      )

    assert count == length(rows)
  end

  defp event_tenant_id!(%{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp event_tenant_id!(event) when is_map(event) do
    raise ArgumentError,
          "event row missing tenant_id: #{inspect(Map.take(event, [:seq, :producer_id, :producer_seq]))}"
  end

  defp maybe_put_tenant_id(row, event, true) when is_map(row) and is_map(event) do
    Map.put(row, :tenant_id, event_tenant_id!(event))
  end

  defp maybe_put_tenant_id(row, _event, false) when is_map(row), do: row

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

  defp archive_session_snapshot(%Session{} = session) do
    %{
      id: session.id,
      title: session.title,
      tenant_id: session.tenant_id,
      creator_principal: session.creator_principal,
      metadata: session.metadata,
      created_at: DateTime.from_naive!(session.inserted_at, "Etc/UTC"),
      last_seq: session.last_seq,
      archived_seq: session.archived_seq,
      retention: session.retention,
      producer_cursors: session.producer_cursors,
      last_progress_poll: session.last_progress_poll,
      snapshot_version: "v1"
    }
  end

  defp ensure_repo_started do
    if Process.whereis(Repo) do
      :ok
    else
      _pid = start_supervised!(Repo)
      :ok
    end
  end

  defp setup_repo_shared_sandbox do
    ensure_repo_started()
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    on_exit(fn ->
      Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)
    end)

    :ok
  end
end
