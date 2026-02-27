defmodule StarciteWeb.TailSocketTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.IdempotentTestAdapter
  alias Starcite.WritePath
  alias Starcite.DataPlane.{CursorUpdate, EventStore, RaftAccess}
  alias Starcite.Repo
  alias StarciteWeb.TailSocket

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})
    :ok
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp base_state(session_id, cursor) do
    %{
      session_id: session_id,
      topic: CursorUpdate.topic(session_id),
      cursor: cursor,
      replay_queue: :queue.new(),
      replay_done: false,
      live_buffer: %{},
      drain_scheduled: false,
      auth_expires_at: nil,
      auth_expiry_timer_ref: nil
    }
  end

  defp drain_until_idle(state, frames \\ [], remaining \\ 20)

  defp drain_until_idle(state, frames, 0), do: {Enum.reverse(frames), state}

  defp drain_until_idle(state, frames, remaining) do
    case TailSocket.handle_info(:drain_replay, state) do
      {:push, {:text, payload}, next_state} ->
        seq = payload |> Jason.decode!() |> Map.fetch!("seq")
        drain_until_idle(next_state, [seq | frames], remaining - 1)

      {:ok, next_state} ->
        idle? =
          next_state.replay_done and :queue.is_empty(next_state.replay_queue) and
            map_size(next_state.live_buffer) == 0 and not next_state.drain_scheduled

        if idle? do
          {Enum.reverse(frames), next_state}
        else
          drain_until_idle(next_state, frames, remaining - 1)
        end
    end
  end

  describe "replay/live boundary" do
    test "replays first, then flushes buffered live events in order without duplicates" do
      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:test"
        })

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "two"},
          actor: "agent:test"
        })

      {:ok, state_after_fetch} = TailSocket.handle_info(:drain_replay, base_state(session_id, 0))

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "three"},
          actor: "agent:test"
        })

      {:ok, update_three} = cursor_update_for(session_id, 3)

      {:ok, state_with_buffered_live} =
        TailSocket.handle_info({:cursor_update, update_three}, state_after_fetch)

      {frames, drained_state} = drain_until_idle(state_with_buffered_live)

      assert frames == [1, 2, 3]
      assert drained_state.cursor == 3

      assert {:ok, ^drained_state} =
               TailSocket.handle_info({:cursor_update, update_three}, drained_state)
    end

    test "pushes live events immediately after replay is complete" do
      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)

      {[], drained_state} = drain_until_idle(base_state(session_id, 0))

      {:ok, _} =
        append_event(session_id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:test"
        })

      {:ok, update} = cursor_update_for(session_id, 1)

      assert {:push, {:text, payload}, next_state} =
               TailSocket.handle_info({:cursor_update, update}, drained_state)

      frame = Jason.decode!(payload)
      assert frame["seq"] == 1
      assert String.ends_with?(frame["inserted_at"], "Z")
      assert next_state.cursor == 1
    end

    test "uses cursor update event payload without performing lookup reads" do
      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)

      {:ok, _} =
        append_event(session_id, %{
          type: "state",
          payload: %{state: "running"},
          actor: "agent:test"
        })

      {:ok, update} = cursor_update_for(session_id, 1)
      assert 1 == EventStore.delete_below(session_id, 2)
      assert :error = EventStore.get_event(session_id, 1)

      handler_id = attach_tail_lookup_handler()
      on_exit(fn -> detach_tail_lookup_handler(handler_id) end)

      state = %{base_state(session_id, 0) | replay_done: true}

      assert {:push, {:text, payload}, next_state} =
               TailSocket.handle_info({:cursor_update, update}, state)

      frame = Jason.decode!(payload)
      assert frame["seq"] == 1
      assert frame["payload"] == %{"state" => "running"}
      assert next_state.cursor == 1
      refute_receive {:tail_lookup, _metadata}, 100
    end

    test "falls back to storage when a live cursor update misses ETS" do
      ensure_repo_started()
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
      Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:test"
        })

      cold_rows = EventStore.from_cursor(session_id, 0, 1)
      insert_cold_rows(session_id, cold_rows)
      assert {:ok, %{archived_seq: 1, trimmed: 1}} = WritePath.ack_archived(session_id, 1)
      assert :error = EventStore.get_event(session_id, 1)

      handler_id = attach_tail_lookup_handler()
      on_exit(fn -> detach_tail_lookup_handler(handler_id) end)

      update = %{
        version: 1,
        session_id: session_id,
        seq: 1,
        last_seq: 1,
        type: "content",
        actor: "agent:test",
        source: nil,
        inserted_at: NaiveDateTime.utc_now()
      }

      state = %{base_state(session_id, 0) | replay_done: true}

      assert {:push, {:text, payload}, next_state} =
               TailSocket.handle_info({:cursor_update, update}, state)

      frame = Jason.decode!(payload)
      assert frame["seq"] == 1
      assert next_state.cursor == 1

      assert_receive {:tail_lookup,
                      %{session_id: ^session_id, seq: 1, source: :ets, result: :miss}}

      assert_receive {:tail_lookup,
                      %{session_id: ^session_id, seq: 1, source: :storage, result: :hit}}
    end

    test "handles delayed cursor updates after archive compaction race" do
      with_archive_adapter(IdempotentTestAdapter)

      {:ok, _pid} =
        start_supervised(
          {Starcite.Archive,
           flush_interval_ms: 10_000, adapter: IdempotentTestAdapter, adapter_opts: []}
        )

      :ok = IdempotentTestAdapter.clear_writes()

      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)
      :ok = Phoenix.PubSub.subscribe(Starcite.PubSub, CursorUpdate.topic(session_id))

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "race"},
          actor: "agent:test"
        })

      assert_receive {:cursor_update, %{session_id: ^session_id, seq: 1} = update}

      send(Starcite.Archive, :flush_tick)

      eventually(fn ->
        {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
        {:ok, session} = RaftAccess.query_session(server_id, session_id)
        assert session.archived_seq == 1
        assert {:ok, _event} = EventStore.get_event(session_id, 1)
      end)

      state = %{base_state(session_id, 0) | replay_done: true}

      assert {:push, {:text, payload}, next_state} =
               TailSocket.handle_info({:cursor_update, update}, state)

      frame = Jason.decode!(payload)
      assert frame["seq"] == 1
      assert next_state.cursor == 1
    end

    test "replays across Postgres cold + ETS hot boundary" do
      ensure_repo_started()
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
      Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

      session_id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: session_id)

      for n <- 1..4 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{n}"},
            actor: "agent:test"
          })
      end

      cold_rows = EventStore.from_cursor(session_id, 0, 2)
      insert_cold_rows(session_id, cold_rows)
      assert {:ok, %{archived_seq: 2, trimmed: 2}} = WritePath.ack_archived(session_id, 2)

      {frames, final_state} = drain_until_idle(base_state(session_id, 0))
      assert frames == [1, 2, 3, 4]
      assert final_state.cursor == 4
    end
  end

  defp attach_tail_lookup_handler do
    handler_id = "tail-lookup-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :tail, :cursor_lookup],
        fn _event, _measurements, metadata, pid ->
          send(pid, {:tail_lookup, metadata})
        end,
        test_pid
      )

    handler_id
  end

  defp detach_tail_lookup_handler(handler_id) when is_binary(handler_id) do
    :telemetry.detach(handler_id)
  end

  defp with_archive_adapter(adapter_mod) when is_atom(adapter_mod) do
    previous = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, adapter_mod)

    on_exit(fn ->
      if is_nil(previous) do
        Application.delete_env(:starcite, :archive_adapter)
      else
        Application.put_env(:starcite, :archive_adapter, previous)
      end
    end)
  end

  defp eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout = Keyword.get(opts, :timeout, 2_000)
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

  describe "auth lifetime" do
    test "stops socket when auth lifetime expires" do
      state =
        base_state("ses-auth", 0)
        |> Map.put(:replay_done, true)

      assert {:stop, :token_expired, {4001, "token_expired"}, ^state} =
               TailSocket.handle_info(:auth_expired, state)
    end
  end

  defp cursor_update_for(session_id, seq) do
    with {:ok, event} <- EventStore.get_event(session_id, seq) do
      {:cursor_update, update} = CursorUpdate.message(session_id, event, seq)
      {:ok, update}
    end
  end

  defp insert_cold_rows(session_id, events) when is_binary(session_id) and is_list(events) do
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

  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp as_datetime(%DateTime{} = value), do: value

  defp ensure_repo_started do
    if Process.whereis(Repo) do
      :ok
    else
      _pid = start_supervised!(Repo)
      :ok
    end
  end
end
