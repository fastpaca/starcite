defmodule Starcite.ArchiveTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Starcite.{ReadPath, Repo, WritePath}
  alias Starcite.DataPlane.EventStore

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    Process.put(:producer_seq_counters, %{})
    {handler_id, collector} = start_archive_batch_collector()

    on_exit(fn ->
      :telemetry.detach(handler_id)

      if Process.alive?(collector) do
        Agent.stop(collector)
      end
    end)

    {:ok, archive_batch_collector: collector}
  end

  test "flush archives and advances cursor via ack" do
    {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 100})

    session_id = "ses-arch-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = WritePath.create_session(id: session_id)

    for i <- 1..5 do
      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "m#{i}"},
          actor: "agent:test"
        })
    end

    eventually(
      fn ->
        {:ok, session} = ReadPath.get_session(session_id)
        assert session.archived_seq == session.last_seq
        assert EventStore.session_size(session_id) == 0
        assert archived_seqs(session_id) == [1, 2, 3, 4, 5]
      end,
      timeout: 2_000
    )
  end

  test "archive process fails loud on invalid archive batch state" do
    {:ok, pid} = start_supervised({Starcite.Archive, flush_interval_ms: 10_000})

    ref = Process.monitor(pid)

    session_id = "ses-arch-fail-#{System.unique_integer([:positive, :monotonic])}"
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    :ok =
      EventStore.put_event(session_id, "acme", %{
        seq: 1,
        type: "content",
        payload: %{text: "m1"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 1,
        tenant_id: "acme",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: NaiveDateTime.utc_now()
      })

    :ok =
      EventStore.put_event(session_id, "acme", %{
        seq: 2,
        type: "content",
        payload: %{text: "m2"},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: 2,
        tenant_id: "beta",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: NaiveDateTime.utc_now()
      })

    send(pid, :flush_tick)

    assert_receive {:DOWN, ^ref, :process, ^pid, {%RuntimeError{}, _}}, 2_000
  end

  describe "archive idempotency" do
    test "duplicate flushes stay idempotent" do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 50})

      session_id = "ses-idem-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "msg#{i}"},
            actor: "agent:test"
          })
      end

      eventually(
        fn ->
          assert archived_seqs(session_id) == [1, 2, 3]
        end,
        timeout: 2_000
      )

      Process.sleep(100)

      eventually(
        fn ->
          assert archived_seqs(session_id) == [1, 2, 3]
          assert archived_event_count(session_id) == 3
        end,
        timeout: 1_000
      )
    end

    test "retried writes succeed without duplicates" do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 50})

      session_id = "ses-retry-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "retry-msg"},
          actor: "agent:test"
        })

      eventually(
        fn ->
          assert archived_seqs(session_id) == [1]
          assert archived_event_count(session_id) == 1
        end,
        timeout: 2_000
      )
    end

    test "archive respects sequence ordering" do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 50})

      session_id = "ses-order-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..5 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "msg#{i}"},
            actor: "agent:test"
          })
      end

      eventually(
        fn ->
          assert archived_seqs(session_id) == [1, 2, 3, 4, 5]
        end,
        timeout: 2_000
      )
    end
  end

  describe "pull-mode behavior" do
    test "interleaves flush order across tenants within a tick", %{
      archive_batch_collector: collector
    } do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 10_000})

      acme_a = "ses-acme-a-#{System.unique_integer([:positive, :monotonic])}"
      acme_b = "ses-acme-b-#{System.unique_integer([:positive, :monotonic])}"
      beta_a = "ses-beta-a-#{System.unique_integer([:positive, :monotonic])}"
      beta_b = "ses-beta-b-#{System.unique_integer([:positive, :monotonic])}"

      tenant_by_session = %{
        acme_a => "acme",
        acme_b => "acme",
        beta_a => "beta",
        beta_b => "beta"
      }

      for {session_id, tenant_id} <- tenant_by_session do
        {:ok, _} = WritePath.create_session(id: session_id, tenant_id: tenant_id)

        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "#{tenant_id}:#{session_id}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          first_batches =
            archive_batches(collector)
            |> Enum.take(4)

          assert length(first_batches) == 4
          assert Enum.all?(first_batches, &(&1.batch_rows == 1))

          tenant_sequence =
            first_batches
            |> Enum.map(fn batch -> Map.fetch!(tenant_by_session, batch.session_id) end)

          assert tenant_sequence == ["acme", "beta", "acme", "beta"]
        end,
        timeout: 2_000
      )
    end

    test "archives pending work across multiple sessions in one scan loop", %{
      archive_batch_collector: collector
    } do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 10_000})

      session_a = "ses-pull-a-#{System.unique_integer([:positive, :monotonic])}"
      session_b = "ses-pull-b-#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _} = WritePath.create_session(id: session_a)
      {:ok, _} = WritePath.create_session(id: session_b)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_a, %{
            type: "content",
            payload: %{text: "a#{i}"},
            actor: "agent:test"
          })

        {:ok, _} =
          append_event(session_b, %{
            type: "content",
            payload: %{text: "b#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session_a_state} = ReadPath.get_session(session_a)
          {:ok, session_b_state} = ReadPath.get_session(session_b)

          assert session_a_state.archived_seq == session_a_state.last_seq
          assert session_b_state.archived_seq == session_b_state.last_seq

          written_sessions =
            collector
            |> archive_batches()
            |> Enum.map(& &1.session_id)
            |> Enum.uniq()
            |> Enum.sort()

          assert written_sessions == Enum.sort([session_a, session_b])
        end,
        timeout: 2_000
      )
    end

    test "respects archive batch size per flush tick and converges over repeated ticks", %{
      archive_batch_collector: collector
    } do
      old_batch_size = Application.get_env(:starcite, :archive_batch_size)
      Application.put_env(:starcite, :archive_batch_size, 2)

      on_exit(fn ->
        if old_batch_size do
          Application.put_env(:starcite, :archive_batch_size, old_batch_size)
        else
          Application.delete_env(:starcite, :archive_batch_size)
        end
      end)

      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 10_000})

      session_id = "ses-batch-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..5 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          [first_batch | _rest] = archive_batches(collector)
          assert first_batch.batch_rows == 2
        end,
        timeout: 1_500
      )

      send(Starcite.Archive, :flush_tick)
      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = ReadPath.get_session(session_id)
          assert session.archived_seq == session.last_seq

          batch_sizes =
            collector
            |> archive_batches()
            |> Enum.map(& &1.batch_rows)

          assert batch_sizes == [2, 2, 1]
        end,
        timeout: 2_000
      )
    end

    test "continues archiving new writes after a full ETS compaction" do
      {:ok, _pid} = start_supervised({Starcite.Archive, flush_interval_ms: 10_000})

      session_id = "ses-resume-#{System.unique_integer([:positive, :monotonic])}"
      {:ok, _} = WritePath.create_session(id: session_id)

      for i <- 1..3 do
        {:ok, _} =
          append_event(session_id, %{
            type: "content",
            payload: %{text: "m#{i}"},
            actor: "agent:test"
          })
      end

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = ReadPath.get_session(session_id)
          assert session.archived_seq == 3
          assert EventStore.session_size(session_id) == 0
        end,
        timeout: 2_000
      )

      {:ok, _} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "m4"},
          actor: "agent:test"
        })

      send(Starcite.Archive, :flush_tick)

      eventually(
        fn ->
          {:ok, session} = ReadPath.get_session(session_id)
          assert session.archived_seq == 4
          assert EventStore.session_size(session_id) == 0
          assert archived_seqs(session_id) == [1, 2, 3, 4]
        end,
        timeout: 2_000
      )
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

  defp archived_seqs(session_id) when is_binary(session_id) and session_id != "" do
    Repo.all(
      from(event in "events",
        where: event.session_id == ^session_id,
        order_by: [asc: event.seq],
        select: event.seq
      )
    )
  end

  defp archived_event_count(session_id) when is_binary(session_id) and session_id != "" do
    Repo.aggregate(
      from(event in "events", where: event.session_id == ^session_id),
      :count,
      :seq
    )
  end

  defp start_archive_batch_collector do
    {:ok, collector} = Agent.start_link(fn -> [] end)
    handler_id = "archive-batch-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :archive, :batch],
        fn _event, measurements, metadata, pid ->
          Agent.update(pid, fn batches ->
            [
              %{session_id: metadata.session_id, tenant_id: metadata.tenant_id}
              |> Map.merge(measurements)
              | batches
            ]
          end)
        end,
        collector
      )

    {handler_id, collector}
  end

  defp archive_batches(collector) when is_pid(collector) do
    collector
    |> Agent.get(& &1)
    |> Enum.reverse()
  end

  defp ensure_repo_sandbox do
    if Process.whereis(Repo) == nil do
      _pid = start_supervised!(Repo)
      :ok
    end

    case Ecto.Adapters.SQL.Sandbox.checkout(Repo) do
      :ok -> :ok
      {:already, _owner} -> :ok
    end

    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    :ok
  end

  defp eventually(fun, opts) when is_function(fun, 0) do
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
          raise "condition not met before timeout"
        end
    catch
      :exit, _reason ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(interval)
          do_eventually(fun, deadline, interval)
        else
          raise "condition not met before timeout"
        end
    end
  end
end
