defmodule Starcite.Storage.EventArchiveTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Starcite.Repo
  alias Starcite.Session.Header
  alias Starcite.Storage.EventArchive
  alias Starcite.Storage.SessionCatalog

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    :ok = EventArchive.clear_cache()
    :ok
  end

  test "write_events is idempotent and reads ordered ranges" do
    session_id = "ses-archive-store-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()
    rows = event_rows(session_id, inserted_at, 1..260)

    persist_session(session_id)

    assert {:ok, 260} = EventArchive.write_events(rows)
    assert {:ok, 0} = EventArchive.write_events(rows)

    assert {:ok, events} = EventArchive.read_events(session_id, 255, 258)
    assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
    assert Enum.all?(events, &(&1.tenant_id == "acme"))
  end

  test "write_events splits archive inserts into configured Postgres batches" do
    session_id = "ses-archive-write-batch-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    persist_session(session_id)

    with_app_env(:archive_db_write_batch_size, 2, fn ->
      {handler_id, collector} = start_query_collector()

      on_exit(fn ->
        :telemetry.detach(handler_id)
      end)

      assert {:ok, 5} = EventArchive.write_events(event_rows(session_id, inserted_at, 1..5))
      assert insert_query_count(collector) == 3
    end)
  end

  test "read_events splits persisted range reads into configured Postgres batches" do
    session_id = "ses-archive-read-batch-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    persist_session(session_id)

    assert {:ok, 5} = EventArchive.write_events(event_rows(session_id, inserted_at, 1..5))

    with_app_env(:archive_db_read_batch_size, 2, fn ->
      {handler_id, collector} = start_query_collector()

      on_exit(fn ->
        :telemetry.detach(handler_id)
      end)

      assert {:ok, events} = EventArchive.read_events(session_id, 1, 5)
      assert Enum.map(events, & &1.seq) == [1, 2, 3, 4, 5]
      assert select_query_count(collector) == 3
    end)
  end

  test "read_events serves cached rows after persistence rows are removed" do
    session_id = "ses-archive-cache-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()

    persist_session(session_id)

    assert {:ok, 3} = EventArchive.write_events(event_rows(session_id, inserted_at, 1..3))
    assert {:ok, first} = EventArchive.read_events(session_id, 1, 3)
    assert Enum.map(first, & &1.seq) == [1, 2, 3]

    assert {3, _} =
             Repo.delete_all(from(event in "events", where: event.session_id == ^session_id))

    assert {:ok, second} = EventArchive.read_events(session_id, 1, 3)
    assert second == first
  end

  test "write_events rejects mixed-tenant rows for one session" do
    session_id = "ses-archive-mixed-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now()
    [first, second] = event_rows(session_id, inserted_at, 1..2)

    persist_session(session_id)

    assert {:error, :archive_write_unavailable} =
             EventArchive.write_events([first, %{second | tenant_id: "beta"}])
  end

  defp persist_session(session_id, tenant_id \\ "acme")
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: tenant_id))
  end

  defp event_rows(session_id, inserted_at, seqs) do
    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{n: seq},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: seq,
        tenant_id: "acme",
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
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

  defp with_app_env(key, value, fun) when is_atom(key) and is_function(fun, 0) do
    sentinel = {:unset, __MODULE__}
    previous = Application.get_env(:starcite, key, sentinel)
    Application.put_env(:starcite, key, value)

    try do
      fun.()
    after
      case previous do
        ^sentinel -> Application.delete_env(:starcite, key)
        _ -> Application.put_env(:starcite, key, previous)
      end
    end
  end

  defp start_query_collector do
    {:ok, collector} = Agent.start_link(fn -> [] end)
    handler_id = "repo-query-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :repo, :query],
        fn _event, _measurements, metadata, pid ->
          query = Map.get(metadata, :query)

          if is_binary(query) do
            Agent.update(pid, fn queries -> [query | queries] end)
          end
        end,
        collector
      )

    {handler_id, collector}
  end

  defp insert_query_count(collector) when is_pid(collector) do
    Agent.get(collector, fn queries ->
      Enum.count(queries, &String.starts_with?(&1, "INSERT INTO \"events\""))
    end)
  end

  defp select_query_count(collector) when is_pid(collector) do
    Agent.get(collector, fn queries ->
      Enum.count(
        queries,
        &(String.starts_with?(&1, "SELECT") and String.contains?(&1, "\"events\""))
      )
    end)
  end
end
