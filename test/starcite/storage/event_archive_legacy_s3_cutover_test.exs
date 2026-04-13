defmodule Starcite.Storage.EventArchiveLegacyS3CutoverTest do
  use ExUnit.Case, async: false

  import Ecto.Query

  alias Starcite.Repo
  alias Starcite.Session.Header
  alias Starcite.Storage.EventArchive
  alias Starcite.Storage.EventArchive.S3
  alias Starcite.Storage.SessionCatalog

  defmodule FakeClient do
    @store __MODULE__.Store

    def store_name, do: @store

    def get_object(%{bucket: bucket}, key) do
      Agent.update(@store, &increment_call(&1, :get, key))

      case Agent.get(@store, fn state -> state |> bucket_objects(bucket) |> Map.get(key) end) do
        nil -> {:ok, :not_found}
        %{body: body, etag: etag} -> {:ok, {body, etag}}
      end
    end

    def put_object(%{bucket: bucket}, key, body, opts \\ []) do
      Agent.get_and_update(@store, fn state ->
        state = increment_call(state, :put, key)
        existing = state |> bucket_objects(bucket) |> Map.get(key)

        if precondition_failed?(existing, opts) do
          {{:error, :precondition_failed}, state}
        else
          bucket_state = bucket_objects(state, bucket)
          etag = etag_for(body)

          next_state =
            put_in(
              state,
              [:objects, bucket],
              Map.put(bucket_state, key, %{body: body, etag: etag})
            )

          {:ok, next_state}
        end
      end)
    end

    def list_keys(%{bucket: bucket}, prefix) do
      keys =
        Agent.get(@store, fn state ->
          state
          |> bucket_objects(bucket)
          |> Map.keys()
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Enum.sort()
        end)

      {:ok, keys}
    end

    def clear_objects do
      Agent.update(@store, fn state -> %{state | objects: %{}, calls: %{}} end)
    end

    def call_count(op, key) when op in [:get, :put] and is_binary(key) do
      Agent.get(@store, fn state ->
        state
        |> Map.get(:calls, %{})
        |> Map.get({op, key}, 0)
      end)
    end

    defp precondition_failed?(existing, opts) do
      if_none_match = Keyword.get(opts, :if_none_match)
      if_match = Keyword.get(opts, :if_match)

      cond do
        if_none_match == "*" and existing != nil -> true
        is_binary(if_match) and existing == nil -> true
        is_binary(if_match) and existing.etag != if_match -> true
        true -> false
      end
    end

    defp etag_for(body) when is_binary(body) do
      digest =
        :crypto.hash(:md5, body)
        |> Base.encode16(case: :lower)

      ~s("#{digest}")
    end

    defp bucket_objects(state, bucket) when is_binary(bucket) do
      state
      |> Map.get(:objects, %{})
      |> Map.get(bucket, %{})
    end

    defp increment_call(state, op, key) do
      update_in(state, [:calls], fn calls ->
        Map.update(calls || %{}, {op, key}, 1, &(&1 + 1))
      end)
    end
  end

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    :ok = EventArchive.clear_cache()

    start_supervised!(%{
      id: FakeClient.store_name(),
      start:
        {Agent, :start_link,
         [fn -> %{objects: %{}, calls: %{}} end, [name: FakeClient.store_name()]]}
    })

    :ok
  end

  test "write_events double-writes to Postgres and legacy S3 during cutover" do
    with_legacy_s3_cutover([], fn opts ->
      session_id = "ses-legacy-s3-write-#{System.unique_integer([:positive, :monotonic])}"
      inserted_at = NaiveDateTime.utc_now()
      rows = event_rows(session_id, inserted_at, 1..3)

      assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: "acme"))
      assert {:ok, 3} = EventArchive.write_events(rows)
      assert archived_event_count(session_id) == 3

      assert {:ok, events} = S3.read_events(session_id, "acme", 1, 3, opts)
      assert Enum.map(events, & &1.seq) == [1, 2, 3]

      tenant_segment = Base.url_encode64("acme", padding: false)
      session_segment = Base.url_encode64(session_id, padding: false)

      assert {:ok, keys} = FakeClient.list_keys(%{bucket: opts[:bucket]}, "#{opts[:prefix]}/")

      assert "#{opts[:prefix]}/events/v1/#{tenant_segment}/#{session_segment}/1.ndjson" in keys
    end)
  end

  test "read_events falls back to legacy S3 and migrates the full archived session into Postgres" do
    with_legacy_s3_cutover([], fn opts ->
      session_id = "ses-legacy-s3-read-#{System.unique_integer([:positive, :monotonic])}"
      inserted_at = NaiveDateTime.utc_now()
      rows = event_rows(session_id, inserted_at, 1..258)

      assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: "acme"))

      assert :ok =
               SessionCatalog.put_progress_batch([
                 %{session_id: session_id, archived_seq: 258}
               ])

      assert {:ok, 258} = S3.write_events(rows, opts)
      assert archived_event_count(session_id) == 0

      assert {:ok, events} = EventArchive.read_events(session_id, 255, 258)
      assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
      assert archived_event_count(session_id) == 258

      :ok = EventArchive.clear_cache()
      FakeClient.clear_objects()

      assert {:ok, replayed} = EventArchive.read_events(session_id, 1, 4)
      assert Enum.map(replayed, & &1.seq) == [1, 2, 3, 4]
    end)
  end

  test "read_events stays on Postgres when the archived range is already complete" do
    with_legacy_s3_cutover([], fn opts ->
      session_id = "ses-legacy-s3-pg-first-#{System.unique_integer([:positive, :monotonic])}"
      inserted_at = NaiveDateTime.utc_now()
      rows = event_rows(session_id, inserted_at, 1..4)

      assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: "acme"))

      assert :ok =
               SessionCatalog.put_progress_batch([
                 %{session_id: session_id, archived_seq: 4}
               ])

      assert {:ok, 4} = EventArchive.write_events_postgres(rows)
      :ok = EventArchive.clear_cache()

      tenant_segment = Base.url_encode64("acme", padding: false)
      session_segment = Base.url_encode64(session_id, padding: false)
      key = "#{opts[:prefix]}/events/v1/#{tenant_segment}/#{session_segment}/1.ndjson"

      assert FakeClient.call_count(:get, key) == 0
      assert {:ok, events} = EventArchive.read_events(session_id, 1, 4)
      assert Enum.map(events, & &1.seq) == [1, 2, 3, 4]
      assert FakeClient.call_count(:get, key) == 0
    end)
  end

  test "boot backfill imports archived sessions from legacy S3" do
    with_legacy_s3_cutover([boot_backfill: true], fn opts ->
      inserted_at = NaiveDateTime.utc_now()

      sessions = [
        {"ses-boot-a-#{System.unique_integer([:positive, :monotonic])}", "acme", 3},
        {"ses-boot-b-#{System.unique_integer([:positive, :monotonic])}", "beta", 2}
      ]

      for {session_id, tenant_id, archived_seq} <- sessions do
        assert :ok = SessionCatalog.persist_created(Header.new(session_id, tenant_id: tenant_id))

        assert :ok =
                 SessionCatalog.put_progress_batch([
                   %{session_id: session_id, archived_seq: archived_seq}
                 ])

        assert {:ok, ^archived_seq} =
                 S3.write_events(
                   event_rows(session_id, inserted_at, 1..archived_seq, tenant_id),
                   opts
                 )
      end

      :ok = EventArchive.run_legacy_s3_backfill()

      assert Enum.map(sessions, fn {session_id, _tenant_id, archived_seq} ->
               {session_id, archived_event_count(session_id), archived_seq}
             end) ==
               Enum.map(sessions, fn {session_id, _tenant_id, archived_seq} ->
                 {session_id, archived_seq, archived_seq}
               end)
    end)
  end

  defp with_legacy_s3_cutover(extra_opts, fun)
       when is_list(extra_opts) and is_function(fun, 1) do
    prefix = "legacy-s3-test-#{System.unique_integer([:positive, :monotonic])}"

    opts =
      [
        enabled: true,
        bucket: "archive-test",
        prefix: prefix,
        client_mod: FakeClient
      ]
      |> Keyword.merge(extra_opts)

    with_app_env(:archive_legacy_s3_opts, opts, fn ->
      fun.(Keyword.drop(opts, [:enabled, :boot_backfill, :migrate_batch_size]))
    end)
  end

  defp event_rows(session_id, inserted_at, seqs, tenant_id \\ "acme")
       when is_binary(session_id) and session_id != "" and is_struct(inserted_at, NaiveDateTime) and
              is_binary(tenant_id) and tenant_id != "" do
    Enum.map(seqs, fn seq ->
      %{
        session_id: session_id,
        seq: seq,
        type: "content",
        payload: %{n: seq},
        actor: "agent:test",
        producer_id: "writer:test",
        producer_seq: seq,
        tenant_id: tenant_id,
        source: nil,
        metadata: %{},
        refs: %{},
        idempotency_key: nil,
        inserted_at: inserted_at
      }
    end)
  end

  defp archived_event_count(session_id) when is_binary(session_id) and session_id != "" do
    Repo.aggregate(from(event in "events", where: event.session_id == ^session_id), :count)
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
end
