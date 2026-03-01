defmodule Starcite.Archive.Adapter.S3Test do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.S3
  alias Starcite.Archive.Adapter.S3.SchemaControl
  alias Starcite.Archive.Adapter.S3.Schema
  alias Starcite.Auth.Principal

  defmodule FakeClient do
    @store __MODULE__.Store

    def store_name, do: @store

    def get_object(%{bucket: bucket}, key) do
      case Agent.get(@store, fn state -> state |> Map.get(bucket, %{}) |> Map.get(key) end) do
        nil -> {:ok, :not_found}
        %{body: body, etag: etag} -> {:ok, {body, etag}}
      end
    end

    def put_object(%{bucket: bucket}, key, body, opts \\ []) do
      Agent.get_and_update(@store, fn state ->
        existing = state |> Map.get(bucket, %{}) |> Map.get(key)

        if precondition_failed?(existing, opts) do
          {{:error, :precondition_failed}, state}
        else
          bucket_state = Map.get(state, bucket, %{})
          etag = etag_for(body)

          next_state =
            Map.put(state, bucket, Map.put(bucket_state, key, %{body: body, etag: etag}))

          {:ok, next_state}
        end
      end)
    end

    def list_keys(%{bucket: bucket}, prefix) do
      keys =
        Agent.get(@store, fn state ->
          state
          |> Map.get(bucket, %{})
          |> Map.keys()
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Enum.sort()
        end)

      {:ok, keys}
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
  end

  setup do
    start_supervised!(%{
      id: FakeClient.store_name(),
      start: {Agent, :start_link, [fn -> %{} end, [name: FakeClient.store_name()]]}
    })

    session_prefix = "s3-test-#{System.unique_integer([:positive, :monotonic])}"

    migration_config = %{
      bucket: "archive-test",
      prefix: session_prefix,
      client_mod: FakeClient,
      request_opts: []
    }

    assert {:ok, _stats} = SchemaControl.migrate(migration_config, actor: "test")

    start_supervised!(
      {S3,
       [
         bucket: "archive-test",
         prefix: session_prefix,
         client_mod: FakeClient
       ]}
    )

    {:ok, prefix: session_prefix, bucket: "archive-test"}
  end

  test "write_events is idempotent and reads across chunk boundaries" do
    session_id = "ses-s3-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    rows = event_rows(session_id, inserted_at, 1..260)

    assert {:ok, 260} = S3.write_events(rows)
    assert {:ok, 0} = S3.write_events(rows)

    assert {:ok, events} = S3.read_events(session_id, 255, 258)
    assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
    assert Enum.all?(events, &(&1.tenant_id == "acme"))
    assert Enum.all?(events, &is_binary(&1.inserted_at))
  end

  test "stores sessions and event chunks in tenant subfolders", %{prefix: prefix, bucket: bucket} do
    session_id = "ses-s3-layout-#{System.unique_integer([:positive, :monotonic])}"
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: session_id,
               title: "Layout",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               created_at: created_at
             })

    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 1..1))

    tenant = Base.url_encode64("acme", padding: false)
    session = Base.url_encode64(session_id, padding: false)

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/")

    assert "#{prefix}/sessions/v1/#{tenant}/#{session}.json" in keys
    assert "#{prefix}/session-tenants/v1/#{session}.json" in keys
    assert "#{prefix}/events/v1/#{tenant}/#{session}/1.ndjson" in keys

    refute "#{prefix}/sessions/v1/#{session}.json" in keys
    refute "#{prefix}/events/v1/#{session}/1.ndjson" in keys
  end

  test "write_events rejects mixed-tenant rows for one session chunk" do
    session_id = "ses-s3-mixed-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    [first, second] = event_rows(session_id, inserted_at, 1..2)

    assert {:error, :archive_write_unavailable} =
             S3.write_events([first, %{second | tenant_id: "beta"}])
  end

  test "read_events fails on unsupported event schema versions", %{prefix: prefix, bucket: bucket} do
    session_id = "ses-s3-schema-#{System.unique_integer([:positive, :monotonic])}"
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: session_id,
               title: "Schema",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               created_at: created_at
             })

    tenant_segment = Base.url_encode64("acme", padding: false)
    session_segment = Base.url_encode64(session_id, padding: false)

    key = "#{prefix}/events/v1/#{tenant_segment}/#{session_segment}/1.ndjson"

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               key,
               Jason.encode!(%{
                 schema_version: Schema.event_schema_version() + 1,
                 seq: 1,
                 type: "content",
                 payload: %{"n" => 1},
                 actor: "agent:test",
                 producer_id: "writer:test",
                 producer_seq: 1,
                 tenant_id: "acme",
                 source: nil,
                 metadata: %{},
                 refs: %{},
                 idempotency_key: nil,
                 inserted_at: "2026-01-01T00:00:00"
               })
             )

    assert {:error, :archive_read_unavailable} = S3.read_events(session_id, 1, 1)
  end

  test "list_sessions_by_ids fails on unsupported tenant-index schema versions", %{
    prefix: prefix,
    bucket: bucket
  } do
    session_id = "ses-s3-index-schema-#{System.unique_integer([:positive, :monotonic])}"
    session_segment = Base.url_encode64(session_id, padding: false)
    key = "#{prefix}/session-tenants/v1/#{session_segment}.json"

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               key,
               Jason.encode!(%{
                 schema_version: Schema.session_tenant_index_schema_version() + 1,
                 tenant_id: "acme"
               })
             )

    assert {:error, :archive_read_unavailable} =
             S3.list_sessions_by_ids([session_id], %{limit: 10, cursor: nil, metadata: %{}})
  end

  test "upserts and lists sessions with metadata filtering and cursor paging" do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-b",
               title: "B",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-c",
               title: "C",
               tenant_id: "beta",
               creator_principal: principal_for_tenant("beta"),
               metadata: %{},
               created_at: created_at
             })

    assert {:error, :archive_write_unavailable} =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A-overwrite-attempt",
               tenant_id: "wrong",
               creator_principal: principal_for_tenant("wrong"),
               metadata: %{},
               created_at: created_at
             })

    assert {:ok, page_1} =
             S3.list_sessions(%{limit: 1, cursor: nil, metadata: %{}, tenant_id: "acme"})

    assert length(page_1.sessions) == 1
    assert is_binary(page_1.next_cursor)

    assert {:ok, page_2} =
             S3.list_sessions(%{
               limit: 2,
               cursor: page_1.next_cursor,
               metadata: %{},
               tenant_id: "acme"
             })

    listed_ids =
      (page_1.sessions ++ page_2.sessions)
      |> Enum.map(& &1.id)
      |> Enum.sort()

    assert listed_ids == ["ses-a", "ses-b"]

    session_a =
      (page_1.sessions ++ page_2.sessions)
      |> Enum.find(&(&1.id == "ses-a"))

    assert session_a.title == "A"
    assert session_a.tenant_id == "acme"

    assert {:ok, by_ids} =
             S3.list_sessions_by_ids(
               ["ses-c", "ses-a", "missing"],
               %{limit: 10, cursor: nil, metadata: %{}}
             )

    assert by_ids.sessions |> Enum.map(& &1.id) |> Enum.sort() == ["ses-a", "ses-c"]

    assert {:ok, by_ids_tenant_scoped} =
             S3.list_sessions_by_ids(
               ["ses-c", "ses-a", "missing"],
               %{limit: 10, cursor: nil, metadata: %{}, tenant_id: "acme"}
             )

    assert by_ids_tenant_scoped.sessions |> Enum.map(& &1.id) == ["ses-a"]
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

  defp principal_for_tenant(tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    %Principal{tenant_id: tenant_id, id: "s3-test", type: :service}
  end
end
