defmodule Starcite.Archive.Adapter.S3Test do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.S3
  alias Starcite.Archive.Adapter.S3.{Config, Layout}
  alias Starcite.Archive.Adapter.S3.Schema
  alias Starcite.Auth.Principal

  defmodule FakeClient do
    @store __MODULE__.Store

    def store_name, do: @store

    def get_object(%{bucket: bucket}, key) do
      Agent.update(@store, &increment_call(&1, :get, key))

      Agent.get_and_update(@store, fn state ->
        metrics = Map.get(state, :__metrics__, %{get_object_count: 0})

        response =
          case state |> bucket_objects(bucket) |> Map.get(key) do
            nil -> {:ok, :not_found}
            %{body: body, etag: etag} -> {:ok, {body, etag}}
          end

        {response, Map.put(state, :__metrics__, increment_metric(metrics, :get_object_count))}
      end)
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
          |> Enum.filter(&is_binary/1)
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Enum.sort()
        end)

      {:ok, keys}
    end

    def call_count(op, key) when op in [:get, :put] and is_binary(key) do
      Agent.get(@store, fn state ->
        state
        |> Map.get(:calls, %{})
        |> Map.get({op, key}, 0)
      end)
    end

    def list_keys_page(%{bucket: bucket}, prefix, opts \\ []) do
      max_keys = Keyword.get(opts, :max_keys, 1_000)
      start_after = Keyword.get(opts, :start_after)
      continuation_token = Keyword.get(opts, :continuation_token)

      keys =
        Agent.get(@store, fn state ->
          state
          |> bucket_objects(bucket)
          |> Map.keys()
          |> Enum.filter(&is_binary/1)
          |> Enum.filter(&String.starts_with?(&1, prefix))
          |> Enum.sort()
          |> then(fn sorted ->
            case continuation_token || start_after do
              nil -> sorted
              marker -> Enum.filter(sorted, &(&1 > marker))
            end
          end)
        end)

      page_keys = Enum.take(keys, max_keys)

      {:ok,
       %{
         keys: page_keys,
         next_continuation_token:
           if(length(keys) > max_keys and page_keys != [], do: List.last(page_keys), else: nil),
         truncated?: length(keys) > max_keys
       }}
    end

    def get_object_count do
      Agent.get(@store, fn state ->
        state
        |> Map.get(:__metrics__, %{})
        |> Map.get(:get_object_count, 0)
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

    defp increment_metric(metrics, key) do
      Map.update(metrics, key, 1, &(&1 + 1))
    end
  end

  setup do
    start_supervised!(%{
      id: FakeClient.store_name(),
      start:
        {Agent, :start_link,
         [
           fn -> %{objects: %{}, calls: %{}, __metrics__: %{get_object_count: 0}} end,
           [name: FakeClient.store_name()]
         ]}
    })

    session_prefix = "s3-test-#{System.unique_integer([:positive, :monotonic])}"
    config_key = {S3, :config}
    previous_config = :persistent_term.get(config_key, :undefined)

    :persistent_term.put(
      config_key,
      Config.build!([], bucket: "archive-test", prefix: session_prefix, client_mod: FakeClient)
    )

    on_exit(fn ->
      case previous_config do
        :undefined -> :persistent_term.erase(config_key)
        config -> :persistent_term.put(config_key, config)
      end
    end)

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
               archived_seq: 0,
               created_at: created_at
             })

    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 1..1))

    tenant = Base.url_encode64("acme", padding: false)
    session = Base.url_encode64(session_id, padding: false)

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/")

    assert "#{prefix}/sessions/v1/#{tenant}/#{session}.json" in keys
    assert "#{prefix}/session-tenants/v1/#{session}.json" in keys
    assert Layout.session_order_tenant_key(%{prefix: prefix}, "acme", session_id) in keys
    assert Layout.session_order_global_key(%{prefix: prefix}, "acme", session_id) in keys
    assert "#{prefix}/events/v1/#{tenant}/#{session}/1.ndjson" in keys

    refute "#{prefix}/sessions/v1/#{session}.json" in keys
    refute "#{prefix}/events/v1/#{session}/1.ndjson" in keys
  end

  test "reuses cached session tenant index across repeated chunk writes", %{
    prefix: prefix,
    bucket: bucket
  } do
    session_id = "ses-s3-cache-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    session_segment = Base.url_encode64(session_id, padding: false)
    tenant_index_key = "#{prefix}/session-tenants/v1/#{session_segment}.json"

    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 1..1))
    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 2..2))

    assert FakeClient.call_count(:put, tenant_index_key) == 1
    assert FakeClient.call_count(:get, tenant_index_key) == 0

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/session-tenants/")
    assert tenant_index_key in keys
  end

  test "fails loudly on cached tenant mismatch", %{prefix: prefix} do
    session_id = "ses-s3-conflict-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    session_segment = Base.url_encode64(session_id, padding: false)
    tenant_index_key = "#{prefix}/session-tenants/v1/#{session_segment}.json"

    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 1..1))

    conflicting_row =
      event_rows(session_id, inserted_at, 2..2)
      |> hd()
      |> Map.put(:tenant_id, "beta")

    assert {:error, :archive_write_unavailable} = S3.write_events([conflicting_row])
    assert FakeClient.call_count(:put, tenant_index_key) == 1
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
               archived_seq: 0,
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
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-b",
               title: "B",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-c",
               title: "C",
               tenant_id: "beta",
               creator_principal: principal_for_tenant("beta"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert {:error, :archive_write_unavailable} =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A-overwrite-attempt",
               tenant_id: "wrong",
               creator_principal: principal_for_tenant("wrong"),
               metadata: %{},
               archived_seq: 0,
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
    assert session_a.archived_seq == 0

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

  test "tenant-scoped pagination uses the ordered index in steady state", %{
    prefix: prefix,
    bucket: bucket
  } do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: "ses-a-acme",
               title: "A",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-b-beta",
               title: "B",
               tenant_id: "beta",
               creator_principal: principal_for_tenant("beta"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-c-acme",
               title: "C",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               Layout.session_order_ready_key(%{prefix: prefix}),
               ~s({"ready":true})
             )

    assert {:ok, page} =
             S3.list_sessions(%{limit: 1, cursor: nil, metadata: %{}, tenant_id: "acme"})

    assert Enum.map(page.sessions, & &1.id) == ["ses-a-acme"]
    assert page.next_cursor == "ses-a-acme"
    assert FakeClient.get_object_count() == 3
  end

  test "global pagination uses the ordered index in steady state", %{
    prefix: prefix,
    bucket: bucket
  } do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             S3.upsert_session(%{
               id: "ses-a",
               title: "A",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-b",
               title: "B",
               tenant_id: "beta",
               creator_principal: principal_for_tenant("beta"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             S3.upsert_session(%{
               id: "ses-c",
               title: "C",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               Layout.session_order_ready_key(%{prefix: prefix}),
               ~s({"ready":true})
             )

    assert {:ok, page_1} = S3.list_sessions(%{limit: 2, cursor: nil, metadata: %{}})
    assert Enum.map(page_1.sessions, & &1.id) == ["ses-a", "ses-b"]
    assert page_1.next_cursor == "ses-b"

    assert {:ok, page_2} =
             S3.list_sessions(%{limit: 2, cursor: page_1.next_cursor, metadata: %{}})

    assert Enum.map(page_2.sessions, & &1.id) == ["ses-c"]
    assert page_2.next_cursor == nil
  end

  test "backfill_session_order_indexes backfills every tenant and marks ready", %{
    prefix: prefix,
    bucket: bucket
  } do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)
    acme_session_id = "ses-startup-acme-#{System.unique_integer([:positive, :monotonic])}"
    beta_session_id = "ses-startup-beta-#{System.unique_integer([:positive, :monotonic])}"
    legacy_session_id = "ses-startup-legacy-#{System.unique_integer([:positive, :monotonic])}"
    acme_tenant = Base.url_encode64("acme", padding: false)
    beta_tenant = Base.url_encode64("beta", padding: false)
    acme_session = Base.url_encode64(acme_session_id, padding: false)
    beta_session = Base.url_encode64(beta_session_id, padding: false)
    legacy_session = Base.url_encode64(legacy_session_id, padding: false)

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               "#{prefix}/sessions/v1/#{acme_tenant}/#{acme_session}.json",
               Schema.encode_session(%{
                 id: acme_session_id,
                 title: "Acme startup",
                 tenant_id: "acme",
                 creator_principal: principal_for_tenant("acme"),
                 metadata: %{},
                 archived_seq: 0,
                 created_at: created_at
               })
             )

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               "#{prefix}/sessions/v1/#{beta_tenant}/#{beta_session}.json",
               Schema.encode_session(%{
                 id: beta_session_id,
                 title: "Beta startup",
                 tenant_id: "beta",
                 creator_principal: principal_for_tenant("beta"),
                 metadata: %{},
                 archived_seq: 0,
                 created_at: created_at
               })
             )

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               "#{prefix}/sessions/v1/#{legacy_session}.json",
               Schema.encode_session(%{
                 id: legacy_session_id,
                 title: "Legacy startup",
                 tenant_id: "acme",
                 creator_principal: principal_for_tenant("acme"),
                 metadata: %{},
                 archived_seq: 0,
                 created_at: created_at
               })
             )

    assert {:ok,
            %{
              ready_already?: false,
              tenants: 2,
              tenant_sessions: 2,
              legacy_sessions: 1,
              sessions_total: 3
            }} = S3.backfill_session_order_indexes()

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/session-order/v1/")

    assert Layout.session_order_ready_key(%{prefix: prefix}) in keys
    assert Layout.session_order_tenant_key(%{prefix: prefix}, "acme", acme_session_id) in keys
    assert Layout.session_order_tenant_key(%{prefix: prefix}, "beta", beta_session_id) in keys
    assert Layout.session_order_tenant_key(%{prefix: prefix}, "acme", legacy_session_id) in keys
  end

  test "list_sessions backfills session-order indexes for legacy session blobs", %{
    prefix: prefix,
    bucket: bucket
  } do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)
    session_id = "ses-s3-legacy-#{System.unique_integer([:positive, :monotonic])}"
    tenant = Base.url_encode64("acme", padding: false)
    session = Base.url_encode64(session_id, padding: false)

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               "#{prefix}/sessions/v1/#{tenant}/#{session}.json",
               Schema.encode_session(%{
                 id: session_id,
                 title: "Legacy",
                 tenant_id: "acme",
                 creator_principal: principal_for_tenant("acme"),
                 metadata: %{},
                 archived_seq: 0,
                 created_at: created_at
               })
             )

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               "#{prefix}/session-tenants/v1/#{session}.json",
               Schema.encode_session_tenant_index("acme")
             )

    assert {:ok, page} =
             S3.list_sessions(%{limit: 10, cursor: nil, metadata: %{}, tenant_id: "acme"})

    assert [%{id: ^session_id}] = page.sessions

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/session-order/v1/")
    assert Layout.session_order_tenant_key(%{prefix: prefix}, "acme", session_id) in keys
    assert Layout.session_order_global_key(%{prefix: prefix}, "acme", session_id) in keys
    assert Layout.session_order_ready_key(%{prefix: prefix}) in keys
  end

  test "update_session_archived_seq updates the stored session row" do
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)
    session_id = "ses-s3-archived-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             S3.upsert_session(%{
               id: session_id,
               title: "Cursor",
               tenant_id: "acme",
               creator_principal: principal_for_tenant("acme"),
               metadata: %{"tag" => "x"},
               archived_seq: 0,
               created_at: created_at
             })

    assert :ok = S3.update_session_archived_seq(session_id, "acme", 7)

    assert {:ok, page} =
             S3.list_sessions_by_ids(
               [session_id],
               %{limit: 10, cursor: nil, metadata: %{}, tenant_id: "acme"}
             )

    assert [%{id: ^session_id, archived_seq: 7, metadata: %{"tag" => "x"}}] = page.sessions
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
