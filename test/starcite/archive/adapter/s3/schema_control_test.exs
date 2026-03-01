defmodule Starcite.Archive.Adapter.S3.SchemaControlTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.S3.{Layout, Schema, SchemaControl}

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

    config = %{
      bucket: "archive-schema-control-test",
      prefix: "s3-schema-control-#{System.unique_integer([:positive, :monotonic])}",
      client_mod: FakeClient,
      request_opts: []
    }

    {:ok, config: config}
  end

  test "startup compatibility requires migration when manifest is behind", %{config: config} do
    assert {:error, :archive_schema_migration_required} =
             SchemaControl.ensure_startup_compatibility(config)
  end

  test "manual migration updates manifest and unblocks startup compatibility", %{config: config} do
    assert {:ok, stats} = SchemaControl.migrate(config, actor: "test")

    assert stats.event_migrations_needed == 0
    assert stats.session_migrations_needed == 0
    assert stats.index_migrations_needed == 0

    assert :ok = SchemaControl.ensure_startup_compatibility(config)

    meta_key = Layout.schema_meta_key(config)

    assert {:ok, {body, _etag}} = FakeClient.get_object(config, meta_key)
    decoded = Jason.decode!(body)

    assert decoded["state"] == "ready"
    assert decoded["current_versions"]["event_chunk"] == Schema.event_schema_version()
    assert decoded["current_versions"]["session"] == Schema.session_schema_version()

    assert decoded["current_versions"]["session_tenant_index"] ==
             Schema.session_tenant_index_schema_version()

    assert decoded["last_migration"]["status"] == "succeeded"
    assert decoded["last_migration"]["actor"] == "test"
  end

  test "startup compatibility rejects manifest schema newer than binary", %{config: config} do
    meta_key = Layout.schema_meta_key(config)

    assert :ok =
             FakeClient.put_object(
               config,
               meta_key,
               Jason.encode!(%{
                 manifest_version: 1,
                 state: "ready",
                 current_versions: %{
                   event_chunk: Schema.event_schema_version() + 1,
                   session: Schema.session_schema_version(),
                   session_tenant_index: Schema.session_tenant_index_schema_version()
                 }
               })
             )

    assert {:error, :archive_schema_version_unsupported} =
             SchemaControl.ensure_startup_compatibility(config)
  end
end
