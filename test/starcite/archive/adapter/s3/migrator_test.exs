defmodule Starcite.Storage.EventArchive.S3.MigratorTest do
  use ExUnit.Case, async: false

  alias Starcite.Storage.EventArchive.S3.{Migrator, Schema}

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

    prefix = "s3-migrator-#{System.unique_integer([:positive, :monotonic])}"
    bucket = "archive-migrator-test"

    config = %{
      bucket: bucket,
      prefix: prefix,
      client_mod: FakeClient,
      request_opts: []
    }

    {:ok, config: config, bucket: bucket, prefix: prefix}
  end

  test "rewrites legacy event chunk payloads", %{
    config: config,
    bucket: bucket,
    prefix: prefix
  } do
    session_id = "ses-migrator-1"
    session_segment = encode_segment(session_id)
    legacy_event_key = "#{prefix}/events/v1/#{session_segment}/1.ndjson"

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               legacy_event_key,
               Jason.encode!(%{
                 seq: 1,
                 type: "content",
                 payload: %{"n" => 1},
                 actor: "agent:test",
                 producer_id: "writer:test",
                 producer_seq: 1,
                 source: nil,
                 metadata: %{},
                 refs: %{},
                 idempotency_key: nil,
                 inserted_at: "2026-01-01T00:00:00"
               })
             )

    assert {:ok, stats} = Migrator.run(config)
    assert stats.event_migrations_needed == 1
    assert stats.event_rewritten == 1

    assert {:ok, {event_body, _etag}} = FakeClient.get_object(%{bucket: bucket}, legacy_event_key)
    [event_line] = String.split(event_body, "\n", trim: true)
    decoded_event = Jason.decode!(event_line)
    assert decoded_event["schema_version"] == Schema.event_schema_version()
    refute Map.has_key?(decoded_event, "tenant_id")
  end

  test "fails migration on unsupported future schema", %{
    config: config,
    bucket: bucket,
    prefix: prefix
  } do
    session_id = "ses-migrator-unsupported"
    event_key = "#{prefix}/events/v1/#{encode_segment(session_id)}/1.ndjson"

    assert :ok =
             FakeClient.put_object(
               %{bucket: bucket},
               event_key,
               Jason.encode!(%{
                 schema_version: Schema.event_schema_version() + 1,
                 seq: 1,
                 type: "content",
                 payload: %{"n" => 1},
                 actor: "agent:test",
                 producer_id: "writer:test",
                 producer_seq: 1,
                 source: nil,
                 metadata: %{},
                 refs: %{},
                 idempotency_key: nil,
                 inserted_at: "2026-01-01T00:00:00"
               })
             )

    assert {:error, :archive_read_unavailable} = Migrator.run(config)
  end

  defp encode_segment(value), do: Base.url_encode64(value, padding: false)
end
