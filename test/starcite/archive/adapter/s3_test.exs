defmodule Starcite.Storage.EventArchive.S3Test do
  use ExUnit.Case, async: false

  alias Starcite.Storage.EventArchive.S3
  alias Starcite.Storage.EventArchive.S3.Config
  alias Starcite.Storage.EventArchive.S3.Schema

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
    start_supervised!(%{
      id: FakeClient.store_name(),
      start:
        {Agent, :start_link,
         [fn -> %{objects: %{}, calls: %{}} end, [name: FakeClient.store_name()]]}
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

    assert {:ok, events} = S3.read_events(session_id, "acme", 255, 258)
    assert Enum.map(events, & &1.seq) == [255, 256, 257, 258]
    assert Enum.all?(events, &(&1.tenant_id == "acme"))
    assert Enum.all?(events, &is_binary(&1.inserted_at))
  end

  test "stores event chunks in tenant subfolders", %{prefix: prefix, bucket: bucket} do
    session_id = "ses-s3-layout-#{System.unique_integer([:positive, :monotonic])}"
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

    assert {:ok, 1} = S3.write_events(event_rows(session_id, inserted_at, 1..1))

    tenant = Base.url_encode64("acme", padding: false)
    session = Base.url_encode64(session_id, padding: false)

    assert {:ok, keys} = FakeClient.list_keys(%{bucket: bucket}, "#{prefix}/")

    assert "#{prefix}/events/v1/#{tenant}/#{session}/1.ndjson" in keys

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

    assert {:error, :archive_read_unavailable} = S3.read_events(session_id, "acme", 1, 1)
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
end
