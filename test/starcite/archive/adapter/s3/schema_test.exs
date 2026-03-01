defmodule Starcite.Archive.Adapter.S3.SchemaTest do
  use ExUnit.Case, async: true

  alias Starcite.Archive.Adapter.S3.Schema

  test "decode_event_chunk migrates legacy unversioned rows with expected tenant" do
    body =
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

    assert {:ok, [event], true} = Schema.decode_event_chunk(body, "acme")
    assert event.seq == 1
    assert event.tenant_id == "acme"
  end

  test "decode_event_chunk rejects unsupported future schema versions" do
    body =
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

    assert {:error, :archive_read_unavailable} = Schema.decode_event_chunk(body, "acme")
  end

  test "decode_session migrates legacy direct session payloads" do
    legacy_body =
      Jason.encode!(%{
        id: "ses-1",
        title: "Legacy",
        tenant_id: "acme",
        creator_principal: %{"id" => "svc", "type" => "service", "tenant_id" => "acme"},
        metadata: %{},
        created_at: "2026-01-01T00:00:00Z"
      })

    assert {:ok, session, true} = Schema.decode_session(legacy_body)
    assert session.id == "ses-1"
    assert session.tenant_id == "acme"
  end

  test "decode_session_tenant_index rejects unsupported future schema versions" do
    body =
      Jason.encode!(%{
        schema_version: Schema.session_tenant_index_schema_version() + 1,
        tenant_id: "acme"
      })

    assert {:error, :archive_read_unavailable} = Schema.decode_session_tenant_index(body)
  end
end
