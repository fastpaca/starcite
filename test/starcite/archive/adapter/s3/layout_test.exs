defmodule Starcite.Archive.Adapter.S3.LayoutTest do
  use ExUnit.Case, async: true

  alias Starcite.Archive.Adapter.S3.Layout

  test "chunk boundaries follow cache-line aligned chunk size" do
    assert Layout.chunk_start_for(1, 256) == 1
    assert Layout.chunk_start_for(256, 256) == 1
    assert Layout.chunk_start_for(257, 256) == 257
    assert Layout.chunk_start_for(512, 256) == 257
  end

  test "chunk_starts_for_range returns all chunk anchors in range" do
    assert Layout.chunk_starts_for_range(1, 1, 256) == [1]
    assert Layout.chunk_starts_for_range(255, 258, 256) == [1, 257]
    assert Layout.chunk_starts_for_range(260, 800, 256) == [257, 513, 769]
  end

  test "group_event_rows groups by tenant, session id, and chunk" do
    rows = [
      %{tenant_id: "acme", session_id: "s1", seq: 1},
      %{tenant_id: "acme", session_id: "s1", seq: 10},
      %{tenant_id: "acme", session_id: "s1", seq: 257},
      %{tenant_id: "beta", session_id: "s2", seq: 1}
    ]

    grouped = Layout.group_event_rows(rows, 256)

    assert Map.keys(grouped) |> Enum.sort() == [
             {"acme", "s1", 1},
             {"acme", "s1", 257},
             {"beta", "s2", 1}
           ]

    assert Enum.map(grouped[{"acme", "s1", 1}], & &1.seq) == [1, 10]
    assert Enum.map(grouped[{"acme", "s1", 257}], & &1.seq) == [257]
    assert Enum.map(grouped[{"beta", "s2", 1}], & &1.seq) == [1]
  end

  test "key layout encodes tenant/session ids and extensions" do
    config = %{prefix: "starcite"}
    tenant = Base.url_encode64("acme", padding: false)
    session = Base.url_encode64("ses-1", padding: false)

    assert Layout.event_chunk_key(config, "acme", "ses-1", 257) ==
             "starcite/events/v1/#{tenant}/#{session}/257.ndjson"

    assert Layout.legacy_event_chunk_key(config, "ses-1", 257) ==
             "starcite/events/v1/#{session}/257.ndjson"

    assert Layout.event_prefix(config) == "starcite/events/v1/"

    assert Layout.session_prefix(config) == "starcite/sessions/v1/"

    assert Layout.session_key(config, "acme", "ses-1") ==
             "starcite/sessions/v1/#{tenant}/#{session}.json"

    assert Layout.legacy_session_key(config, "ses-1") == "starcite/sessions/v1/#{session}.json"

    assert Layout.session_tenant_index_key(config, "ses-1") ==
             "starcite/session-tenants/v1/#{session}.json"

    assert Layout.session_tenant_index_prefix(config) == "starcite/session-tenants/v1/"

    assert Layout.schema_prefix(config) == "starcite/schema/"
    assert Layout.schema_meta_key(config) == "starcite/schema/meta.json"
  end
end
