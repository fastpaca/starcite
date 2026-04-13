defmodule Starcite.Storage.EventArchive.LegacyS3ExportTest do
  use ExUnit.Case, async: true

  alias Starcite.Storage.EventArchive.LegacyS3Export

  test "exported_files discovers tenant-scoped and tenantless legacy archive paths" do
    root = temp_export_root()
    on_exit(fn -> File.rm_rf!(root) end)
    tenant_scoped_path = tenant_scoped_export_path(root, "acme", "ses-current", 1)
    tenantless_path = tenantless_export_path(root, "ses-legacy", 257)

    write_export_file(tenant_scoped_path, [event_line(1, tenant_id: "acme", schema_version: 2)])
    write_export_file(tenantless_path, [event_line(257)])

    assert {:ok, files} = LegacyS3Export.exported_files(root)

    assert Enum.map(files, fn file ->
             {file.session_id, Map.get(file, :tenant_id), file.chunk_start}
           end) == [
             {"ses-current", "acme", 1},
             {"ses-legacy", nil, 257}
           ]
  end

  test "read_file decodes legacy tenantless exports when a tenant override is provided" do
    root = temp_export_root()
    on_exit(fn -> File.rm_rf!(root) end)
    path = tenantless_export_path(root, "ses-legacy-read", 1)
    write_export_file(path, [event_line(1)])

    file = LegacyS3Export.export_file!(path)

    assert {:error, :archive_export_missing_tenant} = LegacyS3Export.read_file(file)

    assert {:ok, [row]} = LegacyS3Export.read_file(file, tenant_id: "acme")

    assert row.session_id == "ses-legacy-read"
    assert row.tenant_id == "acme"
    assert row.seq == 1
  end

  test "read_file rejects unsupported future schema versions" do
    root = temp_export_root()
    on_exit(fn -> File.rm_rf!(root) end)
    path = tenant_scoped_export_path(root, "acme", "ses-future-schema", 1)

    write_export_file(path, [event_line(1, tenant_id: "acme", schema_version: 3)])

    assert {:error, :archive_export_invalid} =
             path
             |> LegacyS3Export.export_file!()
             |> LegacyS3Export.read_file()
  end

  defp temp_export_root do
    root =
      Path.join([
        System.tmp_dir!(),
        "starcite-legacy-export-test-#{System.unique_integer([:positive, :monotonic])}"
      ])

    File.mkdir_p!(root)
    root
  end

  defp tenant_scoped_export_path(root, tenant_id, session_id, chunk_start)
       when is_binary(root) and is_binary(tenant_id) and is_binary(session_id) and
              is_integer(chunk_start) and chunk_start > 0 do
    Path.join([
      root,
      "archive",
      "events",
      "v1",
      encode_segment(tenant_id),
      encode_segment(session_id),
      "#{chunk_start}.ndjson"
    ])
  end

  defp tenantless_export_path(root, session_id, chunk_start)
       when is_binary(root) and is_binary(session_id) and is_integer(chunk_start) and
              chunk_start > 0 do
    Path.join([
      root,
      "archive",
      "events",
      "v1",
      encode_segment(session_id),
      "#{chunk_start}.ndjson"
    ])
  end

  defp write_export_file(path, rows) when is_binary(path) and is_list(rows) do
    File.mkdir_p!(Path.dirname(path))

    body =
      rows
      |> Enum.map(&Jason.encode!/1)
      |> Enum.join("\n")

    File.write!(path, body <> "\n")
  end

  defp event_line(seq, opts \\ []) when is_integer(seq) and seq > 0 and is_list(opts) do
    tenant_id = Keyword.get(opts, :tenant_id)
    schema_version = Keyword.get(opts, :schema_version)

    %{
      "seq" => seq,
      "type" => "content",
      "payload" => %{"n" => seq},
      "actor" => "agent:test",
      "producer_id" => "writer:test",
      "producer_seq" => seq,
      "source" => nil,
      "metadata" => %{},
      "refs" => %{},
      "idempotency_key" => nil,
      "inserted_at" => "2026-01-01T00:00:00"
    }
    |> maybe_put("tenant_id", tenant_id)
    |> maybe_put("schema_version", schema_version)
  end

  defp maybe_put(map, _key, nil) when is_map(map), do: map
  defp maybe_put(map, key, value) when is_map(map), do: Map.put(map, key, value)

  defp encode_segment(value) when is_binary(value) and value != "" do
    Base.url_encode64(value, padding: false)
  end
end
