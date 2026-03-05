defmodule Starcite.Session.RuntimeSnapshotTest do
  use ExUnit.Case, async: true

  alias Starcite.Session
  alias Starcite.Session.RuntimeSnapshot

  test "round-trips minimal snapshot through metadata" do
    session =
      Session.new("ses-1",
        tenant_id: "acme",
        metadata: %{"source" => "test"},
        tail_keep: 128,
        producer_max_entries: 512
      )

    session = %Session{session | last_seq: 10, archived_seq: 8}

    snapshot = RuntimeSnapshot.from_session(session)
    metadata = RuntimeSnapshot.put_in_metadata(session.metadata, snapshot)

    assert {:ok, decoded} = RuntimeSnapshot.decode_from_metadata(metadata)
    assert decoded == snapshot
    assert RuntimeSnapshot.drop_from_metadata(metadata) == %{"source" => "test"}
  end

  test "returns snapshot_missing when runtime metadata key is absent" do
    assert {:error, :snapshot_missing} = RuntimeSnapshot.decode_from_metadata(%{})
  end

  test "rejects missing required snapshot fields" do
    metadata = %{
      RuntimeSnapshot.metadata_key() => %{
        "schema_version" => 1,
        "archived_seq" => 5,
        "tail_keep" => 100
      }
    }

    assert {:error, :invalid_snapshot} = RuntimeSnapshot.decode_from_metadata(metadata)
  end

  test "rejects unsupported snapshot schema versions" do
    metadata = %{
      RuntimeSnapshot.metadata_key() => %{
        "schema_version" => 99,
        "archived_seq" => 0,
        "tail_keep" => 100,
        "producer_max_entries" => 10_000
      }
    }

    assert {:error, :unsupported_snapshot_version} =
             RuntimeSnapshot.decode_from_metadata(metadata)
  end

  test "rejects invalid archived sequence type" do
    metadata = %{
      RuntimeSnapshot.metadata_key() => %{
        "schema_version" => 1,
        "archived_seq" => -1,
        "tail_keep" => 100,
        "producer_max_entries" => 10_000
      }
    }

    assert {:error, :invalid_snapshot} = RuntimeSnapshot.decode_from_metadata(metadata)
  end
end
