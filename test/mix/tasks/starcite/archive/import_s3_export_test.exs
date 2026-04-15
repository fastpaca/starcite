defmodule Mix.Tasks.Starcite.Archive.ImportS3ExportTest do
  use ExUnit.Case, async: false

  import Ecto.Query
  import ExUnit.CaptureIO

  alias Starcite.Repo
  alias Starcite.Session.Header
  alias Starcite.Storage.EventArchive
  alias Starcite.Storage.SessionCatalog

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    clear_persisted_state()
    Mix.Task.reenable("starcite.archive.import_s3_export")
    Mix.Task.reenable("starcite.archive.reconcile_progress")
    Mix.Task.reenable("starcite.archive.verify_cutover")
    :ok
  end

  test "import task copies archive rows without mutating archived progress" do
    root = temp_export_root()
    on_exit(fn -> File.rm_rf!(root) end)

    assert :ok = SessionCatalog.persist_created(Header.new("ses-import-gap", tenant_id: "acme"))

    assert :ok =
             SessionCatalog.persist_created(Header.new("ses-import-legacy", tenant_id: "beta"))

    write_export_file(
      tenant_scoped_export_path(root, "acme", "ses-import-gap", 1),
      [event_line(1, tenant_id: "acme"), event_line(2, tenant_id: "acme")]
    )

    write_export_file(
      tenant_scoped_export_path(root, "acme", "ses-import-gap", 257),
      [event_line(257, tenant_id: "acme"), event_line(258, tenant_id: "acme")]
    )

    write_export_file(tenantless_export_path(root, "ses-import-legacy", 1), [event_line(1)])

    output =
      capture_io(fn ->
        Mix.Task.run("starcite.archive.import_s3_export", ["--root", root])
      end)

    assert output =~ "import_complete files=3 rows=5 inserted=5 dry_run=false"
    assert event_seqs("ses-import-gap") == [1, 2, 257, 258]
    assert event_seqs("ses-import-legacy") == [1]
    assert {:ok, 0} = SessionCatalog.get_progress("ses-import-gap")
    assert {:ok, 0} = SessionCatalog.get_progress("ses-import-legacy")
  end

  test "import task dry-run leaves archive rows and progress unchanged" do
    root = temp_export_root()
    on_exit(fn -> File.rm_rf!(root) end)

    assert :ok =
             SessionCatalog.persist_created(Header.new("ses-import-dry-run", tenant_id: "acme"))

    write_export_file(
      tenant_scoped_export_path(root, "acme", "ses-import-dry-run", 1),
      [event_line(1, tenant_id: "acme"), event_line(2, tenant_id: "acme")]
    )

    output =
      capture_io(fn ->
        Mix.Task.run("starcite.archive.import_s3_export", [
          "--root",
          root,
          "--dry-run"
        ])
      end)

    assert output =~ "import_complete files=1 rows=2 inserted=0 dry_run=true"
    assert event_seqs("ses-import-dry-run") == []
    assert {:ok, 0} = SessionCatalog.get_progress("ses-import-dry-run")
  end

  test "reconcile task recomputes archived progress from contiguous imported rows" do
    inserted_at = NaiveDateTime.utc_now()

    assert :ok =
             SessionCatalog.persist_created(Header.new("ses-reconcile-gap", tenant_id: "acme"))

    assert :ok =
             SessionCatalog.persist_created(Header.new("ses-reconcile-empty", tenant_id: "acme"))

    assert {:ok, 3} =
             EventArchive.write_events(event_rows("ses-reconcile-gap", inserted_at, [1, 2, 257]))

    assert :ok =
             SessionCatalog.put_progress_batch([
               %{session_id: "ses-reconcile-gap", archived_seq: 5},
               %{session_id: "ses-reconcile-empty", archived_seq: 7}
             ])

    total_sessions = session_count()

    output =
      capture_io(fn ->
        Mix.Task.run("starcite.archive.reconcile_progress", [])
      end)

    assert output =~
             "reconcile_complete total_sessions=#{total_sessions} archived_sessions=1 updated_sessions=2 orphan_sessions=0 dry_run=false"

    assert {:ok, 2} = SessionCatalog.get_progress("ses-reconcile-gap")
    assert {:ok, 0} = SessionCatalog.get_progress("ses-reconcile-empty")
  end

  test "verify task reports cutover ready after reconcile" do
    inserted_at = NaiveDateTime.utc_now()

    assert :ok = SessionCatalog.persist_created(Header.new("ses-verify-ready", tenant_id: "acme"))

    assert {:ok, 2} =
             EventArchive.write_events(event_rows("ses-verify-ready", inserted_at, [1, 2]))

    _ =
      capture_io(fn ->
        Mix.Task.run("starcite.archive.reconcile_progress", [])
      end)

    total_sessions = session_count()

    output =
      capture_io(fn ->
        Mix.Task.run("starcite.archive.verify_cutover", [])
      end)

    assert output =~
             "cutover_ready total_sessions=#{total_sessions} archived_sessions=1 mismatch_sessions=0 orphan_sessions=0"
  end

  test "verify task blocks cutover on mismatched progress and orphan archive rows" do
    inserted_at = NaiveDateTime.utc_now()

    assert :ok =
             SessionCatalog.persist_created(Header.new("ses-verify-mismatch", tenant_id: "acme"))

    assert {:ok, 1} =
             EventArchive.write_events(event_rows("ses-verify-mismatch", inserted_at, [1]))

    assert {:ok, 2} =
             EventArchive.write_events(event_rows("ses-verify-orphan", inserted_at, [1, 2]))

    assert :ok =
             SessionCatalog.put_progress_batch([
               %{session_id: "ses-verify-mismatch", archived_seq: 3}
             ])

    total_sessions = session_count()

    output =
      capture_io(fn ->
        assert_raise Mix.Error, "archive cutover verification failed", fn ->
          Mix.Task.run("starcite.archive.verify_cutover", ["--limit", "5"])
        end
      end)

    assert output =~
             "cutover_blocked total_sessions=#{total_sessions} archived_sessions=1 mismatch_sessions=1 orphan_sessions=1 limit=5"

    assert output =~ "mismatch session_id=ses-verify-mismatch archived_seq=3 contiguous_seq=1"
    assert output =~ "orphan session_id=ses-verify-orphan event_count=2 max_seq=2"
  end

  defp temp_export_root do
    root =
      Path.join([
        System.tmp_dir!(),
        "starcite-import-task-test-#{System.unique_integer([:positive, :monotonic])}"
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
  end

  defp event_rows(session_id, inserted_at, seqs)
       when is_binary(session_id) and session_id != "" and is_struct(inserted_at, NaiveDateTime) and
              is_list(seqs) do
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

  defp maybe_put(map, _key, nil) when is_map(map), do: map
  defp maybe_put(map, key, value) when is_map(map), do: Map.put(map, key, value)

  defp event_seqs(session_id) when is_binary(session_id) and session_id != "" do
    Repo.all(
      from(event in "events",
        where: event.session_id == ^session_id,
        order_by: [asc: event.seq],
        select: event.seq
      )
    )
  end

  defp encode_segment(value) when is_binary(value) and value != "" do
    Base.url_encode64(value, padding: false)
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

  defp clear_persisted_state do
    _ = Repo.delete_all("events")
    _ = Repo.delete_all("sessions")
    :ok
  end

  defp session_count do
    Repo.aggregate("sessions", :count)
  end
end
