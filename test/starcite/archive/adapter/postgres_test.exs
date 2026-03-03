defmodule Starcite.Archive.Adapter.PostgresTest do
  use ExUnit.Case, async: false

  alias Starcite.Archive.Adapter.Postgres
  alias Starcite.Auth.Principal
  alias Starcite.Repo

  setup do
    ensure_repo_started()
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    on_exit(fn ->
      Ecto.Adapters.SQL.Sandbox.mode(Repo, :auto)
    end)

    :ok
  end

  test "upsert_session ignores stale runtime snapshots for existing rows" do
    id = "ses-pg-stale-#{System.unique_integer([:positive, :monotonic])}"
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             Postgres.upsert_session(%{
               id: id,
               title: "Original",
               tenant_id: "acme",
               creator_principal: principal("acme"),
               metadata: %{"source" => "original"},
               created_at: created_at,
               last_seq: 1,
               archived_seq: 1,
               last_progress_poll: 2
             })

    assert :ok =
             Postgres.upsert_session(%{
               id: id,
               title: "StaleOverwrite",
               tenant_id: "wrong",
               creator_principal: principal("wrong"),
               metadata: %{"source" => "stale"},
               created_at: created_at,
               last_seq: 1,
               archived_seq: 1,
               last_progress_poll: 2
             })

    assert {:ok, %{sessions: [session], next_cursor: nil}} =
             Postgres.list_sessions_by_ids([id], %{limit: 10, cursor: nil, metadata: %{}})

    assert session.title == "Original"
    assert session.tenant_id == "acme"
    assert session.metadata == %{"source" => "original"}
    assert session.last_seq == 1
    assert session.archived_seq == 1
    assert session.last_progress_poll == 2
  end

  test "upsert_session updates existing rows when runtime snapshot is newer" do
    id = "ses-pg-newer-#{System.unique_integer([:positive, :monotonic])}"
    created_at = DateTime.utc_now() |> DateTime.truncate(:second)

    assert :ok =
             Postgres.upsert_session(%{
               id: id,
               title: "Original",
               tenant_id: "acme",
               creator_principal: principal("acme"),
               metadata: %{"source" => "original"},
               created_at: created_at,
               last_seq: 1,
               archived_seq: 1,
               retention: %{"tail_keep" => 100},
               producer_cursors: %{},
               last_progress_poll: 2,
               snapshot_version: "v1"
             })

    assert :ok =
             Postgres.upsert_session(%{
               id: id,
               title: "Updated",
               tenant_id: "acme",
               creator_principal: principal("acme"),
               metadata: %{"source" => "updated"},
               created_at: created_at,
               last_seq: 2,
               archived_seq: 2,
               retention: %{"tail_keep" => 200},
               producer_cursors: %{},
               last_progress_poll: 3,
               snapshot_version: "v1"
             })

    assert {:ok, %{sessions: [session], next_cursor: nil}} =
             Postgres.list_sessions_by_ids([id], %{limit: 10, cursor: nil, metadata: %{}})

    assert session.title == "Updated"
    assert session.metadata == %{"source" => "updated"}
    assert session.last_seq == 2
    assert session.archived_seq == 2
    assert session.retention == %{"tail_keep" => 200}
    assert session.last_progress_poll == 3
    assert session.snapshot_version == "v1"
  end

  defp principal(tenant_id) when is_binary(tenant_id) and tenant_id != "" do
    %Principal{tenant_id: tenant_id, id: "pg-test", type: :service}
  end

  defp ensure_repo_started do
    if Process.whereis(Repo) do
      :ok
    else
      _pid = start_supervised!(Repo)
      :ok
    end
  end
end
