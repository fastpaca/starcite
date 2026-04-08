defmodule Starcite.Storage.SessionCatalogTest do
  use ExUnit.Case, async: false

  alias Starcite.Auth.Principal
  alias Starcite.Repo
  alias Starcite.Session.Header
  alias Starcite.Storage.SessionCatalog

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    :ok
  end

  test "persists headers and rehydrates sessions from the catalog" do
    header =
      Header.new("ses-catalog-1",
        tenant_id: "acme",
        title: "Draft",
        creator_principal: %Principal{tenant_id: "acme", id: "svc", type: :service},
        metadata: %{workflow: "contract"}
      )

    assert :ok = SessionCatalog.persist_created(header)

    assert :ok =
             SessionCatalog.put_progress_batch([
               %{session_id: "ses-catalog-1", archived_seq: 12}
             ])

    assert {:ok, loaded_header} = SessionCatalog.get_header("ses-catalog-1")
    assert loaded_header.id == "ses-catalog-1"
    assert loaded_header.tenant_id == "acme"
    assert loaded_header.metadata == %{"workflow" => "contract"}

    assert :eq ==
             loaded_header.updated_at
             |> NaiveDateTime.truncate(:second)
             |> NaiveDateTime.compare(loaded_header.created_at)

    assert {:ok, session} = SessionCatalog.get_session("ses-catalog-1")
    assert session.id == "ses-catalog-1"
    assert session.tenant_id == "acme"
    assert session.last_seq == 12
    assert session.archived_seq == 12
  end

  test "lists sessions from the catalog without consulting the archive adapter" do
    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-a",
                 tenant_id: "acme",
                 title: "A",
                 metadata: %{marker: "x", user_id: "u1"}
               )
             )

    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-b",
                 tenant_id: "acme",
                 title: "B",
                 metadata: %{marker: "x", user_id: "u2"}
               )
             )

    assert {:ok, page} =
             SessionCatalog.list_sessions(%{
               limit: 10,
               cursor: nil,
               tenant_id: "acme",
               metadata: %{"marker" => "x", "user_id" => "u1"}
             })

    assert Enum.map(page.sessions, & &1.id) == ["ses-catalog-a"]
  end

  test "progress updates are monotonic" do
    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-progress", tenant_id: "acme", metadata: %{})
             )

    assert :ok =
             SessionCatalog.put_progress_batch([
               %{session_id: "ses-catalog-progress", archived_seq: 4}
             ])

    assert :ok =
             SessionCatalog.put_progress_batch([
               %{session_id: "ses-catalog-progress", archived_seq: 2}
             ])

    assert {:ok, 4} = SessionCatalog.get_progress("ses-catalog-progress")
  end

  test "lists sessions by creator id from indexed scalar columns" do
    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-owner-a",
                 tenant_id: "acme",
                 creator_principal: %Principal{tenant_id: "acme", id: "user-a", type: :user},
                 metadata: %{}
               )
             )

    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-owner-b",
                 tenant_id: "acme",
                 creator_principal: %Principal{tenant_id: "acme", id: "user-b", type: :user},
                 metadata: %{}
               )
             )

    assert {:ok, page} =
             SessionCatalog.list_sessions(%{
               limit: 10,
               cursor: nil,
               tenant_id: "acme",
               owner_principal_ids: ["user-a"],
               metadata: %{}
             })

    assert Enum.map(page.sessions, & &1.id) == ["ses-catalog-owner-a"]

    assert [%{creator_principal: %Principal{id: "user-a", type: :user, tenant_id: "acme"}}] =
             page.sessions
  end

  test "updates title and merges metadata without replacing unrelated keys" do
    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-update",
                 tenant_id: "acme",
                 title: "Draft",
                 metadata: %{workflow: "contract", tags: ["one"]}
               )
             )

    assert {:ok, original} = SessionCatalog.get_header("ses-catalog-update")

    assert {:ok, updated} =
             SessionCatalog.update_header("ses-catalog-update", %{
               title: "Final",
               metadata: %{"summary" => "Generated", "tags" => ["one", "two"]}
             })

    assert updated.title == "Final"

    assert updated.metadata == %{
             "workflow" => "contract",
             "summary" => "Generated",
             "tags" => ["one", "two"]
           }

    assert updated.version == original.version + 1
    assert NaiveDateTime.compare(updated.updated_at, original.updated_at) in [:eq, :gt]
  end

  test "returns expected_version conflict when compare-and-swap token is stale" do
    assert :ok =
             SessionCatalog.persist_created(
               Header.new("ses-catalog-conflict",
                 tenant_id: "acme",
                 title: "Draft",
                 metadata: %{workflow: "contract"}
               )
             )

    assert {:ok, current} = SessionCatalog.get_header("ses-catalog-conflict")
    expected_version = current.version

    assert {:ok, _updated} =
             SessionCatalog.update_header("ses-catalog-conflict", %{
               metadata: %{"summary" => "fresh"},
               expected_version: expected_version
             })

    assert {:error, {:expected_version_conflict, ^expected_version, current_version}} =
             SessionCatalog.update_header("ses-catalog-conflict", %{
               title: "Stale",
               expected_version: expected_version
             })

    assert current_version == expected_version + 1
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
end
