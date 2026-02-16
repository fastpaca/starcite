defmodule StarciteWeb.Auth.PolicyTest do
  use ExUnit.Case, async: true

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Policy

  test "authorize_issue_token allows service and denies principal" do
    assert :ok = Policy.authorize_issue_token(%{kind: :service})
    assert :ok = Policy.authorize_issue_token(%{kind: :none})
    assert {:error, :forbidden} = Policy.authorize_issue_token(%{kind: :principal})
  end

  test "authorize_create_session allows service with explicit creator principal" do
    assert {:ok, %Principal{tenant_id: "acme", id: "user-1", type: :user}} =
             Policy.authorize_create_session(
               %{kind: :service},
               %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "authorize_create_session denies service without creator principal" do
    assert {:error, :invalid_session} =
             Policy.authorize_create_session(%{kind: :service}, %{"id" => "ses-1"})
  end

  test "authorize_create_session allows scoped principal user with implicit self principal" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:ok, ^principal} =
             Policy.authorize_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:create"]},
               %{"title" => "t"}
             )
  end

  test "authorize_create_session denies principal user without create scope" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:error, :forbidden_scope} =
             Policy.authorize_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{"title" => "t"}
             )
  end

  test "authorize_create_session denies principal user creator override" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:error, :invalid_session} =
             Policy.authorize_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:create"]},
               %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "authorize_create_session denies principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert {:error, :forbidden} =
             Policy.authorize_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:create"]},
               %{}
             )
  end

  test "authorize_list_sessions returns tenant-fenced owner scope for principal user" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, %{tenant_id: "acme", owner_principal_ids: ["user-1", "user-2"]}} =
             Policy.authorize_list_sessions(%{
               kind: :principal,
               principal: principal,
               scopes: ["session:read"],
               owner_principal_ids: ["user-2", "user-1"]
             })
  end

  test "authorize_list_sessions defaults owner scope to the authenticated user" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, %{tenant_id: "acme", owner_principal_ids: ["user-1"]}} =
             Policy.authorize_list_sessions(%{
               kind: :principal,
               principal: principal,
               scopes: ["session:read"]
             })
  end

  test "authorize_list_sessions denies principal user without read scope" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:error, :forbidden_scope} =
             Policy.authorize_list_sessions(%{
               kind: :principal,
               principal: principal,
               scopes: ["session:append"]
             })
  end

  test "authorize_list_sessions denies principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert {:error, :forbidden} =
             Policy.authorize_list_sessions(%{kind: :principal, principal: principal})
  end

  test "authorize_session_reference enforces session bounds for principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert :ok =
             Policy.authorize_session_reference(
               %{kind: :principal, principal: principal, session_ids: ["ses-1"]},
               "ses-1"
             )

    assert {:error, :forbidden_session} =
             Policy.authorize_session_reference(
               %{kind: :principal, principal: principal, session_ids: ["ses-1"]},
               "ses-2"
             )
  end

  test "authorize_append allows principal user on own-created session without explicit owner bindings" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert :ok =
             Policy.authorize_append(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{
                 id: "ses-1",
                 creator_principal: %{
                   "tenant_id" => "acme",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "authorize_append allows principal user with explicit session grant" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert :ok =
             Policy.authorize_append(
               %{
                 kind: :principal,
                 principal: principal,
                 scopes: ["session:append"],
                 session_ids: ["ses-2"]
               },
               %{
                 id: "ses-2",
                 creator_principal: %{
                   "tenant_id" => "acme",
                   "id" => "user-9",
                   "type" => "user"
                 }
               }
             )
  end

  test "authorize_append denies tenant mismatch for principal user" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:error, :forbidden_tenant} =
             Policy.authorize_append(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{
                 id: "ses-1",
                 creator_principal: %{
                   "tenant_id" => "beta",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "authorize_append allows owner-principal policy grant" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert :ok =
             Policy.authorize_append(
               %{
                 kind: :principal,
                 principal: principal,
                 scopes: ["session:append"],
                 owner_principal_ids: ["user-9"]
               },
               %{
                 id: "ses-1",
                 creator_principal: %{
                   "tenant_id" => "acme",
                   "id" => "user-9",
                   "type" => "user"
                 }
               }
             )
  end

  test "resolve_actor enforces principal actor identity" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, "user:user-1"} =
             Policy.resolve_actor(%{kind: :principal, principal: principal}, nil)

    assert {:ok, "user:user-1"} =
             Policy.resolve_actor(%{kind: :principal, principal: principal}, "user:user-1")

    assert {:error, :invalid_event} =
             Policy.resolve_actor(%{kind: :principal, principal: principal}, "user:other")
  end

  test "stamp_event_metadata attaches principal metadata" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    metadata =
      Policy.stamp_event_metadata(%{kind: :principal, principal: principal}, %{
        "request_id" => "r-1"
      })

    assert metadata["request_id"] == "r-1"
    assert metadata["starcite_principal"]["tenant_id"] == "acme"
    assert metadata["starcite_principal"]["principal_id"] == "user-1"
    assert metadata["starcite_principal"]["principal_type"] == "user"
  end
end
