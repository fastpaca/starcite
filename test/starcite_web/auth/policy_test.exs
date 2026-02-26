defmodule StarciteWeb.Auth.PolicyTest do
  use ExUnit.Case, async: true

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Policy

  test "can_issue_token allows tenant-scoped service, denies none mode, and denies principal" do
    params = %{"principal" => %{"tenant_id" => "acme", "id" => "user-1", "type" => "user"}}

    assert :ok =
             Policy.can_issue_token(
               %{kind: :service, tenant_id: "acme", scopes: ["auth:issue"]},
               params
             )

    assert {:error, :forbidden} = Policy.can_issue_token(%{kind: :none}, params)
    assert {:error, :forbidden} = Policy.can_issue_token(%{kind: :principal}, params)
  end

  test "can_issue_token denies cross-tenant issue requests" do
    params = %{"principal" => %{"tenant_id" => "beta", "id" => "user-1", "type" => "user"}}

    assert {:error, :forbidden_tenant} =
             Policy.can_issue_token(
               %{kind: :service, tenant_id: "acme", scopes: ["auth:issue"]},
               params
             )
  end

  test "can_issue_token enforces auth:issue scope when service scopes are present" do
    params = %{"principal" => %{"tenant_id" => "acme", "id" => "user-1", "type" => "user"}}

    assert {:error, :forbidden_scope} =
             Policy.can_issue_token(
               %{kind: :service, tenant_id: "acme", scopes: ["session:read"]},
               params
             )
  end

  test "can_issue_token denies malformed service issue payloads" do
    assert {:error, :forbidden_tenant} =
             Policy.can_issue_token(
               %{kind: :service, tenant_id: "acme", scopes: ["auth:issue"]},
               %{"principal" => %{"id" => "user-1", "type" => "user"}}
             )

    assert {:error, :invalid_issue_request} =
             Policy.can_issue_token(
               %{kind: :service, tenant_id: "acme", scopes: ["auth:issue"]},
               %{"scopes" => ["session:read"]}
             )
  end

  test "can_create_session allows service with explicit same-tenant creator principal" do
    assert {:ok, %Principal{tenant_id: "acme", id: "user-1", type: :user}} =
             Policy.can_create_session(
               %{kind: :service, tenant_id: "acme", scopes: ["session:create"]},
               %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "can_create_session denies service without creator principal" do
    assert {:error, :invalid_session} =
             Policy.can_create_session(%{kind: :service, tenant_id: "acme"}, %{"id" => "ses-1"})
  end

  test "can_create_session denies service creator principal tenant mismatch" do
    assert {:error, :forbidden_tenant} =
             Policy.can_create_session(
               %{kind: :service, tenant_id: "acme", scopes: ["session:create"]},
               %{
                 "creator_principal" => %{
                   "tenant_id" => "beta",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "can_create_session enforces session:create scope when service scopes are present" do
    assert {:error, :forbidden_scope} =
             Policy.can_create_session(
               %{kind: :service, tenant_id: "acme", scopes: ["session:read"]},
               %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-1",
                   "type" => "user"
                 }
               }
             )
  end

  test "can_create_session allows scoped principal user with implicit self principal" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:ok, ^principal} =
             Policy.can_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:create"]},
               %{"title" => "t"}
             )
  end

  test "can_create_session denies principal user without create scope" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:error, :forbidden_scope} =
             Policy.can_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{"title" => "t"}
             )
  end

  test "can_create_session denies principal user creator override" do
    principal = %Principal{tenant_id: "acme", id: "user-9", type: :user}

    assert {:error, :invalid_session} =
             Policy.can_create_session(
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

  test "can_create_session denies principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert {:error, :forbidden} =
             Policy.can_create_session(
               %{kind: :principal, principal: principal, scopes: ["session:create"]},
               %{}
             )
  end

  test "can_list_sessions returns tenant-fenced self scope for principal user" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, %{tenant_id: "acme", owner_principal_ids: ["user-1"]}} =
             Policy.can_list_sessions(%{
               kind: :principal,
               principal: principal,
               scopes: ["session:read"]
             })
  end

  test "can_list_sessions returns tenant-fenced scope for service token" do
    assert {:ok, %{tenant_id: "acme", owner_principal_ids: nil}} =
             Policy.can_list_sessions(%{
               kind: :service,
               tenant_id: "acme",
               scopes: ["session:read"]
             })
  end

  test "can_list_sessions enforces session:read scope when service scopes are present" do
    assert {:error, :forbidden_scope} =
             Policy.can_list_sessions(%{
               kind: :service,
               tenant_id: "acme",
               scopes: ["session:create"]
             })
  end

  test "can_list_sessions denies malformed service auth contexts" do
    assert {:error, :forbidden} =
             Policy.can_list_sessions(%{
               kind: :service,
               scopes: ["session:read"]
             })
  end

  test "can_list_sessions denies principal user without read scope" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:error, :forbidden_scope} =
             Policy.can_list_sessions(%{
               kind: :principal,
               principal: principal,
               scopes: ["session:append"]
             })
  end

  test "can_list_sessions denies principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert {:error, :forbidden} =
             Policy.can_list_sessions(%{kind: :principal, principal: principal})
  end

  test "allowed_to_access_session enforces session bounds for principal agent" do
    principal = %Principal{tenant_id: "acme", id: "agent-1", type: :agent}

    assert :ok =
             Policy.allowed_to_access_session(
               %{kind: :principal, principal: principal, session_ids: ["ses-1"]},
               "ses-1"
             )

    assert {:error, :forbidden_session} =
             Policy.allowed_to_access_session(
               %{kind: :principal, principal: principal, session_ids: ["ses-1"]},
               "ses-2"
             )
  end

  test "allowed_to_access_session denies service token access to session-scoped endpoints" do
    assert {:error, :forbidden_session} =
             Policy.allowed_to_access_session(%{kind: :service, tenant_id: "acme"}, "ses-1")
  end

  test "allowed_to_append_session allows principal user on own-created session" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert :ok =
             Policy.allowed_to_append_session(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{
                 id: "ses-1",
                 creator_principal: %Principal{tenant_id: "acme", id: "user-1", type: :user}
               }
             )
  end

  test "allowed_to_append_session allows principal user with explicit session grant" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert :ok =
             Policy.allowed_to_append_session(
               %{
                 kind: :principal,
                 principal: principal,
                 scopes: ["session:append"],
                 session_ids: ["ses-2"]
               },
               %{
                 id: "ses-2",
                 creator_principal: %Principal{tenant_id: "acme", id: "user-9", type: :user}
               }
             )
  end

  test "allowed_to_append_session denies tenant mismatch for principal user" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:error, :forbidden_tenant} =
             Policy.allowed_to_append_session(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               %{
                 id: "ses-1",
                 creator_principal: %Principal{tenant_id: "beta", id: "user-1", type: :user}
               }
             )
  end

  test "allowed_to_read_session enforces read scope and principal session grants" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    session = %{
      id: "ses-2",
      creator_principal: %Principal{tenant_id: "acme", id: "user-9", type: :user}
    }

    assert {:error, :forbidden_scope} =
             Policy.allowed_to_read_session(
               %{kind: :principal, principal: principal, scopes: ["session:append"]},
               session
             )

    assert {:error, :forbidden_session} =
             Policy.allowed_to_read_session(
               %{kind: :principal, principal: principal, scopes: ["session:read"]},
               session
             )

    assert :ok =
             Policy.allowed_to_read_session(
               %{
                 kind: :principal,
                 principal: principal,
                 scopes: ["session:read"],
                 session_ids: ["ses-2"]
               },
               session
             )
  end

  test "resolve_event_actor enforces principal actor identity" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, "user:user-1"} =
             Policy.resolve_event_actor(%{kind: :principal, principal: principal}, nil)

    assert {:ok, "user:user-1"} =
             Policy.resolve_event_actor(%{kind: :principal, principal: principal}, "user:user-1")

    assert {:error, :invalid_event} =
             Policy.resolve_event_actor(%{kind: :principal, principal: principal}, "user:other")
  end

  test "attach_principal_metadata attaches principal metadata" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    metadata =
      Policy.attach_principal_metadata(%{kind: :principal, principal: principal}, %{
        "request_id" => "r-1"
      })

    assert metadata["request_id"] == "r-1"
    assert metadata["starcite_principal"]["tenant_id"] == "acme"
    assert metadata["starcite_principal"]["principal_id"] == "user-1"
    assert metadata["starcite_principal"]["principal_type"] == "user"
  end
end
