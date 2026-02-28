defmodule StarciteWeb.Auth.PolicyTest do
  use ExUnit.Case, async: true

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy

  test "can_create_session requires session:create scope" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    assert {:ok, ^principal} =
             Policy.can_create_session(
               jwt_ctx(%{
                 scopes: ["session:create"],
                 principal: principal
               }),
               %{}
             )

    assert {:error, :forbidden_scope} =
             Policy.can_create_session(jwt_ctx(%{scopes: ["session:read"]}), %{})
  end

  test "none mode allows create/list/read/append without jwt claims" do
    session = %{id: "ses-1", metadata: %{"tenant_id" => "acme"}, creator_principal: nil}
    none = Context.none()

    assert {:ok, nil} = Policy.can_create_session(none, %{})
    assert {:ok, "ses-1"} = Policy.resolve_create_session_id(none, "ses-1")
    assert {:ok, :all} = Policy.can_list_sessions(none)
    assert :ok = Policy.allowed_to_access_session(none, "ses-1")
    assert :ok = Policy.allowed_to_read_session(none, session)
    assert :ok = Policy.allowed_to_append_session(none, session)
  end

  test "resolve_create_session_id enforces optional session_id lock" do
    assert {:ok, nil} = Policy.resolve_create_session_id(jwt_ctx(%{session_id: nil}), nil)

    assert {:ok, "ses-1"} =
             Policy.resolve_create_session_id(jwt_ctx(%{session_id: "ses-1"}), nil)

    assert {:ok, "ses-1"} =
             Policy.resolve_create_session_id(jwt_ctx(%{session_id: "ses-1"}), "ses-1")

    assert {:error, :forbidden_session} =
             Policy.resolve_create_session_id(jwt_ctx(%{session_id: "ses-1"}), "ses-2")
  end

  test "can_list_sessions returns tenant and optional session filter" do
    assert {:ok, %{tenant_id: "acme", session_id: nil}} =
             Policy.can_list_sessions(jwt_ctx(%{scopes: ["session:read"]}))

    assert {:ok, %{tenant_id: "acme", session_id: "ses-42"}} =
             Policy.can_list_sessions(
               jwt_ctx(%{
                 session_id: "ses-42",
                 scopes: ["session:read"]
               })
             )

    assert {:error, :forbidden_scope} =
             Policy.can_list_sessions(jwt_ctx(%{scopes: ["session:append"]}))
  end

  test "allowed_to_access_session enforces session_id claim lock" do
    assert :ok = Policy.allowed_to_access_session(jwt_ctx(%{session_id: nil}), "ses-1")
    assert :ok = Policy.allowed_to_access_session(jwt_ctx(%{session_id: "ses-1"}), "ses-1")

    assert {:error, :forbidden_session} =
             Policy.allowed_to_access_session(jwt_ctx(%{session_id: "ses-1"}), "ses-2")
  end

  test "allowed_to_append_session enforces scope, tenant, and session lock" do
    session = %{id: "ses-1", metadata: %{"tenant_id" => "acme"}, creator_principal: nil}

    assert :ok =
             Policy.allowed_to_append_session(
               jwt_ctx(%{session_id: nil, scopes: ["session:append"]}),
               session
             )

    assert {:error, :forbidden_scope} =
             Policy.allowed_to_append_session(
               jwt_ctx(%{session_id: nil, scopes: ["session:read"]}),
               session
             )

    assert {:error, :forbidden_tenant} =
             Policy.allowed_to_append_session(
               jwt_ctx(%{tenant_id: "beta", session_id: nil, scopes: ["session:append"]}),
               session
             )

    assert {:error, :forbidden_session} =
             Policy.allowed_to_append_session(
               jwt_ctx(%{session_id: "ses-2", scopes: ["session:append"]}),
               session
             )
  end

  test "allowed_to_read_session enforces scope, tenant, and session lock" do
    session = %{id: "ses-9", metadata: %{"tenant_id" => "acme"}, creator_principal: nil}

    assert :ok =
             Policy.allowed_to_read_session(
               jwt_ctx(%{session_id: "ses-9", scopes: ["session:read"]}),
               session
             )

    assert {:error, :forbidden_scope} =
             Policy.allowed_to_read_session(
               jwt_ctx(%{session_id: "ses-9", scopes: ["session:append"]}),
               session
             )
  end

  test "resolve_event_actor requires actor to match JWT sub" do
    auth = jwt_ctx(%{subject: "user:user-1"})

    assert {:ok, "user:user-1"} = Policy.resolve_event_actor(auth, nil)
    assert {:ok, "user:user-1"} = Policy.resolve_event_actor(auth, "user:user-1")
    assert {:error, :invalid_event} = Policy.resolve_event_actor(auth, "user:other")
  end

  test "attach_principal_metadata includes tenant and subject" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}

    auth =
      jwt_ctx(%{
        tenant_id: "acme",
        subject: "user:user-1",
        principal: principal
      })

    metadata = Policy.attach_principal_metadata(auth, %{"request_id" => "r-1"})

    assert metadata["request_id"] == "r-1"
    assert metadata["starcite_principal"]["tenant_id"] == "acme"
    assert metadata["starcite_principal"]["subject"] == "user:user-1"
    assert metadata["starcite_principal"]["actor"] == "user:user-1"
    assert metadata["starcite_principal"]["principal_type"] == "user"
    assert metadata["starcite_principal"]["principal_id"] == "user-1"
  end

  defp jwt_ctx(attrs) when is_map(attrs) do
    defaults = %Context{
      kind: :jwt,
      tenant_id: "acme",
      subject: "user:user-1",
      scopes: [],
      session_id: nil,
      principal: nil
    }

    defaults
    |> Map.from_struct()
    |> Map.merge(attrs)
    |> then(&struct!(Context, &1))
  end
end
