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
    session = %{id: "ses-1", tenant_id: "acme"}
    none = Context.none()

    assert {:ok, %Principal{tenant_id: "service", id: "service", type: :service}} =
             Policy.can_create_session(none, %{})

    assert {:ok, "ses-1"} = Policy.resolve_create_session_id(none, "ses-1")
    assert {:ok, %{limit: 10}} = Policy.authorize_session_list(none, %{limit: 10})
    assert :ok = Policy.authorize_session_access(none, "ses-1", :read)
    assert :ok = Policy.authorize_session_resource(none, session, :read)
    assert :ok = Policy.authorize_session_access(none, "ses-1", :manage)
    assert :ok = Policy.authorize_session_resource(none, session, :manage)
    assert :ok = Policy.authorize_session_access(none, "ses-1", :append)
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

  test "authorize_session_list scopes reads to the caller tenant and optional pinned session" do
    assert {:ok, %{limit: 10, tenant_id: "acme"}} =
             Policy.authorize_session_list(jwt_ctx(%{scopes: ["session:read"]}), %{limit: 10})

    assert {:ok, %{limit: 10, tenant_id: "acme", session_ids: ["ses-42"]}} =
             Policy.authorize_session_list(
               jwt_ctx(%{session_id: "ses-42", scopes: ["session:read"]}),
               %{limit: 10}
             )

    assert {:error, :forbidden_scope} =
             Policy.authorize_session_list(jwt_ctx(%{scopes: ["session:append"]}), %{limit: 10})
  end

  test "authorize_lifecycle_subscription requires a service principal with read scope" do
    assert {:ok, "acme"} =
             Policy.authorize_lifecycle_subscription(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "acme", id: "svc-backend", type: :service},
                 scopes: ["session:read"]
               })
             )

    assert {:error, :forbidden} =
             Policy.authorize_lifecycle_subscription(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "acme", id: "user-1", type: :user},
                 scopes: ["session:read"]
               })
             )

    assert {:error, :forbidden_scope} =
             Policy.authorize_lifecycle_subscription(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "acme", id: "svc-backend", type: :service},
                 scopes: ["session:append"]
               })
             )

    assert {:error, :forbidden_session} =
             Policy.authorize_lifecycle_subscription(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "acme", id: "svc-backend", type: :service},
                 scopes: ["session:read"],
                 session_id: "ses-42"
               })
             )
  end

  test "authorize_session_access enforces append scope and session lock" do
    assert :ok =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: nil, scopes: ["session:append"]}),
               "ses-1",
               :append
             )

    assert :ok =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-1", scopes: ["session:append"]}),
               "ses-1",
               :append
             )

    assert {:error, :forbidden_scope} =
             Policy.authorize_session_access(
               jwt_ctx(%{scopes: ["session:read"]}),
               "ses-1",
               :append
             )

    assert {:error, :forbidden_session} =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-1", scopes: ["session:append"]}),
               "ses-2",
               :append
             )
  end

  test "authorize_session_access enforces read access on session ids" do
    assert :ok =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-9", scopes: ["session:read"]}),
               "ses-9",
               :read
             )

    assert {:error, :forbidden_scope} =
             Policy.authorize_session_access(
               jwt_ctx(%{scopes: ["session:append"]}),
               "ses-9",
               :read
             )

    assert {:error, :forbidden_session} =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-locked", scopes: ["session:read"]}),
               "ses-9",
               :read
             )
  end

  test "authorize_session_resource enforces tenant checks on loaded sessions" do
    session = %{id: "ses-9", tenant_id: "acme"}

    assert :ok =
             Policy.authorize_session_resource(
               jwt_ctx(%{session_id: "ses-9", scopes: ["session:read"]}),
               session,
               :read
             )

    assert {:error, :forbidden_tenant} =
             Policy.authorize_session_resource(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "beta", id: "user-1", type: :user},
                 scopes: ["session:read"]
               }),
               session,
               :read
             )
  end

  test "authorize_session_access enforces manage access on session ids" do
    assert :ok =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-11", scopes: ["session:create"]}),
               "ses-11",
               :manage
             )

    assert {:error, :forbidden_scope} =
             Policy.authorize_session_access(
               jwt_ctx(%{scopes: ["session:read"]}),
               "ses-11",
               :manage
             )

    assert {:error, :forbidden_session} =
             Policy.authorize_session_access(
               jwt_ctx(%{session_id: "ses-locked", scopes: ["session:create"]}),
               "ses-11",
               :manage
             )
  end

  test "authorize_session_resource enforces manage tenant checks on loaded sessions" do
    session = %{id: "ses-11", tenant_id: "acme"}

    assert :ok =
             Policy.authorize_session_resource(
               jwt_ctx(%{session_id: "ses-11", scopes: ["session:create"]}),
               session,
               :manage
             )

    assert {:error, :forbidden_tenant} =
             Policy.authorize_session_resource(
               jwt_ctx(%{
                 principal: %Principal{tenant_id: "beta", id: "user-1", type: :user},
                 scopes: ["session:create"]
               }),
               session,
               :manage
             )
  end

  test "resolve_event_actor requires actor to match principal identity" do
    auth = jwt_ctx(%{principal: %Principal{tenant_id: "acme", id: "user-1", type: :user}})

    assert {:ok, "user:user-1"} = Policy.resolve_event_actor(auth, nil)
    assert {:ok, "user:user-1"} = Policy.resolve_event_actor(auth, "user:user-1")
    assert {:error, :invalid_event} = Policy.resolve_event_actor(auth, "user:other")
  end

  test "attach_principal_metadata includes canonical principal identity" do
    principal = %Principal{tenant_id: "acme", id: "user-1", type: :user}
    auth = jwt_ctx(%{principal: principal})

    metadata = Policy.attach_principal_metadata(auth, %{"request_id" => "r-1"})

    assert metadata["request_id"] == "r-1"
    assert metadata["starcite_principal"]["tenant_id"] == "acme"
    assert metadata["starcite_principal"]["actor"] == "user:user-1"
    assert metadata["starcite_principal"]["principal_type"] == "user"
    assert metadata["starcite_principal"]["principal_id"] == "user-1"
  end

  defp jwt_ctx(attrs) when is_map(attrs) do
    defaults = %Context{
      kind: :jwt,
      principal: %Principal{tenant_id: "acme", id: "user-1", type: :user},
      scopes: [],
      session_id: nil,
      expires_at: nil
    }

    defaults
    |> Map.from_struct()
    |> Map.merge(attrs)
    |> then(&struct!(Context, &1))
  end
end
