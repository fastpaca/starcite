defmodule StarciteWeb.SessionDiscoverySocketTest do
  use ExUnit.Case, async: true

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.SessionDiscoverySocket

  test "pushes discovery updates for jwt tenant subscribers" do
    auth = jwt_auth("acme", nil)
    {:ok, state} = SessionDiscoverySocket.init(%{auth_context: auth})

    assert {:push, {:text, payload}, ^state} =
             SessionDiscoverySocket.handle_info(
               {:session_discovery, update("ses-1", "acme")},
               state
             )

    frame = Jason.decode!(payload)
    assert frame["version"] == 1
    assert frame["kind"] == "session_created"
    assert frame["state"] == "active"
    assert frame["session_id"] == "ses-1"
    assert frame["tenant_id"] == "acme"
    assert frame["occurred_at"] == "2026-03-02T00:00:00Z"
  end

  test "drops discovery updates from other tenants" do
    auth = jwt_auth("acme", nil)
    {:ok, state} = SessionDiscoverySocket.init(%{auth_context: auth})

    assert {:ok, ^state} =
             SessionDiscoverySocket.handle_info(
               {:session_discovery, update("ses-1", "beta")},
               state
             )
  end

  test "enforces jwt session_id lock for discovery updates" do
    auth = jwt_auth("acme", "ses-allowed")
    {:ok, state} = SessionDiscoverySocket.init(%{auth_context: auth})

    assert {:ok, ^state} =
             SessionDiscoverySocket.handle_info(
               {:session_discovery, update("ses-other", "acme")},
               state
             )

    assert {:push, {:text, payload}, ^state} =
             SessionDiscoverySocket.handle_info(
               {:session_discovery, update("ses-allowed", "acme")},
               state
             )

    frame = Jason.decode!(payload)
    assert frame["session_id"] == "ses-allowed"
  end

  test "allows all discovery updates in auth none mode" do
    {:ok, state} = SessionDiscoverySocket.init(%{auth_context: Context.none()})

    assert {:push, {:text, payload}, ^state} =
             SessionDiscoverySocket.handle_info(
               {:session_discovery, update("ses-1", "acme")},
               state
             )

    frame = Jason.decode!(payload)
    assert frame["tenant_id"] == "acme"
  end

  test "closes socket on auth expiry message" do
    {:ok, state} = SessionDiscoverySocket.init(%{auth_context: Context.none()})

    assert {:stop, :token_expired, {4001, "token_expired"}, ^state} =
             SessionDiscoverySocket.handle_info(:auth_expired, state)
  end

  defp update(session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    %{
      version: 1,
      kind: :session_created,
      state: :active,
      session_id: session_id,
      tenant_id: tenant_id,
      occurred_at: "2026-03-02T00:00:00Z"
    }
  end

  defp jwt_auth(tenant_id, session_id)
       when is_binary(tenant_id) and tenant_id != "" and
              (is_binary(session_id) or is_nil(session_id)) do
    %Context{
      kind: :jwt,
      principal: %Principal{tenant_id: tenant_id, id: "user-1", type: :user},
      scopes: ["session:read"],
      session_id: session_id
    }
  end
end
