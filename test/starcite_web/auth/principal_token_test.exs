defmodule StarciteWeb.Auth.PrincipalTokenTest do
  use ExUnit.Case, async: true

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.PrincipalToken

  @config %{
    principal_token_salt: "principal-token-v1",
    principal_token_default_ttl_seconds: 600,
    principal_token_max_ttl_seconds: 900,
    jwt_leeway_seconds: 0
  }

  test "issues and verifies principal token with normalized actor" do
    {:ok, issued} =
      PrincipalToken.issue(
        %{
          "principal" => %{"tenant_id" => "acme", "id" => "user-42", "type" => "user"},
          "scopes" => ["session:append", "session:read"],
          "session_ids" => ["ses-1"],
          "ttl_seconds" => 120
        },
        @config
      )

    assert issued.principal.tenant_id == "acme"
    assert issued.principal.id == "user-42"
    assert issued.principal.type == :user
    assert Principal.actor(issued.principal) == "user:user-42"

    assert {:ok, verified} = PrincipalToken.verify(issued.token, @config)
    assert verified.principal.tenant_id == "acme"
    assert verified.principal.id == "user-42"
    assert verified.principal.type == :user
    assert verified.scopes == ["session:append", "session:read"]
    assert verified.session_ids == ["ses-1"]
    assert Principal.actor(verified.principal) == "user:user-42"
  end

  test "rejects issue when scopes are missing or empty" do
    assert {:error, :invalid_issue_request} =
             PrincipalToken.issue(
               %{
                 "principal" => %{"tenant_id" => "acme", "id" => "agent-1", "type" => "agent"},
                 "scopes" => []
               },
               @config
             )
  end

  test "rejects issue when ttl exceeds max" do
    assert {:error, :invalid_issue_request} =
             PrincipalToken.issue(
               %{
                 "principal" => %{"tenant_id" => "acme", "id" => "agent-1", "type" => "agent"},
                 "scopes" => ["session:append"],
                 "ttl_seconds" => 901
               },
               @config
             )
  end

  test "uses default ttl when ttl_seconds is omitted" do
    {:ok, issued} =
      PrincipalToken.issue(
        %{
          "principal" => %{"tenant_id" => "acme", "id" => "user-42", "type" => "user"},
          "scopes" => ["session:read"]
        },
        @config
      )

    assert issued.expires_in == @config.principal_token_default_ttl_seconds
    assert {:ok, verified} = PrincipalToken.verify(issued.token, @config)
    assert verified.principal == %Principal{tenant_id: "acme", id: "user-42", type: :user}
  end

  test "rejects issue for agent principals without exactly one session id" do
    base_attrs = %{
      "principal" => %{"tenant_id" => "acme", "id" => "agent-1", "type" => "agent"},
      "scopes" => ["session:read"]
    }

    assert {:error, :invalid_issue_request} = PrincipalToken.issue(base_attrs, @config)

    assert {:error, :invalid_issue_request} =
             PrincipalToken.issue(Map.put(base_attrs, "session_ids", []), @config)

    assert {:error, :invalid_issue_request} =
             PrincipalToken.issue(Map.put(base_attrs, "session_ids", ["ses-1", "ses-2"]), @config)

    assert {:ok, issued} =
             PrincipalToken.issue(Map.put(base_attrs, "session_ids", ["ses-1"]), @config)

    assert issued.session_ids == ["ses-1"]
  end

  test "rejects verify when agent token has invalid session binding claims" do
    now = System.system_time(:second)

    missing_session_ids_token =
      Phoenix.Token.sign(
        StarciteWeb.Endpoint,
        @config.principal_token_salt,
        %{
          "v" => 1,
          "typ" => "principal",
          "tenant_id" => "acme",
          "sub" => "agent-1",
          "principal_type" => "agent",
          "actor" => "agent:agent-1",
          "scopes" => ["session:read"],
          "iat" => now,
          "exp" => now + 60
        }
      )

    assert {:error, :invalid_bearer_token} =
             PrincipalToken.verify(missing_session_ids_token, @config)

    multiple_session_ids_token =
      Phoenix.Token.sign(
        StarciteWeb.Endpoint,
        @config.principal_token_salt,
        %{
          "v" => 1,
          "typ" => "principal",
          "tenant_id" => "acme",
          "sub" => "agent-1",
          "principal_type" => "agent",
          "actor" => "agent:agent-1",
          "scopes" => ["session:read"],
          "session_ids" => ["ses-1", "ses-2"],
          "iat" => now,
          "exp" => now + 60
        }
      )

    assert {:error, :invalid_bearer_token} =
             PrincipalToken.verify(multiple_session_ids_token, @config)
  end

  test "rejects verify when actor does not match principal identity" do
    now = System.system_time(:second)

    token =
      Phoenix.Token.sign(
        StarciteWeb.Endpoint,
        @config.principal_token_salt,
        %{
          "v" => 1,
          "typ" => "principal",
          "tenant_id" => "acme",
          "sub" => "user-42",
          "principal_type" => "user",
          "actor" => "user:other",
          "scopes" => ["session:read"],
          "iat" => now,
          "exp" => now + 60
        }
      )

    assert {:error, :invalid_bearer_token} = PrincipalToken.verify(token, @config)
  end

  test "rejects verify when claim exp is in the past" do
    now = System.system_time(:second)

    token =
      Phoenix.Token.sign(
        StarciteWeb.Endpoint,
        @config.principal_token_salt,
        %{
          "v" => 1,
          "typ" => "principal",
          "tenant_id" => "acme",
          "sub" => "agent-1",
          "principal_type" => "agent",
          "actor" => "agent:agent-1",
          "scopes" => ["session:read"],
          "iat" => now - 120,
          "exp" => now - 1
        }
      )

    assert {:error, :token_expired} = PrincipalToken.verify(token, @config)
  end
end
