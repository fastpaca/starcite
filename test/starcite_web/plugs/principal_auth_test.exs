defmodule StarciteWeb.Plugs.PrincipalAuthTest do
  use ExUnit.Case, async: false

  import Plug.Test

  alias StarciteWeb.Plugs.{PrincipalAuth, ServiceAuth}

  @auth_env_key StarciteWeb.Auth

  setup do
    previous_auth = Application.get_env(:starcite, @auth_env_key)

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end
    end)

    :ok
  end

  test "principal auth plug is a no-op when auth is already assigned" do
    conn = conn(:get, "/") |> Plug.Conn.assign(:auth, %{kind: :service})
    result = PrincipalAuth.call(conn, [])
    assert result.assigns[:auth] == %{kind: :service}
    refute result.halted
  end

  test "authenticate_principal_conn returns none context in none mode" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)
    conn = conn(:get, "/")
    assert {:ok, %{kind: :none}} = PrincipalAuth.authenticate_principal_conn(conn)
  end

  test "authenticate_token returns none context in none mode" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)
    assert {:ok, %{kind: :none}} = PrincipalAuth.authenticate_token("anything")
  end

  test "principal auth plug assigns auth in none mode" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)

    conn = conn(:get, "/")
    result = PrincipalAuth.call(conn, [])

    assert result.assigns[:auth] == %{kind: :none}
    refute result.halted
  end

  test "authenticate_token prefers token_expired from principal verifier" do
    Application.put_env(
      :starcite,
      @auth_env_key,
      mode: :jwt,
      issuer: "https://issuer.example",
      audience: "starcite-api",
      jwks_url: "http://localhost:1/.well-known/jwks.json",
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 1_000,
      principal_token_salt: "principal-token-v1",
      principal_token_default_ttl_seconds: 600,
      principal_token_max_ttl_seconds: 900
    )

    now = System.system_time(:second)

    expired_principal_token =
      Phoenix.Token.sign(
        StarciteWeb.Endpoint,
        ServiceAuth.config().principal_token_salt,
        %{
          "v" => 1,
          "typ" => "principal",
          "tenant_id" => "acme",
          "sub" => "user-1",
          "principal_type" => "user",
          "actor" => "user:user-1",
          "scopes" => ["session:read"],
          "iat" => now - 120,
          "exp" => now - 1
        }
      )

    assert {:error, :token_expired} = PrincipalAuth.authenticate_token(expired_principal_token)
  end
end
