defmodule StarciteWeb.Plugs.ServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.Auth.Principal
  alias Starcite.AuthTestSupport
  alias StarciteWeb.Auth.JWKS
  alias StarciteWeb.Plugs.ServiceAuth

  @auth_env_key StarciteWeb.Auth
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    previous_auth = Application.get_env(:starcite, @auth_env_key)

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end

      :ok = JWKS.clear_cache()
    end)

    :ok
  end

  test "bearer_token parses a valid bearer header" do
    conn = conn(:get, "/") |> put_req_header("authorization", "Bearer abc.def")
    assert {:ok, "abc.def"} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token accepts websocket access_token query param" do
    conn =
      conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=abc.def")
      |> put_req_header("upgrade", "websocket")

    assert {:ok, "abc.def"} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token uses raw access_token preserved in conn.private by redaction plug" do
    conn =
      conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=%5BREDACTED%5D")
      |> put_req_header("upgrade", "websocket")
      |> put_private(:starcite_ws_access_token, "abc.def")

    assert {:ok, "abc.def"} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token ignores access_token query param for non-websocket requests" do
    conn = conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=abc.def")
    assert {:error, :missing_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token rejects websocket requests that provide both auth header and access_token" do
    conn =
      conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=abc.def")
      |> put_req_header("upgrade", "websocket")
      |> put_req_header("authorization", "Bearer ghi.jkl")

    assert {:error, :invalid_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token rejects blank websocket access_token query param" do
    conn =
      conn(:get, "/v1/sessions/ses-1/tail?cursor=0&access_token=   ")
      |> put_req_header("upgrade", "websocket")

    assert {:error, :invalid_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token rejects missing authorization header" do
    conn = conn(:get, "/")
    assert {:error, :missing_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token rejects multiple authorization headers" do
    conn = conn(:get, "/")

    conn = %{
      conn
      | req_headers: [
          {"authorization", "Bearer one"},
          {"authorization", "Bearer two"} | conn.req_headers
        ]
    }

    assert {:error, :invalid_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "bearer_token rejects comma-separated token values" do
    conn =
      conn(:get, "/")
      |> put_req_header("authorization", "Bearer one, Bearer two")

    assert {:error, :invalid_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "authenticate_conn rejects missing bearer token" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")
    conn = conn(:get, "/")
    assert {:error, :missing_bearer_token} = ServiceAuth.authenticate_conn(conn)
  end

  test "authenticate_conn allows requests when auth mode is none" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)
    conn = conn(:get, "/")

    assert {:ok, %{kind: :none}} = ServiceAuth.authenticate_conn(conn)
  end

  test "authenticate_token rejects non-binary token values" do
    assert {:error, :invalid_bearer_token} = ServiceAuth.authenticate_token(nil)
    assert {:error, :invalid_bearer_token} = ServiceAuth.authenticate_token("")
  end

  test "authenticate_token accepts JWT and extracts scope/session/sub claims" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "acme",
        "sub" => "user:user-42",
        "scope" => "session:create session:read session:append",
        "session_id" => "ses-1"
      })

    assert {:ok, auth_context} = ServiceAuth.authenticate_token(token)
    assert auth_context.kind == :jwt
    assert auth_context.tenant_id == "acme"
    assert auth_context.subject == "user:user-42"
    assert auth_context.session_id == "ses-1"
    assert auth_context.scopes == ["session:create", "session:read", "session:append"]
    assert auth_context.principal == %Principal{tenant_id: "acme", id: "user-42", type: :user}
    assert auth_context.bearer_token == token
    assert is_integer(auth_context.expires_at)
  end

  test "authenticate_token accepts non user/agent sub without principal struct" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "acme",
        "sub" => "svc:customer-a",
        "scopes" => ["session:read"]
      })

    assert {:ok, %{subject: "svc:customer-a", principal: nil}} =
             ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token rejects jwt missing tenant_id claim" do
    {private_key, kid} = jwt_signing_fixture!()

    claims =
      base_claims()
      |> Map.delete("tenant_id")

    token = AuthTestSupport.sign_rs256(private_key, claims, kid)

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token rejects jwt missing scope claims" do
    {private_key, kid} = jwt_signing_fixture!()

    claims =
      base_claims()
      |> Map.delete("scope")
      |> Map.delete("scopes")

    token = AuthTestSupport.sign_rs256(private_key, claims, kid)

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token rejects jwt missing sub claim" do
    {private_key, kid} = jwt_signing_fixture!()

    claims =
      base_claims()
      |> Map.delete("sub")

    token = AuthTestSupport.sign_rs256(private_key, claims, kid)

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token rejects invalid session_id claim types" do
    {private_key, kid} = jwt_signing_fixture!()
    token = sign_token(private_key, kid, %{"session_id" => 7})
    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_token(token)
  end

  test "plug halts malformed bearer tokens with invalid_request header" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")

    conn =
      conn(:get, "/")
      |> put_req_header("authorization", "Bearer one two")

    result = ServiceAuth.call(conn, %{})

    assert result.status == 401
    assert result.halted

    assert get_resp_header(result, "www-authenticate") == [
             ~s(Bearer realm="starcite", error="invalid_request", error_description="Malformed bearer token")
           ]
  end

  defp jwt_signing_fixture! do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!("http://localhost:#{bypass.port}#{@jwks_path}")

    {private_key, kid}
  end

  defp configure_jwt_auth!(jwks_url) when is_binary(jwks_url) and jwks_url != "" do
    Application.put_env(
      :starcite,
      @auth_env_key,
      issuer: @issuer,
      audience: @audience,
      jwks_url: jwks_url,
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 60_000
    )
  end

  defp sign_token(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    claims =
      base_claims()
      |> Map.merge(overrides)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end

  defp base_claims do
    %{
      "iss" => @issuer,
      "aud" => @audience,
      "sub" => "svc:customer-a",
      "tenant_id" => "acme",
      "scope" => "session:create session:read session:append",
      "exp" => System.system_time(:second) + 300
    }
  end
end
