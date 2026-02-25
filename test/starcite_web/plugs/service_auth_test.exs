defmodule StarciteWeb.Plugs.ServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

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
    conn = conn(:get, "/") |> Plug.Conn.put_req_header("authorization", "Bearer abc.def")
    assert {:ok, "abc.def"} = ServiceAuth.bearer_token(conn)
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
      |> Plug.Conn.put_req_header("authorization", "Bearer one, Bearer two")

    assert {:error, :invalid_bearer_token} = ServiceAuth.bearer_token(conn)
  end

  test "authenticate_service_conn rejects missing bearer token in jwt mode" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")
    conn = conn(:get, "/")
    assert {:error, :missing_bearer_token} = ServiceAuth.authenticate_service_conn(conn)
  end

  test "authenticate_service_token rejects non-binary token values" do
    assert {:error, :invalid_bearer_token} = ServiceAuth.authenticate_service_token(nil)
    assert {:error, :invalid_bearer_token} = ServiceAuth.authenticate_service_token("")
  end

  test "authenticate_service_token accepts tenant-scoped jwt with scope claim" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "tenant_id" => "acme",
        "scope" => "session:create session:read auth:issue"
      })

    assert {:ok, auth_context} = ServiceAuth.authenticate_service_token(token)
    assert auth_context.kind == :service
    assert auth_context.tenant_id == "acme"
    assert auth_context.scopes == ["session:create", "session:read", "auth:issue"]
    assert auth_context.bearer_token == token
    assert is_integer(auth_context.expires_at)
  end

  test "authenticate_service_token accepts scopes array claim" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "scope" => nil,
        "scopes" => ["session:create", "session:read", "auth:issue"]
      })

    assert {:ok, %{scopes: ["session:create", "session:read", "auth:issue"]}} =
             ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_token merges scope and scopes claims with stable order" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "scope" => "session:create session:read",
        "scopes" => ["session:read", "auth:issue", "session:create"]
      })

    assert {:ok, %{scopes: scopes}} = ServiceAuth.authenticate_service_token(token)
    assert scopes == ["session:create", "session:read", "auth:issue"]
  end

  test "authenticate_service_token rejects jwt missing tenant_id claim" do
    {private_key, kid} = jwt_signing_fixture!()

    claims =
      base_claims()
      |> Map.delete("tenant_id")

    token = AuthTestSupport.sign_rs256(private_key, claims, kid)

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_token rejects jwt missing scope claims" do
    {private_key, kid} = jwt_signing_fixture!()

    claims =
      base_claims()
      |> Map.delete("scope")

    token = AuthTestSupport.sign_rs256(private_key, claims, kid)

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_token rejects empty scope values" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "scope" => "   ",
        "scopes" => []
      })

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_token rejects non-string scopes entries" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "scope" => nil,
        "scopes" => ["session:create", 7]
      })

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_token rejects non-integer exp values" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_service_token(kid, %{
        "exp" => Integer.to_string(System.system_time(:second) + 120)
      })

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_service_token(token)
  end

  test "authenticate_service_conn returns none context in none mode" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)

    conn = conn(:get, "/")
    assert {:ok, %{kind: :none}} = ServiceAuth.authenticate_service_conn(conn)
  end

  test "service auth plug assigns auth in none mode" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)

    conn = conn(:get, "/")
    result = ServiceAuth.call(conn, %{required: true})

    assert result.assigns[:auth] == %{kind: :none}
    refute result.halted
  end

  test "service auth plug in optional mode preserves request and exposes auth error" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")

    conn =
      conn(:get, "/")
      |> put_req_header("authorization", "Bearer one two")

    result = ServiceAuth.call(conn, %{required: false})

    assert result.assigns[:service_auth_error] == :invalid_bearer_token
    refute Map.has_key?(result.assigns, :auth)
    refute result.halted
  end

  test "service auth plug in required mode halts malformed bearer tokens with invalid_request" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")

    conn =
      conn(:get, "/")
      |> put_req_header("authorization", "Bearer one two")

    result = ServiceAuth.call(conn, %{required: true})

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
      mode: :jwt,
      issuer: @issuer,
      audience: @audience,
      jwks_url: jwks_url,
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 60_000,
      principal_token_salt: "principal-token-v1",
      principal_token_default_ttl_seconds: 5,
      principal_token_max_ttl_seconds: 15
    )
  end

  defp sign_service_token(private_key, kid, overrides)
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
      "scope" => "session:create session:read auth:issue",
      "exp" => System.system_time(:second) + 300
    }
  end
end
