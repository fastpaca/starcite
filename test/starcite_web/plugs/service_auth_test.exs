defmodule StarciteWeb.Plugs.ServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.Auth.Principal
  alias Starcite.AuthTestSupport
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.JWKS
  alias StarciteWeb.Plugs.ServiceAuth

  @auth_env_key StarciteWeb.Auth
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    previous_auth = Application.get_env(:starcite, @auth_env_key)
    :ok = JWKS.clear_cache()

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

  test "authenticate_conn emits auth telemetry and caches jwks by kid" do
    attach_auth_handler()
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "acme",
        "sub" => "user:user-42",
        "scope" => "session:create session:read session:append",
        "session_id" => "ses-1"
      })

    conn =
      conn(:get, "/v1/sessions")
      |> put_req_header("authorization", "Bearer #{token}")

    assert %Plug.Conn{assigns: %{auth: %Context{kind: :jwt}}} = ServiceAuth.call(conn, [])

    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :refresh)
    assert_receive_auth_event(:jwt_verify, :jwt, :ok, :none, :none)
    assert_receive_auth_event(:plug, :jwt, :ok, :none, :none)

    assert {:ok, %Context{kind: :jwt}} = ServiceAuth.authenticate_token(token)

    assert_receive_auth_event(:jwks_fetch, :jwt, :ok, :none, :cache)
    assert_receive_auth_event(:jwt_verify, :jwt, :ok, :none, :none)
  end

  test "authenticate_conn allows requests when auth mode is none" do
    Application.put_env(:starcite, @auth_env_key, mode: :none)
    conn = conn(:get, "/")

    assert {:ok,
            %Context{
              kind: :none,
              principal: %Principal{tenant_id: "service", id: "service", type: :service}
            }} = ServiceAuth.authenticate_conn(conn)
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
    assert auth_context.session_id == "ses-1"
    assert auth_context.scopes == ["session:create", "session:read", "session:append"]
    assert auth_context.principal == %Principal{tenant_id: "acme", id: "user-42", type: :user}
    assert is_integer(auth_context.expires_at)
  end

  test "authenticate_token extracts a service principal from svc subject" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "acme",
        "sub" => "svc:customer-a",
        "scopes" => ["session:read"]
      })

    assert {:ok,
            %Context{
              principal: %Principal{tenant_id: "acme", id: "customer-a", type: :service}
            }} =
             ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token extracts a service principal from org subject" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "anor-ai",
        "sub" => "org:anor-ai",
        "scopes" => ["session:read"]
      })

    assert {:ok,
            %Context{
              principal: %Principal{tenant_id: "anor-ai", id: "anor-ai", type: :service}
            }} =
             ServiceAuth.authenticate_token(token)
  end

  test "authenticate_token rejects unsupported subject principal types" do
    {private_key, kid} = jwt_signing_fixture!()

    token =
      private_key
      |> sign_token(kid, %{
        "tenant_id" => "acme",
        "sub" => "device:edge-1",
        "scopes" => ["session:read"]
      })

    assert {:error, :invalid_jwt_claims} = ServiceAuth.authenticate_token(token)
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

    result = ServiceAuth.call(conn, [])

    assert result.status == 401
    assert result.halted

    assert get_resp_header(result, "www-authenticate") == [
             ~s(Bearer realm="starcite", error="invalid_request", error_description="Malformed bearer token")
           ]
  end

  test "plug is a no-op when auth is already assigned" do
    configure_jwt_auth!("http://localhost:1#{@jwks_path}")
    auth = Context.none()
    conn = conn(:get, "/v1/sessions") |> assign(:auth, auth)

    assert ServiceAuth.call(conn, ServiceAuth.init([])) == conn
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

  defp attach_auth_handler do
    handler_id = "auth-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :auth],
        fn _event, measurements, metadata, pid ->
          send(pid, {:auth_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp assert_receive_auth_event(stage, mode, outcome, error_reason, source)
       when stage in [:plug, :jwt_verify, :jwks_fetch] and mode in [:none, :jwt] and
              outcome in [:ok, :error] and is_atom(error_reason) and
              source in [:none, :cache, :refresh] do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline)
  end

  defp do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:auth_event, %{count: 1, duration_ms: duration_ms},
       %{
         stage: ^stage,
         mode: ^mode,
         outcome: ^outcome,
         error_reason: ^error_reason,
         source: ^source
       }} ->
        assert is_integer(duration_ms)
        assert duration_ms >= 0
        :ok

      {:auth_event, _measurements, _metadata} ->
        do_assert_receive_auth_event(stage, mode, outcome, error_reason, source, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for auth telemetry stage=#{inspect(stage)} mode=#{inspect(mode)} outcome=#{inspect(outcome)} error_reason=#{inspect(error_reason)} source=#{inspect(source)}"
        )
    end
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
