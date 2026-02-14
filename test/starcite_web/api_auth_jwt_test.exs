defmodule StarciteWeb.ApiAuthJwtTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias Starcite.Runtime
  alias StarciteWeb.Auth.JWKS

  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    Starcite.Runtime.TestHelper.reset()
    previous_auth = Application.get_env(:starcite, StarciteWeb.Auth)

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, StarciteWeb.Auth)
      else
        Application.put_env(:starcite, StarciteWeb.Auth, previous_auth)
      end

      :ok = JWKS.clear_cache()
    end)

    :ok
  end

  test "returns 401 when token is missing in jwt mode" do
    bypass = Bypass.open()
    configure_jwt_auth!(bypass)

    conn = json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")})

    assert conn.status == 401
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "unauthorized"
    assert get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="starcite")]
  end

  test "accepts valid JWT for session create and append" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-valid"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    auth_header = {"authorization", "Bearer #{token}"}
    session_id = unique_id("ses")

    create_conn = json_conn(:post, "/v1/sessions", %{"id" => session_id}, [auth_header])
    assert create_conn.status == 201

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "ok"},
          "actor" => "agent:test",
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [auth_header]
      )

    assert append_conn.status == 201
  end

  test "returns 401 for invalid JWT audience" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-bad-aud"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    token =
      AuthTestSupport.sign_rs256(
        private_key,
        %{
          "iss" => @issuer,
          "aud" => "not-starcite-api",
          "sub" => "svc:customer-a",
          "exp" => System.system_time(:second) + 300
        },
        kid
      )

    conn =
      json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")}, [
        {"authorization", "Bearer #{token}"}
      ])

    assert conn.status == 401
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "unauthorized"

    assert get_resp_header(conn, "www-authenticate") == [
             ~s(Bearer realm="starcite", error="invalid_token")
           ]
  end

  test "returns 401 when JWKS is structurally invalid in jwt mode" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-invalid-jwks"

    invalid_jwks =
      AuthTestSupport.jwks_for_private_key(private_key, kid)
      |> Map.update!("keys", fn [key] -> [Map.delete(key, "use")] end)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(invalid_jwks))
    end)

    configure_jwt_auth!(bypass)

    token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    conn =
      json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")}, [
        {"authorization", "Bearer #{token}"}
      ])

    assert conn.status == 401
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "unauthorized"

    assert get_resp_header(conn, "www-authenticate") == [
             ~s(Bearer realm="starcite", error="invalid_token")
           ]
  end

  test "protects tail websocket upgrade with JWT auth" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-tail"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    session_id = unique_id("ses")
    {:ok, _} = Runtime.create_session(id: session_id)

    without_auth =
      conn_get("/v1/sessions/#{session_id}/tail?cursor=0", websocket_headers())

    assert without_auth.status == 401
    assert get_resp_header(without_auth, "www-authenticate") == [~s(Bearer realm="starcite")]

    token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    with_auth =
      conn_get("/v1/sessions/#{session_id}/tail?cursor=bad", [
        {"authorization", "Bearer #{token}"}
        | websocket_headers()
      ])

    assert with_auth.status == 400
    body = Jason.decode!(with_auth.resp_body)
    assert body["error"] == "invalid_cursor"
  end

  defp configure_jwt_auth!(bypass) do
    Application.put_env(
      :starcite,
      StarciteWeb.Auth,
      mode: :jwt,
      issuer: @issuer,
      audience: @audience,
      jwks_url: "http://localhost:#{bypass.port}#{@jwks_path}",
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 1_000
    )
  end

  defp valid_claims(opts) when is_list(opts) do
    %{
      "iss" => @issuer,
      "aud" => @audience,
      "sub" => Keyword.fetch!(opts, :sub),
      "exp" => System.system_time(:second) + 300
    }
  end

  defp websocket_headers do
    [
      {"connection", "upgrade"},
      {"upgrade", "websocket"},
      {"sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="},
      {"sec-websocket-version", "13"}
    ]
  end

  defp conn_get(path, headers) do
    conn =
      conn(:get, path)
      |> put_headers(headers)

    @endpoint.call(conn, @endpoint.init([]))
  end

  defp json_conn(method, path, body, headers \\ []) do
    conn =
      conn(method, path)
      |> put_req_header("content-type", "application/json")
      |> put_headers(headers)

    conn =
      if body do
        %{conn | body_params: body, params: body}
      else
        conn
      end

    @endpoint.call(conn, @endpoint.init([]))
  end

  defp put_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      put_req_header(acc, name, value)
    end)
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end
end
