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

    create_conn =
      json_conn(:post, "/v1/sessions", service_create_session_body(session_id), [auth_header])

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

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
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

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
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

  test "issues a principal token from service auth and infers actor on append" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-issue"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    service_header = {"authorization", "Bearer #{service_token}"}
    session_id = unique_id("ses")

    create_conn =
      json_conn(
        :post,
        "/v1/sessions",
        service_create_session_body(session_id),
        [service_header]
      )

    assert create_conn.status == 201

    issue_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-42",
            "type" => "user"
          },
          "scopes" => ["session:append", "session:read"],
          "session_ids" => [session_id],
          "ttl_seconds" => 120
        },
        [service_header]
      )

    assert issue_conn.status == 200
    issue_body = Jason.decode!(issue_conn.resp_body)
    principal_token = issue_body["token"]
    assert is_binary(principal_token) and principal_token != ""

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "hi"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert append_conn.status == 201

    {:ok, [event]} = Runtime.get_events_from_cursor(session_id, 0, 10)
    assert event.actor == "user:user-42"
    assert event.metadata["starcite_principal"]["tenant_id"] == "acme"
    assert event.metadata["starcite_principal"]["principal_type"] == "user"
    assert event.metadata["starcite_principal"]["principal_id"] == "user-42"
  end

  test "enforces principal session constraints on append" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-session-bounds"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    service_header = {"authorization", "Bearer #{service_token}"}
    allowed_session_id = unique_id("ses")
    blocked_session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(allowed_session_id),
               [service_header]
             ).status

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(blocked_session_id),
               [service_header]
             ).status

    issue_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "agent-99",
            "type" => "agent"
          },
          "scopes" => ["session:append"],
          "session_ids" => [allowed_session_id]
        },
        [service_header]
      )

    assert issue_conn.status == 200
    principal_token = Jason.decode!(issue_conn.resp_body)["token"]

    blocked_append =
      json_conn(
        :post,
        "/v1/sessions/#{blocked_session_id}/append",
        %{
          "type" => "state",
          "payload" => %{"state" => "x"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert blocked_append.status == 403
    body = Jason.decode!(blocked_append.resp_body)
    assert body["error"] == "forbidden_session"
  end

  test "requires service auth for token issuance endpoint" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-issue-service-only"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect_once(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    service_header = {"authorization", "Bearer #{service_token}"}

    issue_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-42",
            "type" => "user"
          },
          "scopes" => ["session:read"]
        },
        [service_header]
      )

    assert issue_conn.status == 200
    principal_token = Jason.decode!(issue_conn.resp_body)["token"]

    nested_issue_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-43",
            "type" => "user"
          },
          "scopes" => ["session:read"]
        },
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert nested_issue_conn.status == 401
    body = Jason.decode!(nested_issue_conn.resp_body)
    assert body["error"] == "unauthorized"
  end

  test "denies session create and list for agent principal tokens" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-principal-deny-create-list"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)
    session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(session_id),
               [service_header]
             ).status

    principal_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "agent-7",
            "type" => "agent"
          },
          "scopes" => ["session:read", "session:append"],
          "session_ids" => [session_id]
        }
      )

    create_conn =
      json_conn(
        :post,
        "/v1/sessions",
        %{"id" => unique_id("ses")},
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert create_conn.status == 403
    assert Jason.decode!(create_conn.resp_body)["error"] == "forbidden"

    list_conn =
      json_conn(:get, "/v1/sessions", nil, [{"authorization", "Bearer #{principal_token}"}])

    assert list_conn.status == 403
    assert Jason.decode!(list_conn.resp_body)["error"] == "forbidden"
  end

  test "lists user-owned sessions for user principal tokens" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-principal-user-list"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)
    visible_session_id = unique_id("ses")
    hidden_session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(visible_session_id, "acme", "user-42"),
               [service_header]
             ).status

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(hidden_session_id, "acme", "user-99"),
               [service_header]
             ).status

    principal_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-42",
            "type" => "user"
          },
          "scopes" => ["session:read"]
        }
      )

    list_conn =
      json_conn(:get, "/v1/sessions", nil, [{"authorization", "Bearer #{principal_token}"}])

    assert list_conn.status == 200
    body = Jason.decode!(list_conn.resp_body)
    ids = body["sessions"] |> Enum.map(& &1["id"])
    assert ids == [visible_session_id]
  end

  test "enforces scope checks for append and tail using principal token" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-principal-scope"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)
    session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(session_id),
               [service_header]
             ).status

    append_only_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "agent-12",
            "type" => "agent"
          },
          "scopes" => ["session:append"],
          "session_ids" => [session_id]
        }
      )

    tail_conn =
      conn_get("/v1/sessions/#{session_id}/tail?cursor=0", [
        {"authorization", "Bearer #{append_only_token}"}
        | websocket_headers()
      ])

    assert tail_conn.status == 403
    assert Jason.decode!(tail_conn.resp_body)["error"] == "forbidden_scope"

    read_only_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "agent-13",
            "type" => "agent"
          },
          "scopes" => ["session:read"],
          "session_ids" => [session_id]
        }
      )

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "scope denied"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [{"authorization", "Bearer #{read_only_token}"}]
      )

    assert append_conn.status == 403
    assert Jason.decode!(append_conn.resp_body)["error"] == "forbidden_scope"
  end

  test "enforces tenant boundary for principal append and tail" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-principal-tenant"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)
    session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(session_id),
               [service_header]
             ).status

    principal_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "beta",
            "id" => "user-tenant-mismatch",
            "type" => "user"
          },
          "scopes" => ["session:read", "session:append"],
          "session_ids" => [session_id]
        }
      )

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "forbidden tenant"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert append_conn.status == 403
    assert Jason.decode!(append_conn.resp_body)["error"] == "forbidden_tenant"

    tail_conn =
      conn_get("/v1/sessions/#{session_id}/tail?cursor=0", [
        {"authorization", "Bearer #{principal_token}"}
        | websocket_headers()
      ])

    assert tail_conn.status == 403
    assert Jason.decode!(tail_conn.resp_body)["error"] == "forbidden_tenant"
  end

  test "rejects actor spoofing for principal append" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-principal-actor"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)
    session_id = unique_id("ses")

    assert 201 ==
             json_conn(
               :post,
               "/v1/sessions",
               service_create_session_body(session_id),
               [service_header]
             ).status

    principal_token =
      issue_principal_token!(
        service_header,
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-actor",
            "type" => "user"
          },
          "scopes" => ["session:append"],
          "session_ids" => [session_id]
        }
      )

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "spoof"},
          "actor" => "user:someone-else",
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [{"authorization", "Bearer #{principal_token}"}]
      )

    assert append_conn.status == 400
    assert Jason.decode!(append_conn.resp_body)["error"] == "invalid_event"
  end

  test "validates issue payload fields and ttl bounds" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-issue-validation"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_header = service_auth_header(private_key, kid)

    invalid_scopes_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-42",
            "type" => "user"
          },
          "scopes" => []
        },
        [service_header]
      )

    assert invalid_scopes_conn.status == 400
    assert Jason.decode!(invalid_scopes_conn.resp_body)["error"] == "invalid_issue_request"

    ttl_too_high_conn =
      json_conn(
        :post,
        "/v1/auth/issue",
        %{
          "principal" => %{
            "tenant_id" => "acme",
            "id" => "user-42",
            "type" => "user"
          },
          "scopes" => ["session:read"],
          "ttl_seconds" => 9_999
        },
        [service_header]
      )

    assert ttl_too_high_conn.status == 400
    assert Jason.decode!(ttl_too_high_conn.resp_body)["error"] == "invalid_issue_request"
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
      jwks_refresh_ms: 1_000,
      principal_token_salt: "principal-token-v1",
      principal_token_default_ttl_seconds: 600,
      principal_token_max_ttl_seconds: 900
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

  defp service_auth_header(private_key, kid) when is_tuple(private_key) and is_binary(kid) do
    token =
      AuthTestSupport.sign_rs256(
        private_key,
        valid_claims(sub: "svc:customer-a"),
        kid
      )

    {"authorization", "Bearer #{token}"}
  end

  defp issue_principal_token!(service_header, body)
       when is_tuple(service_header) and is_map(body) do
    issue_conn = json_conn(:post, "/v1/auth/issue", body, [service_header])
    assert issue_conn.status == 200
    Jason.decode!(issue_conn.resp_body)["token"]
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp service_create_session_body(id, tenant_id \\ "acme", creator_principal_id \\ "user-seed")
       when is_binary(id) and is_binary(tenant_id) and is_binary(creator_principal_id) do
    %{
      "id" => id,
      "creator_principal" => %{
        "tenant_id" => tenant_id,
        "id" => creator_principal_id,
        "type" => "user"
      }
    }
  end
end
