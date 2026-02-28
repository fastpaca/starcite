defmodule StarciteWeb.ApiAuthJwtTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias Starcite.{ReadPath, Repo, WritePath}
  alias StarciteWeb.Auth.JWKS

  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok = ensure_repo_started()
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
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

  test "returns 401 when token is missing" do
    bypass = Bypass.open()
    configure_jwt_auth!(bypass)

    conn = json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")})

    assert conn.status == 401
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "unauthorized"
    assert get_resp_header(conn, "www-authenticate") == [~s(Bearer realm="starcite")]
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
      token_for(private_key, kid, %{
        "aud" => "not-starcite-api",
        "sub" => "user:user-42"
      })

    conn =
      json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")}, [
        {"authorization", "Bearer #{token}"}
      ])

    assert conn.status == 401
    assert Jason.decode!(conn.resp_body)["error"] == "unauthorized"
  end

  test "returns 401 when JWT is missing required claims" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-missing-claims"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    for claims <- [
          %{"sub" => "user:user-42"} |> valid_claims() |> Map.delete("tenant_id"),
          %{"sub" => "user:user-42"}
          |> valid_claims()
          |> Map.delete("scope")
          |> Map.delete("scopes"),
          %{"sub" => "user:user-42"} |> valid_claims() |> Map.delete("sub"),
          valid_claims(%{"sub" => "user:user-42", "session_id" => 42})
        ] do
      token = AuthTestSupport.sign_rs256(private_key, claims, kid)

      conn =
        json_conn(:post, "/v1/sessions", %{"id" => unique_id("ses")}, [
          {"authorization", "Bearer #{token}"}
        ])

      assert conn.status == 401
      assert Jason.decode!(conn.resp_body)["error"] == "unauthorized"
    end
  end

  test "accepts valid JWT for create list append and derives actor from sub" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-valid-flow"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    session_id = unique_id("ses")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "scope" => "session:create session:read session:append",
        "session_id" => session_id
      })

    auth_header = {"authorization", "Bearer #{token}"}

    create_conn = json_conn(:post, "/v1/sessions", %{"id" => session_id}, [auth_header])
    assert create_conn.status == 201

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "ok"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [auth_header]
      )

    assert append_conn.status == 201

    {:ok, [event]} = ReadPath.get_events_from_cursor(session_id, 0, 10)
    assert event.actor == "user:user-42"
    assert event.metadata["starcite_principal"]["tenant_id"] == "acme"
    assert event.metadata["starcite_principal"]["actor"] == "user:user-42"
    assert event.metadata["starcite_principal"]["principal_type"] == "user"
    assert event.metadata["starcite_principal"]["principal_id"] == "user-42"

    assert_session_list_contains(auth_header, session_id)
  end

  test "enforces scope checks for append and list" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-scope-checks"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    read_only_token = token_for(private_key, kid, %{"scope" => "session:read"})

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

    append_only_token = token_for(private_key, kid, %{"scope" => "session:append"})

    list_conn =
      json_conn(:get, "/v1/sessions", nil, [{"authorization", "Bearer #{append_only_token}"}])

    assert list_conn.status == 403
    assert Jason.decode!(list_conn.resp_body)["error"] == "forbidden_scope"
  end

  test "enforces tenant boundary for append, list, and tail" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-tenant-boundary"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)
    beta_session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: beta_session_id, tenant_id: "beta")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-acme",
        "tenant_id" => "acme",
        "scope" => "session:read session:append"
      })

    auth_header = {"authorization", "Bearer #{token}"}

    append_conn =
      json_conn(
        :post,
        "/v1/sessions/#{beta_session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "forbidden tenant"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [auth_header]
      )

    assert append_conn.status == 403
    assert Jason.decode!(append_conn.resp_body)["error"] == "forbidden_tenant"

    list_conn = json_conn(:get, "/v1/sessions", nil, [auth_header])
    assert list_conn.status == 200
    ids = Jason.decode!(list_conn.resp_body)["sessions"] |> Enum.map(& &1["id"])
    refute beta_session_id in ids

    tail_conn =
      conn_get("/v1/sessions/#{beta_session_id}/tail?cursor=0", [
        auth_header
        | websocket_headers()
      ])

    assert tail_conn.status == 403
    assert Jason.decode!(tail_conn.resp_body)["error"] == "forbidden_tenant"
  end

  test "session_id claim locks list and append to one session" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-session-lock"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    service_token =
      token_for(private_key, kid, %{"scope" => "session:create session:read session:append"})

    service_header = {"authorization", "Bearer #{service_token}"}
    allowed_session_id = unique_id("ses")
    blocked_session_id = unique_id("ses")

    assert 201 ==
             json_conn(:post, "/v1/sessions", %{"id" => allowed_session_id}, [service_header]).status

    assert 201 ==
             json_conn(:post, "/v1/sessions", %{"id" => blocked_session_id}, [service_header]).status

    scoped_token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "scope" => "session:read session:append",
        "session_id" => allowed_session_id
      })

    scoped_header = {"authorization", "Bearer #{scoped_token}"}

    list_conn = json_conn(:get, "/v1/sessions", nil, [scoped_header])
    assert list_conn.status == 200
    list_ids = Jason.decode!(list_conn.resp_body)["sessions"] |> Enum.map(& &1["id"])
    assert list_ids == [allowed_session_id]

    allowed_append =
      json_conn(
        :post,
        "/v1/sessions/#{allowed_session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "ok"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [scoped_header]
      )

    assert allowed_append.status == 201

    blocked_append =
      json_conn(
        :post,
        "/v1/sessions/#{blocked_session_id}/append",
        %{
          "type" => "content",
          "payload" => %{"text" => "blocked"},
          "producer_id" => "writer:test",
          "producer_seq" => 1
        },
        [scoped_header]
      )

    assert blocked_append.status == 403
    assert Jason.decode!(blocked_append.resp_body)["error"] == "forbidden_session"
  end

  test "protects tail websocket upgrade with JWT auth and session lock" do
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-tail-auth"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    configure_jwt_auth!(bypass)

    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    without_auth = conn_get("/v1/sessions/#{session_id}/tail?cursor=0", websocket_headers())
    assert without_auth.status == 401

    scoped_token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "scope" => "session:read",
        "session_id" => unique_id("ses")
      })

    denied_tail =
      conn_get(
        "/v1/sessions/#{session_id}/tail?cursor=0&access_token=#{URI.encode_www_form(scoped_token)}",
        websocket_headers()
      )

    assert denied_tail.status == 403
    assert Jason.decode!(denied_tail.resp_body)["error"] == "forbidden_session"
  end

  defp ensure_repo_started do
    case Process.whereis(Repo) do
      nil ->
        case Repo.start_link() do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
        end

      _pid ->
        :ok
    end
  end

  defp configure_jwt_auth!(bypass) do
    Application.put_env(
      :starcite,
      StarciteWeb.Auth,
      issuer: @issuer,
      audience: @audience,
      jwks_url: "http://localhost:#{bypass.port}#{@jwks_path}",
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 1_000
    )
  end

  defp valid_claims(overrides) when is_map(overrides) do
    %{
      "iss" => @issuer,
      "aud" => @audience,
      "sub" => "user:user-42",
      "tenant_id" => "acme",
      "scope" => "session:create session:read session:append",
      "exp" => System.system_time(:second) + 300
    }
    |> Map.merge(overrides)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp token_for(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    claims = valid_claims(overrides)
    AuthTestSupport.sign_rs256(private_key, claims, kid)
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

  defp assert_session_list_contains(auth_header, session_id)
       when is_tuple(auth_header) and is_binary(session_id) and session_id != "" do
    eventually(
      fn ->
        list_conn = json_conn(:get, "/v1/sessions", nil, [auth_header])
        assert list_conn.status == 200

        ids = Jason.decode!(list_conn.resp_body)["sessions"] |> Enum.map(& &1["id"])
        assert session_id in ids
      end,
      timeout: 2_000,
      interval: 50
    )
  end

  defp eventually(fun, opts) when is_function(fun, 0) and is_list(opts) do
    timeout = Keyword.get(opts, :timeout, 1_000)
    interval = Keyword.get(opts, :interval, 50)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_eventually(fun, deadline, interval)
  end

  defp do_eventually(fun, deadline, interval) do
    fun.()
  rescue
    error in [ExUnit.AssertionError] ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(interval)
        do_eventually(fun, deadline, interval)
      else
        reraise(error, __STACKTRACE__)
      end
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end
end
