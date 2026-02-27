defmodule StarciteWeb.TailControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias Starcite.Archive.TestAdapter
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    previous_archive_adapter = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, TestAdapter)

    Starcite.Runtime.TestHelper.reset()

    previous_auth = Application.get_env(:starcite, @auth_env_key)
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> put_resp_content_type("application/json")
      |> resp(200, Jason.encode!(jwks))
    end)

    Application.put_env(
      :starcite,
      @auth_env_key,
      issuer: @issuer,
      audience: @audience,
      jwks_url: "http://localhost:#{bypass.port}#{@jwks_path}",
      jwt_leeway_seconds: 0,
      jwks_refresh_ms: 1_000
    )

    token = token_for(private_key, kid, %{"sub" => "user:user-tail", "tenant_id" => "acme"})
    Process.put(:default_auth_header, {"authorization", "Bearer #{token}"})

    on_exit(fn ->
      if is_nil(previous_archive_adapter) do
        Application.delete_env(:starcite, :archive_adapter)
      else
        Application.put_env(:starcite, :archive_adapter, previous_archive_adapter)
      end

      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end

      :ok = JWKS.clear_cache()
      Process.delete(:default_auth_header)
    end)

    :ok
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp conn_get(path, headers \\ []) do
    auth_headers =
      case Process.get(:default_auth_header) do
        nil -> headers
        default_header -> [default_header | headers]
      end

    conn = conn(:get, path)
    conn = Enum.reduce(auth_headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    @endpoint.call(conn, @endpoint.init([]))
  end

  describe "GET /v1/sessions/:id/tail" do
    test "returns 400 without websocket upgrade headers" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, metadata: %{"tenant_id" => "acme"})

      conn = conn_get("/v1/sessions/#{id}/tail?cursor=0")

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_websocket_upgrade"
      assert is_binary(body["message"])
    end

    test "returns 404 for missing session" do
      conn =
        conn_get("/v1/sessions/missing/tail?cursor=0", [
          {"connection", "upgrade"},
          {"upgrade", "websocket"},
          {"sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="},
          {"sec-websocket-version", "13"}
        ])

      assert conn.status == 404
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_not_found"
      assert is_binary(body["message"])
    end

    test "returns 400 for invalid cursor" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, metadata: %{"tenant_id" => "acme"})

      conn =
        conn_get("/v1/sessions/#{id}/tail?cursor=bad", [
          {"connection", "upgrade"},
          {"upgrade", "websocket"},
          {"sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="},
          {"sec-websocket-version", "13"}
        ])

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_cursor"
      assert is_binary(body["message"])
    end

    test "returns 400 for invalid tail batch size" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, metadata: %{"tenant_id" => "acme"})

      conn =
        conn_get("/v1/sessions/#{id}/tail?cursor=0&batch_size=bad", [
          {"connection", "upgrade"},
          {"upgrade", "websocket"},
          {"sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="},
          {"sec-websocket-version", "13"}
        ])

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_tail_batch_size"
      assert is_binary(body["message"])
    end
  end

  defp token_for(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    claims =
      %{
        "iss" => @issuer,
        "aud" => @audience,
        "sub" => "user:user-tail",
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "exp" => System.system_time(:second) + 300
      }
      |> Map.merge(overrides)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end
end
