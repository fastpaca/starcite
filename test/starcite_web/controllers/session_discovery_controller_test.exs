defmodule StarciteWeb.SessionDiscoveryControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
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

    token = token_for(private_key, kid, %{"sub" => "user:user-stream", "tenant_id" => "acme"})
    Process.put(:default_auth_header, {"authorization", "Bearer #{token}"})

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end

      :ok = safe_clear_jwks_cache()
      Process.delete(:default_auth_header)
    end)

    {:ok, private_key: private_key, kid: kid}
  end

  describe "GET /v1/sessions/stream" do
    test "returns 400 without websocket upgrade headers" do
      conn = conn_get("/v1/sessions/stream")

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_websocket_upgrade"
      assert is_binary(body["message"])
    end

    test "returns 403 when jwt is missing session:read scope", %{
      private_key: private_key,
      kid: kid
    } do
      token =
        token_for(private_key, kid, %{
          "sub" => "user:user-stream",
          "tenant_id" => "acme",
          "scopes" => ["session:append"]
        })

      conn =
        conn_get("/v1/sessions/stream", [
          {"authorization", "Bearer #{token}"},
          {"connection", "upgrade"},
          {"upgrade", "websocket"},
          {"sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ=="},
          {"sec-websocket-version", "13"}
        ])

      assert conn.status == 403
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "forbidden_scope"
      assert is_binary(body["message"])
    end
  end

  defp conn_get(path, headers \\ []) do
    has_auth_header? =
      Enum.any?(headers, fn {key, _value} -> String.downcase(key) == "authorization" end)

    default_headers =
      if has_auth_header? do
        []
      else
        case Process.get(:default_auth_header) do
          nil -> []
          default_header -> [default_header]
        end
      end

    conn = conn(:get, path)

    conn =
      Enum.reduce(default_headers ++ headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)

    @endpoint.call(conn, @endpoint.init([]))
  end

  defp token_for(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    claims =
      %{
        "iss" => @issuer,
        "aud" => @audience,
        "sub" => "user:user-stream",
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "exp" => System.system_time(:second) + 300
      }
      |> Map.merge(overrides)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end

  defp safe_clear_jwks_cache do
    try do
      JWKS.clear_cache()
    rescue
      ArgumentError -> :ok
    end
  end
end
