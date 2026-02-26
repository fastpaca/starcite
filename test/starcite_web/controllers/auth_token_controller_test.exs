defmodule StarciteWeb.AuthTokenControllerTest do
  use ExUnit.Case, async: false

  import Plug.Test

  @endpoint StarciteWeb.Endpoint
  @auth_env_key StarciteWeb.Auth

  setup do
    previous_auth = Application.get_env(:starcite, @auth_env_key)

    Application.put_env(
      :starcite,
      @auth_env_key,
      mode: :none,
      jwt_leeway_seconds: 1,
      jwks_refresh_ms: 60_000,
      principal_token_salt: "principal-token-v1",
      principal_token_default_ttl_seconds: 5,
      principal_token_max_ttl_seconds: 15
    )

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end
    end)

    :ok
  end

  test "POST /v1/auth/issue is forbidden in none auth mode" do
    conn =
      json_conn(:post, "/v1/auth/issue", %{
        "principal" => %{"tenant_id" => "acme", "id" => "user-1", "type" => "user"},
        "scopes" => ["session:read"]
      })

    assert conn.status == 403
    body = Jason.decode!(conn.resp_body)
    assert body["error"] == "forbidden"
  end

  defp json_conn(method, path, body) do
    conn =
      conn(method, path)
      |> Plug.Conn.put_req_header("content-type", "application/json")

    conn =
      if body do
        %{conn | body_params: body, params: body}
      else
        conn
      end

    @endpoint.call(conn, @endpoint.init([]))
  end
end
