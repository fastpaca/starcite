defmodule StarciteWeb.Plugs.ServiceAuthTest do
  use ExUnit.Case, async: false

  import Plug.Test

  alias StarciteWeb.Plugs.ServiceAuth

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
end
