defmodule StarciteWeb.TailControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.WritePath

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp conn_get(path, headers \\ []) do
    conn = conn(:get, path)
    conn = Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
    @endpoint.call(conn, @endpoint.init([]))
  end

  describe "GET /v1/sessions/:id/tail" do
    test "returns 400 without websocket upgrade headers" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

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
      {:ok, _} = WritePath.create_session(id: id)

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
  end
end
