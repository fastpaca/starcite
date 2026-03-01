defmodule StarciteWeb.Plugs.CORSTest do
  use ExUnit.Case, async: true

  import Plug.Conn
  import Plug.Test

  @endpoint StarciteWeb.Endpoint

  test "adds permissive CORS headers to regular responses" do
    conn =
      conn(:get, "/health/live")
      |> put_req_header("origin", "https://ui.example")
      |> @endpoint.call(@endpoint.init([]))

    assert conn.status == 200
    assert get_resp_header(conn, "access-control-allow-origin") == ["*"]
    assert get_resp_header(conn, "access-control-expose-headers") == ["*"]
    assert get_resp_header(conn, "access-control-allow-credentials") == []
  end

  test "short-circuits browser preflight requests" do
    conn =
      conn(:options, "/v1/sessions")
      |> put_req_header("origin", "https://ui.example")
      |> put_req_header("access-control-request-method", "POST")
      |> put_req_header("access-control-request-headers", "authorization,content-type")
      |> @endpoint.call(@endpoint.init([]))

    assert conn.status == 204
    assert conn.halted
    assert conn.resp_body == ""
    assert get_resp_header(conn, "access-control-allow-origin") == ["*"]

    assert get_resp_header(conn, "access-control-allow-methods") == [
             "GET,POST,PUT,PATCH,DELETE,OPTIONS"
           ]

    assert get_resp_header(conn, "access-control-allow-headers") == [
             "authorization,content-type"
           ]

    assert get_resp_header(conn, "access-control-expose-headers") == ["*"]
    assert get_resp_header(conn, "access-control-allow-credentials") == []
  end
end
