defmodule StarciteWeb.OpenApiSpecTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  @endpoint StarciteWeb.Endpoint

  test "GET /v1/openapi returns the generated OpenAPI document" do
    conn =
      conn(:get, "/v1/openapi")
      |> put_req_header("accept", "application/json")

    conn = @endpoint.call(conn, @endpoint.init([]))

    assert conn.status == 200

    body = Jason.decode!(conn.resp_body)
    assert body["openapi"] == "3.0.0"
    assert body["info"]["title"] == "Starcite API"
    assert Map.has_key?(body["paths"], "/v1/sessions")
    assert Map.has_key?(body["paths"], "/v1/sessions/{id}/append")
    assert Map.has_key?(body["paths"], "/v1/sessions/{id}/tail")
  end

  test "GET /v1/docs serves Swagger UI" do
    conn = conn(:get, "/v1/docs")
    conn = @endpoint.call(conn, @endpoint.init([]))

    assert conn.status == 200
    assert conn.resp_body =~ "Swagger UI"
    assert conn.resp_body =~ "/v1/openapi"
  end
end
