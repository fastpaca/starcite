defmodule StarciteWeb.SessionControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.Runtime

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp json_conn(method, path, body) do
    conn =
      conn(method, path)
      |> put_req_header("content-type", "application/json")

    conn =
      if body do
        %{conn | body_params: body, params: body}
      else
        conn
      end

    @endpoint.call(conn, @endpoint.init([]))
  end

  describe "POST /v1/sessions" do
    test "creates session with server-generated id" do
      conn =
        json_conn(:post, "/v1/sessions", %{
          "title" => "Draft",
          "metadata" => %{"workflow" => "legal"}
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert String.starts_with?(body["id"], "ses_")
      assert body["title"] == "Draft"
      assert body["metadata"]["workflow"] == "legal"
      assert body["last_seq"] == 0
      assert String.ends_with?(body["created_at"], "Z")
      assert String.ends_with?(body["updated_at"], "Z")
    end

    test "creates session with caller-provided id" do
      id = unique_id("ses")

      conn =
        json_conn(:post, "/v1/sessions", %{
          "id" => id,
          "metadata" => %{"tenant_id" => "acme"}
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
    end

    test "accepts empty title string" do
      id = unique_id("ses")

      conn =
        json_conn(:post, "/v1/sessions", %{
          "id" => id,
          "title" => ""
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
      assert body["title"] == ""
    end

    test "duplicate id returns 409" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      conn = json_conn(:post, "/v1/sessions", %{"id" => id})

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_exists"
      assert is_binary(body["message"])
    end
  end

  describe "POST /v1/sessions/:id/append" do
    test "appends an event" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "agent:test"
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["seq"] == 1
      assert body["last_seq"] == 1
      assert body["deduped"] == false
    end

    test "expected_seq conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      {:ok, _} =
        Runtime.append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:test"
        })

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "two"},
          "actor" => "agent:test",
          "expected_seq" => 0
        })

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "expected_seq_conflict"
      assert is_binary(body["message"])
    end

    test "idempotency conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      conn1 =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "state",
          "payload" => %{"state" => "running"},
          "actor" => "agent:test",
          "idempotency_key" => "k1"
        })

      assert conn1.status == 201

      conn2 =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "state",
          "payload" => %{"state" => "completed"},
          "actor" => "agent:test",
          "idempotency_key" => "k1"
        })

      assert conn2.status == 409
      body = Jason.decode!(conn2.resp_body)
      assert body["error"] == "idempotency_conflict"
      assert is_binary(body["message"])
    end

    test "same idempotency key and payload dedupes" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      body = %{
        "type" => "state",
        "payload" => %{"state" => "running"},
        "actor" => "agent:test",
        "idempotency_key" => "k1"
      }

      conn1 = json_conn(:post, "/v1/sessions/#{id}/append", body)
      conn2 = json_conn(:post, "/v1/sessions/#{id}/append", body)

      assert conn1.status == 201
      assert conn2.status == 201

      seq1 = Jason.decode!(conn1.resp_body)["seq"]
      body2 = Jason.decode!(conn2.resp_body)

      assert body2["seq"] == seq1
      assert body2["deduped"] == true
    end

    test "missing required fields returns 400" do
      id = unique_id("ses")
      {:ok, _} = Runtime.create_session(id: id)

      conn = json_conn(:post, "/v1/sessions/#{id}/append", %{"payload" => %{"text" => "hi"}})

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_event"
      assert is_binary(body["message"])
    end

    test "returns 404 with error message when session is missing" do
      id = unique_id("missing")

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "agent:test"
        })

      assert conn.status == 404
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_not_found"
      assert is_binary(body["message"])
    end
  end
end
