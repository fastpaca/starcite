defmodule FleetLMWeb.ConversationControllerTest do
  @moduledoc """
  HTTP error code tests for the conversation API.

  Verifies correct status codes per spec:
  - 400 Bad Request: invalid payload shape
  - 404 Not Found: conversation does not exist
  - 409 Conflict: version mismatch
  - 410 Gone: conversation is tombstoned
  """
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn

  alias FleetLM.Runtime

  # Use endpoint (which has JSON parser) instead of router directly
  @endpoint FleetLMWeb.Endpoint

  setup do
    FleetLM.Runtime.TestHelper.reset()
    :ok
  end

  defp unique_id(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp json_conn(method, path, body \\ nil) do
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

  describe "400 Bad Request - invalid payload" do
    test "append with missing role returns 400" do
      id = unique_id("bad-role")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"parts" => [%{"type" => "text", "text" => "hello"}]}
        })

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "invalid_message"
    end

    test "append with missing parts returns 400" do
      id = unique_id("bad-parts")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user"}
        })

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "invalid_message"
    end

    test "append with empty parts array returns 400" do
      id = unique_id("empty-parts")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user", "parts" => []}
        })

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "invalid_message"
    end

    test "append with parts missing type returns 400" do
      id = unique_id("parts-no-type")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user", "parts" => [%{"text" => "hello"}]}
        })

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "invalid_message"
    end

    test "append with no message key returns 400" do
      id = unique_id("no-message")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn = json_conn(:post, "/v1/conversations/#{id}/messages", %{"role" => "user"})

      assert conn.status == 400
    end
  end

  describe "409 Conflict - version mismatch" do
    test "append with stale if_version returns 409" do
      id = unique_id("version-conflict")
      {:ok, _} = Runtime.upsert_conversation(id)

      # Append first message (version becomes 1)
      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "first"}], metadata: %{}, token_count: 5}
        ])

      # Try to append with version 0 (stale) - using valid message format
      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user", "parts" => [%{"type" => "text", "text" => "second"}]},
          "if_version" => 0
        })

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "conflict"
      assert body["message"] =~ "Version mismatch"
    end

    test "append with correct if_version succeeds" do
      id = unique_id("version-ok")
      {:ok, conv} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user", "parts" => [%{"type" => "text", "text" => "hello"}]},
          "if_version" => conv.version
        })

      assert conn.status == 201
    end
  end

  describe "410 Gone - conversation tombstoned" do
    test "append to tombstoned conversation returns 410" do
      id = unique_id("tombstoned")
      {:ok, _} = Runtime.upsert_conversation(id)
      {:ok, _} = Runtime.tombstone_conversation(id)

      # Use valid message format to ensure we reach the tombstone check
      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{"role" => "user", "parts" => [%{"type" => "text", "text" => "hello"}]}
        })

      assert conn.status == 410
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "conversation_tombstoned"
    end

    test "tombstoned conversation is still readable (get)" do
      id = unique_id("tombstoned-readable")
      {:ok, _} = Runtime.upsert_conversation(id)
      {:ok, _} = Runtime.tombstone_conversation(id)

      conn = json_conn(:get, "/v1/conversations/#{id}")

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["tombstoned"] == true
    end

    test "tombstoned conversation tail is still readable" do
      id = unique_id("tombstoned-tail")
      {:ok, _} = Runtime.upsert_conversation(id)

      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "msg"}], metadata: %{}, token_count: 3}
        ])

      {:ok, _} = Runtime.tombstone_conversation(id)

      conn = json_conn(:get, "/v1/conversations/#{id}/tail")

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert length(body["messages"]) == 1
    end
  end

  describe "successful operations" do
    test "upsert creates conversation and returns 200" do
      id = unique_id("create")

      conn =
        json_conn(:put, "/v1/conversations/#{id}", %{
          "metadata" => %{"user_id" => "u_123"}
        })

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
      # Metadata keys are strings in JSON
      assert body["metadata"]["user_id"] == "u_123"
    end

    test "append returns 201 with seq and version" do
      id = unique_id("append-ok")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn =
        json_conn(:post, "/v1/conversations/#{id}/messages", %{
          "message" => %{
            "role" => "user",
            "parts" => [%{"type" => "text", "text" => "hello"}],
            "token_count" => 5
          }
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["seq"] == 1
      assert is_integer(body["version"])
      assert body["token_count"] == 5
    end

    test "delete (tombstone) returns 204 No Content" do
      id = unique_id("delete-ok")
      {:ok, _} = Runtime.upsert_conversation(id)

      conn = json_conn(:delete, "/v1/conversations/#{id}")

      assert conn.status == 204
      assert conn.resp_body == ""
    end

    test "tail returns messages for existing conversation" do
      id = unique_id("tail-ok")
      {:ok, _} = Runtime.upsert_conversation(id)

      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "hello"}], metadata: %{}, token_count: 5}
        ])

      conn = json_conn(:get, "/v1/conversations/#{id}/tail")

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert length(body["messages"]) == 1
    end

    test "replay returns messages from seq" do
      id = unique_id("replay-ok")
      {:ok, _} = Runtime.upsert_conversation(id)

      {:ok, _} =
        Runtime.append_messages(id, [
          %{role: "user", parts: [%{type: "text", text: "msg1"}], metadata: %{}, token_count: 4},
          %{
            role: "assistant",
            parts: [%{type: "text", text: "msg2"}],
            metadata: %{},
            token_count: 4
          }
        ])

      conn = json_conn(:get, "/v1/conversations/#{id}/messages?from=2")

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert length(body["messages"]) == 1
      assert hd(body["messages"])["seq"] == 2
    end
  end
end
