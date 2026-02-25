defmodule StarciteWeb.SessionControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.{Repo, WritePath}

  @endpoint StarciteWeb.Endpoint

  setup do
    Starcite.Runtime.TestHelper.reset()
    :ok = ensure_repo_started()
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    :ok
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

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  defp service_create_body(attrs) when is_map(attrs) do
    Map.merge(
      %{
        "creator_principal" => %{"tenant_id" => "acme", "id" => "user-test", "type" => "user"}
      },
      attrs
    )
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
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "title" => "Draft",
            "metadata" => %{"workflow" => "legal"}
          })
        )

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert String.starts_with?(body["id"], "ses_")
      assert body["title"] == "Draft"
      assert body["metadata"]["workflow"] == "legal"
      assert body["metadata"]["tenant_id"] == "acme"
      assert body["last_seq"] == 0
      assert String.ends_with?(body["created_at"], "Z")
      assert String.ends_with?(body["updated_at"], "Z")
    end

    test "creates session with caller-provided id" do
      id = unique_id("ses")

      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "id" => id,
            "metadata" => %{"tenant_id" => "acme"}
          })
        )

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
    end

    test "rejects metadata tenant_id mismatch with creator principal tenant" do
      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "metadata" => %{"tenant_id" => "beta"}
          })
        )

      assert conn.status == 403
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "forbidden_tenant"
    end

    test "accepts empty title string" do
      id = unique_id("ses")

      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "id" => id,
            "title" => ""
          })
        )

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
      assert body["title"] == ""
    end

    test "duplicate id returns 409" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      conn = json_conn(:post, "/v1/sessions", service_create_body(%{"id" => id}))

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_exists"
      assert is_binary(body["message"])
    end
  end

  describe "GET /v1/sessions" do
    test "lists sessions and supports metadata filter" do
      marker = unique_id("marker")
      id1 = unique_id("ses")
      id2 = unique_id("ses")
      id3 = unique_id("ses")

      assert 201 ==
               json_conn(:post, "/v1/sessions", %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-test",
                   "type" => "user"
                 },
                 "id" => id1,
                 "title" => "A",
                 "metadata" => %{"user_id" => "u1", "tenant_id" => "acme", "marker" => marker}
               }).status

      assert 201 ==
               json_conn(:post, "/v1/sessions", %{
                 "creator_principal" => %{
                   "tenant_id" => "acme",
                   "id" => "user-test",
                   "type" => "user"
                 },
                 "id" => id2,
                 "title" => "B",
                 "metadata" => %{"user_id" => "u2", "tenant_id" => "acme", "marker" => marker}
               }).status

      assert 201 ==
               json_conn(:post, "/v1/sessions", %{
                 "creator_principal" => %{
                   "tenant_id" => "beta",
                   "id" => "user-test",
                   "type" => "user"
                 },
                 "id" => id3,
                 "title" => "C",
                 "metadata" => %{"user_id" => "u1", "tenant_id" => "beta", "marker" => marker}
               }).status

      conn =
        json_conn(
          :get,
          "/v1/sessions?metadata[user_id]=u1&metadata[marker]=#{marker}",
          nil
        )

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      ids = body["sessions"] |> Enum.map(& &1["id"]) |> Enum.sort()

      assert ids == Enum.sort([id1, id3])
      assert body["next_cursor"] in [nil, id1, id3]
    end

    test "supports cursor pagination" do
      marker = unique_id("marker")
      id1 = unique_id("ses-a")
      id2 = unique_id("ses-b")

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => id1,
                   "metadata" => %{"marker" => marker}
                 })
               ).status

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => id2,
                   "metadata" => %{"marker" => marker}
                 })
               ).status

      conn1 = json_conn(:get, "/v1/sessions?limit=1&metadata[marker]=#{marker}", nil)
      assert conn1.status == 200

      body1 = Jason.decode!(conn1.resp_body)
      assert length(body1["sessions"]) == 1
      assert is_binary(body1["next_cursor"])

      cursor = body1["next_cursor"]

      conn2 =
        json_conn(
          :get,
          "/v1/sessions?limit=1&metadata[marker]=#{marker}&cursor=#{cursor}",
          nil
        )

      assert conn2.status == 200
      body2 = Jason.decode!(conn2.resp_body)
      assert length(body2["sessions"]) == 1

      ids = [hd(body1["sessions"])["id"], hd(body2["sessions"])["id"]] |> Enum.sort()
      assert ids == Enum.sort([id1, id2])
    end

    test "invalid limit returns 400" do
      conn = json_conn(:get, "/v1/sessions?limit=0", nil)

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_limit"
    end
  end

  describe "POST /v1/sessions/:id/append" do
    test "appends an event" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "agent:test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["seq"] == 1
      assert body["last_seq"] == 1
      assert body["deduped"] == false
    end

    test "expected_seq conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      {:ok, _} =
        WritePath.append_event(id, %{
          type: "content",
          payload: %{text: "one"},
          actor: "agent:test",
          producer_id: "writer-1",
          producer_seq: 1
        })

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "two"},
          "actor" => "agent:test",
          "producer_id" => "writer-1",
          "producer_seq" => 2,
          "expected_seq" => 0
        })

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "expected_seq_conflict"
      assert is_binary(body["message"])
    end

    test "producer replay conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      body = %{
        "type" => "state",
        "payload" => %{"state" => "running"},
        "actor" => "agent:test",
        "producer_id" => "writer-1",
        "producer_seq" => 1
      }

      conn1 = json_conn(:post, "/v1/sessions/#{id}/append", body)

      assert conn1.status == 201

      conn2 =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "state",
          "payload" => %{"state" => "completed"},
          "actor" => "agent:test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn2.status == 409
      body2 = Jason.decode!(conn2.resp_body)
      assert body2["error"] == "producer_replay_conflict"
      assert is_binary(body2["message"])
    end

    test "same producer sequence and payload dedupes" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      body = %{
        "type" => "state",
        "payload" => %{"state" => "running"},
        "actor" => "agent:test",
        "producer_id" => "writer-1",
        "producer_seq" => 1
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

    test "producer sequence conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "state",
          "payload" => %{"state" => "running"},
          "actor" => "agent:test",
          "producer_id" => "writer-1",
          "producer_seq" => 2
        })

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "producer_seq_conflict"
      assert is_binary(body["message"])
    end

    test "missing required fields returns 400" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id)

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
          "actor" => "agent:test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 404
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_not_found"
      assert is_binary(body["message"])
    end
  end
end
