defmodule StarciteWeb.SessionControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias Starcite.DataPlane.SessionStore
  alias Starcite.ReadPath
  alias Starcite.Repo
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    Starcite.Runtime.TestHelper.reset()
    ensure_repo_sandbox()
    previous_auth = Application.get_env(:starcite, @auth_env_key)
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.stub(bypass, "GET", @jwks_path, fn conn ->
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

    token = token_for(private_key, kid, %{"sub" => "user:user-test", "tenant_id" => "acme"})
    Process.put(:default_auth_header, {"authorization", "Bearer #{token}"})

    on_exit(fn ->
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

  defp service_create_body(attrs) when is_map(attrs) do
    Map.merge(
      %{
        "creator_principal" => %{"tenant_id" => "acme", "id" => "user-test", "type" => "user"}
      },
      attrs
    )
  end

  defp json_conn(method, path, body, headers \\ []) do
    auth_headers =
      case Process.get(:default_auth_header) do
        nil -> headers
        default_header -> [default_header | headers]
      end

    conn =
      conn(method, path)
      |> put_req_header("content-type", "application/json")
      |> put_headers(auth_headers)

    conn =
      if body do
        %{conn | body_params: body, params: body}
      else
        conn
      end

    @endpoint.call(conn, @endpoint.init([]))
  end

  defp ensure_repo_sandbox do
    if Process.whereis(Repo) == nil do
      _pid = start_supervised!(Repo)
      :ok
    end

    case Ecto.Adapters.SQL.Sandbox.checkout(Repo) do
      :ok -> :ok
      {:already, _owner} -> :ok
    end

    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})
    :ok
  end

  defp put_headers(conn, headers) when is_list(headers) do
    Enum.reduce(headers, conn, fn {name, value}, acc ->
      put_req_header(acc, name, value)
    end)
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
      assert body["last_seq"] == 0
      assert body["archived"] == false
      assert String.ends_with?(body["created_at"], "Z")
      assert String.ends_with?(body["updated_at"], "Z")
      assert body["version"] == 1
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

    test "accepts metadata tenant_id mismatch because tenancy comes from principal" do
      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "metadata" => %{"tenant_id" => "beta"}
          })
        )

      assert conn.status == 201
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
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn = json_conn(:post, "/v1/sessions", service_create_body(%{"id" => id}))

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_exists"
      assert is_binary(body["message"])
    end

    test "returns 503 when in-memory replication quorum cannot be reached" do
      original_cluster_node_ids = Application.get_env(:starcite, :cluster_node_ids)
      original_replication_factor = Application.get_env(:starcite, :routing_replication_factor)

      on_exit(fn ->
        Application.put_env(:starcite, :cluster_node_ids, original_cluster_node_ids)
        Application.put_env(:starcite, :routing_replication_factor, original_replication_factor)
      end)

      Application.put_env(:starcite, :cluster_node_ids, [Node.self(), :"missing@127.0.0.1"])
      Application.put_env(:starcite, :routing_replication_factor, 2)

      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "id" => unique_id("ses"),
            "metadata" => %{"workflow" => "replication-failure"}
          })
        )

      assert conn.status == 503
      body = Jason.decode!(conn.resp_body)
      assert body["error"] in ["owner_unavailable", "replication_unavailable"]
      assert is_binary(body["message"])
    end

    test "supports explicit no-auth mode for local runs" do
      Application.put_env(:starcite, @auth_env_key, mode: :none)
      Process.delete(:default_auth_header)

      conn =
        json_conn(:post, "/v1/sessions", %{
          "id" => unique_id("ses"),
          "title" => "Local",
          "metadata" => %{"workflow" => "local"}
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["title"] == "Local"
      assert body["metadata"]["workflow"] == "local"
      refute Map.has_key?(body["metadata"], "tenant_id")
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

      assert {:ok, _} =
               WritePath.create_session(
                 id: id3,
                 tenant_id: "beta",
                 metadata: %{"user_id" => "u1", "tenant_id" => "beta", "marker" => marker}
               )

      conn =
        json_conn(
          :get,
          "/v1/sessions?metadata[user_id]=u1&metadata[marker]=#{marker}",
          nil
        )

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      ids = body["sessions"] |> Enum.map(& &1["id"]) |> Enum.sort()

      assert ids == [id1]
      assert body["next_cursor"] in [nil, id1]
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

    test "lists sessions from the durable catalog without touching event archive reads" do
      marker = unique_id("catalog-only")

      id = unique_id("ses-catalog-http")

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => id,
                   "metadata" => %{"marker" => marker}
                 })
               ).status

      conn = json_conn(:get, "/v1/sessions?metadata[marker]=#{marker}", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert Enum.map(body["sessions"], & &1["id"]) == [id]
    end

    test "excludes archived sessions from default results and supports archived filters" do
      active_id = unique_id("ses-active")
      archived_id = unique_id("ses-archived")
      marker = unique_id("archive-filter")

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => active_id,
                   "metadata" => %{"marker" => marker}
                 })
               ).status

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => archived_id,
                   "metadata" => %{"marker" => marker}
                 })
               ).status

      archive_conn = json_conn(:post, "/v1/sessions/#{archived_id}/archive", %{})
      assert archive_conn.status == 200
      assert Jason.decode!(archive_conn.resp_body)["archived"] == true

      default_conn = json_conn(:get, "/v1/sessions?metadata[marker]=#{marker}", nil)
      assert default_conn.status == 200

      assert Enum.map(Jason.decode!(default_conn.resp_body)["sessions"], & &1["id"]) == [
               active_id
             ]

      archived_conn =
        json_conn(:get, "/v1/sessions?archived=true&metadata[marker]=#{marker}", nil)

      assert archived_conn.status == 200

      assert Enum.map(Jason.decode!(archived_conn.resp_body)["sessions"], & &1["id"]) == [
               archived_id
             ]

      all_conn = json_conn(:get, "/v1/sessions?archived=all&metadata[marker]=#{marker}", nil)
      assert all_conn.status == 200

      assert Enum.sort(Enum.map(Jason.decode!(all_conn.resp_body)["sessions"], & &1["id"])) ==
               Enum.sort([active_id, archived_id])
    end

    test "invalid limit returns 400" do
      conn = json_conn(:get, "/v1/sessions?limit=0", nil)

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_limit"
    end
  end

  describe "PATCH /v1/sessions/:id" do
    test "updates title and merges metadata without dropping unrelated keys" do
      id = unique_id("ses")

      assert {:ok, created} =
               WritePath.create_session(
                 id: id,
                 tenant_id: "acme",
                 title: "Draft",
                 metadata: %{"workflow" => "contract", "tags" => ["one"]}
               )

      conn =
        json_conn(:patch, "/v1/sessions/#{id}", %{
          "title" => "Final",
          "metadata" => %{"summary" => "Generated", "tags" => ["one", "two"]}
        })

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
      assert body["title"] == "Final"

      assert body["metadata"] == %{
               "workflow" => "contract",
               "summary" => "Generated",
               "tags" => ["one", "two"]
             }

      assert body["last_seq"] == 0
      assert body["created_at"] == created.created_at
      assert body["version"] == 2

      {:ok, created_at, _} = DateTime.from_iso8601(body["created_at"])
      {:ok, updated_at, _} = DateTime.from_iso8601(body["updated_at"])
      assert DateTime.compare(updated_at, created_at) in [:eq, :gt]
    end

    test "supports optimistic concurrency with expected_version" do
      id = unique_id("ses")

      assert {:ok, created} =
               WritePath.create_session(
                 id: id,
                 tenant_id: "acme",
                 title: "Draft",
                 metadata: %{"workflow" => "contract"}
               )

      assert 200 ==
               json_conn(:patch, "/v1/sessions/#{id}", %{
                 "metadata" => %{"summary" => "fresh"},
                 "expected_version" => created.version
               }).status

      conn =
        json_conn(:patch, "/v1/sessions/#{id}", %{
          "title" => "Stale",
          "expected_version" => created.version
        })

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "expected_version_conflict"
      assert is_binary(body["message"])
      assert body["current_version"] == 2
    end

    test "rejects no-op patches" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn = json_conn(:patch, "/v1/sessions/#{id}", %{"metadata" => %{}})

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_session"
    end
  end

  describe "GET /v1/sessions/:id" do
    test "returns archived sessions by id" do
      id = unique_id("ses")

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => id,
                   "title" => "Archived draft"
                 })
               ).status

      assert 200 == json_conn(:post, "/v1/sessions/#{id}/archive", %{}).status

      conn = json_conn(:get, "/v1/sessions/#{id}", nil)
      assert conn.status == 200

      body = Jason.decode!(conn.resp_body)
      assert body["id"] == id
      assert body["title"] == "Archived draft"
      assert body["archived"] == true
    end
  end

  describe "POST /v1/sessions/:id/archive" do
    test "archives and unarchives a session" do
      id = unique_id("ses")

      assert 201 ==
               json_conn(
                 :post,
                 "/v1/sessions",
                 service_create_body(%{
                   "id" => id,
                   "title" => "Reversible"
                 })
               ).status

      archive_conn = json_conn(:post, "/v1/sessions/#{id}/archive", %{})
      assert archive_conn.status == 200

      archived_body = Jason.decode!(archive_conn.resp_body)
      assert archived_body["id"] == id
      assert archived_body["archived"] == true

      unarchive_conn = json_conn(:post, "/v1/sessions/#{id}/unarchive", %{})
      assert unarchive_conn.status == 200

      unarchived_body = Jason.decode!(unarchive_conn.resp_body)
      assert unarchived_body["id"] == id
      assert unarchived_body["archived"] == false
    end
  end

  describe "POST /v1/sessions/:id/append" do
    test "appends an event" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert body["seq"] == 1
      assert body["last_seq"] == 1
      assert body["deduped"] == false
      refute Map.has_key?(body, "epoch")
      assert body["cursor"] == 1
      assert body["committed_cursor"] == 0
    end

    test "expected_seq conflict returns 409" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

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
          "actor" => "user:user-test",
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
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      body = %{
        "type" => "state",
        "payload" => %{"state" => "running"},
        "actor" => "user:user-test",
        "producer_id" => "writer-1",
        "producer_seq" => 1
      }

      conn1 = json_conn(:post, "/v1/sessions/#{id}/append", body)

      assert conn1.status == 201

      conn2 =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "state",
          "payload" => %{"state" => "completed"},
          "actor" => "user:user-test",
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
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      body = %{
        "type" => "state",
        "payload" => %{"state" => "running"},
        "actor" => "user:user-test",
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

    test "missing required fields returns 400" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

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
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 404
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "session_not_found"
      assert is_binary(body["message"])
    end
  end

  describe "ingest-edge telemetry" do
    test "create emits success with error_reason none" do
      attach_ingest_edge_handler()

      conn =
        json_conn(
          :post,
          "/v1/sessions",
          service_create_body(%{
            "title" => "Telemetry"
          })
        )

      assert conn.status == 201
      assert_receive_ingest_edge(:create_session, :ok, :none, "acme")
    end

    test "append expected_seq conflict emits seq_conflict reason" do
      attach_ingest_edge_handler()
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

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
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 2,
          "expected_seq" => 0
        })

      assert conn.status == 409
      assert_receive_ingest_edge(:append_event, :error, :seq_conflict, "acme")
    end

    test "update expected_version conflict emits a dedicated conflict reason" do
      attach_ingest_edge_handler()
      id = unique_id("ses")

      assert {:ok, session} =
               WritePath.create_session(
                 id: id,
                 tenant_id: "acme",
                 title: "Draft",
                 metadata: %{"workflow" => "contract"}
               )

      assert 200 ==
               json_conn(:patch, "/v1/sessions/#{id}", %{
                 "metadata" => %{"summary" => "fresh"},
                 "expected_version" => session.version
               }).status

      conn =
        json_conn(:patch, "/v1/sessions/#{id}", %{
          "title" => "Stale",
          "expected_version" => session.version
        })

      assert conn.status == 409
      assert_receive_ingest_edge(:update_session, :error, :expected_version_conflict, "acme")
    end
  end

  describe "request telemetry" do
    test "successful append emits request telemetry with error_reason none" do
      attach_request_handler()
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 201

      assert_receive_requests(:append_event, [
        {:route, :ok, :none},
        {:dispatch, :ok, :none},
        {:ack, :ok, :none},
        {:total, :ok, :none}
      ])
    end

    test "append expected_seq conflict emits request telemetry with conflict reason" do
      attach_request_handler()
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

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
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 2,
          "expected_seq" => 0
        })

      assert conn.status == 409

      assert_receive_requests(:append_event, [
        {:route, :ok, :none},
        {:dispatch, :error, :expected_seq_conflict},
        {:ack, :error, :expected_seq_conflict},
        {:total, :error, :expected_seq_conflict}
      ])
    end
  end

  describe "POST /v1/sessions/:id/projections" do
    test "writes projection item versions from the request payload" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      conn =
        json_conn(
          :post,
          "/v1/sessions/#{id}/projections",
          %{
            "items" => [
              %{
                "item_id" => "msg-1",
                "version" => 1,
                "from_seq" => 1,
                "to_seq" => 2,
                "payload" => %{"role" => "assistant", "text" => "one two"},
                "metadata" => %{"kind" => "message"},
                "schema" => "ai.message.v1",
                "content_type" => "application/json"
              },
              %{
                "item_id" => "msg-2",
                "version" => 1,
                "from_seq" => 3,
                "to_seq" => 3,
                "payload" => %{"role" => "assistant", "text" => "three"},
                "metadata" => %{"kind" => "message"}
              }
            ]
          }
        )

      assert conn.status == 201
      body = Jason.decode!(conn.resp_body)
      assert [first_item, second_item] = body["items"]
      assert first_item["item_id"] == "msg-1"
      assert first_item["version"] == 1
      assert first_item["from_seq"] == 1
      assert first_item["to_seq"] == 2
      assert first_item["payload"] == %{"role" => "assistant", "text" => "one two"}
      assert first_item["metadata"] == %{"kind" => "message"}
      assert first_item["schema"] == "ai.message.v1"
      assert first_item["content_type"] == "application/json"
      assert second_item["item_id"] == "msg-2"
      assert second_item["version"] == 1
      assert second_item["from_seq"] == 3
      assert second_item["to_seq"] == 3

      assert {:ok, latest_items} = ReadPath.latest_projection_items(id)
      assert Enum.map(latest_items, & &1.item_id) == ["msg-1", "msg-2"]
    end

    test "rejects overlapping latest items" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three", "four"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _item} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      conn =
        json_conn(
          :post,
          "/v1/sessions/#{id}/projections",
          %{
            "items" => [
              %{
                "item_id" => "msg-2",
                "version" => 1,
                "from_seq" => 2,
                "to_seq" => 3,
                "payload" => %{"text" => "two three"},
                "metadata" => %{}
              }
            ]
          }
        )

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "projection_item_overlap"
    end

    test "rejects duplicate item_ids in a single batch" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      conn =
        json_conn(
          :post,
          "/v1/sessions/#{id}/projections",
          %{
            "items" => [
              %{
                "item_id" => "msg-1",
                "version" => 1,
                "from_seq" => 1,
                "to_seq" => 1,
                "payload" => %{"text" => "one"},
                "metadata" => %{}
              },
              %{
                "item_id" => "msg-1",
                "version" => 2,
                "from_seq" => 2,
                "to_seq" => 2,
                "payload" => %{"text" => "two"},
                "metadata" => %{}
              }
            ]
          }
        )

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "projection_item_duplicate"
    end

    test "rejects overlapping intervals within the same batch" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      conn =
        json_conn(
          :post,
          "/v1/sessions/#{id}/projections",
          %{
            "items" => [
              %{
                "item_id" => "msg-1",
                "version" => 1,
                "from_seq" => 1,
                "to_seq" => 2,
                "payload" => %{"text" => "one two"},
                "metadata" => %{}
              },
              %{
                "item_id" => "msg-2",
                "version" => 1,
                "from_seq" => 2,
                "to_seq" => 3,
                "payload" => %{"text" => "two three"},
                "metadata" => %{}
              }
            ]
          }
        )

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "projection_item_overlap"
    end

    test "rejects intervals outside the current raw seq range" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      conn =
        json_conn(
          :post,
          "/v1/sessions/#{id}/projections",
          %{
            "item_id" => "msg-1",
            "version" => 1,
            "from_seq" => 1,
            "to_seq" => 3,
            "payload" => %{"text" => "one two three"},
            "metadata" => %{}
          }
        )

      assert conn.status == 400
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "invalid_projection_item"
    end
  end

  describe "GET /v1/sessions/:id/projections" do
    test "lists the latest projection version for each item id" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 3,
                 to_seq: 3,
                 payload: %{"text" => "three"},
                 metadata: %{}
               })

      conn = json_conn(:get, "/v1/sessions/#{id}/projections", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)

      assert [%{"item_id" => "msg-1", "version" => 2}, %{"item_id" => "msg-2", "version" => 1}] =
               body["items"]
    end

    test "rejects cross-tenant projection reads across all projection endpoints" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "beta")

      {:ok, _} =
        WritePath.append_event(id, %{
          type: "content",
          payload: %{text: "hidden"},
          actor: "user:user-beta",
          producer_id: "writer-1",
          producer_seq: 1
        })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "hidden"},
                 metadata: %{}
               })

      for path <- [
            "/v1/sessions/#{id}/projections",
            "/v1/sessions/#{id}/projections/msg-1",
            "/v1/sessions/#{id}/projections/msg-1/versions",
            "/v1/sessions/#{id}/projections/msg-1/versions/1"
          ] do
        conn = json_conn(:get, path, nil)

        assert conn.status == 403
        body = Jason.decode!(conn.resp_body)
        assert body["error"] == "forbidden_tenant"
      end
    end
  end

  describe "GET /v1/sessions/:id/projections/:item_id" do
    test "returns the latest stored version for one item id" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["item_id"] == "msg-1"
      assert body["version"] == 2
      assert body["payload"] == %{"text" => "one two"}
    end
  end

  describe "GET /v1/sessions/:id/projections/:item_id/versions" do
    test "lists all stored versions for one item id" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1/versions", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert Enum.map(body["items"], & &1["version"]) == [1, 2]
    end

    test "reads version history from persisted archived-safe projection state" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three", "four"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      assert {:ok, %{archived_seq: 4}} = WritePath.ack_archived(id, 4)

      :ok = SessionStore.clear()

      conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1/versions", nil)

      assert conn.status == 200
      assert Enum.map(Jason.decode!(conn.resp_body)["items"], & &1["version"]) == [1, 2]
    end
  end

  describe "GET /v1/sessions/:id/projections/:item_id/versions/:version" do
    test "returns one stored projection item version" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1/versions/1", nil)

      assert conn.status == 200
      body = Jason.decode!(conn.resp_body)
      assert body["item_id"] == "msg-1"
      assert body["version"] == 1
      assert body["payload"] == %{"text" => "one"}
    end
  end

  describe "DELETE /v1/sessions/:id/projections/:item_id" do
    test "deletes all stored versions for one item id" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two", "three"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-2",
                 version: 1,
                 from_seq: 3,
                 to_seq: 3,
                 payload: %{"text" => "three"},
                 metadata: %{}
               })

      conn = json_conn(:delete, "/v1/sessions/#{id}/projections/msg-1", nil)

      assert conn.status == 204
      assert {:error, :projection_item_not_found} = ReadPath.get_projection_item(id, "msg-1")
      assert {:ok, [%{item_id: "msg-2"}]} = ReadPath.latest_projection_items(id)
    end

    test "deletes all stored versions from the public projection API" do
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      for {text, producer_seq} <- Enum.with_index(["one", "two"], 1) do
        {:ok, _} =
          WritePath.append_event(id, %{
            type: "content",
            payload: %{text: text},
            actor: "user:user-test",
            producer_id: "writer-1",
            producer_seq: producer_seq
          })
      end

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 1,
                 from_seq: 1,
                 to_seq: 1,
                 payload: %{"text" => "one"},
                 metadata: %{}
               })

      assert {:ok, _} =
               WritePath.put_projection_item(id, %{
                 item_id: "msg-1",
                 version: 2,
                 from_seq: 1,
                 to_seq: 2,
                 payload: %{"text" => "one two"},
                 metadata: %{}
               })

      conn = json_conn(:delete, "/v1/sessions/#{id}/projections/msg-1", nil)

      assert conn.status == 204

      versions_conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1/versions", nil)
      assert versions_conn.status == 404
      assert Jason.decode!(versions_conn.resp_body)["error"] == "projection_item_not_found"

      version_conn = json_conn(:get, "/v1/sessions/#{id}/projections/msg-1/versions/1", nil)
      assert version_conn.status == 404
      assert Jason.decode!(version_conn.resp_body)["error"] == "projection_item_not_found"
    end
  end

  describe "endpoint telemetry" do
    test "append emits Phoenix endpoint stop telemetry" do
      attach_endpoint_handler()
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 201
      assert_receive_endpoint_stop("POST", 201)
    end

    test "append emits edge-stage telemetry before controller timing starts" do
      attach_edge_stage_handler()
      id = unique_id("ses")
      {:ok, _} = WritePath.create_session(id: id, tenant_id: "acme")

      conn =
        json_conn(:post, "/v1/sessions/#{id}/append", %{
          "type" => "content",
          "payload" => %{"text" => "hello"},
          "actor" => "user:user-test",
          "producer_id" => "writer-1",
          "producer_seq" => 1
        })

      assert conn.status == 201
      assert_receive_edge_stage(:controller_entry, "POST")
    end
  end

  defp token_for(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    claims =
      %{
        "iss" => @issuer,
        "aud" => @audience,
        "sub" => "user:user-test",
        "tenant_id" => "acme",
        "scope" => "session:create session:read session:append",
        "exp" => System.system_time(:second) + 300
      }
      |> Map.merge(overrides)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end

  defp attach_ingest_edge_handler do
    handler_id = "ingest-edge-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :ingest, :edge],
        fn _event, measurements, metadata, pid ->
          send(pid, {:ingest_edge_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp attach_request_handler do
    handler_id = "request-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :request],
        fn _event, measurements, metadata, pid ->
          send(pid, {:request_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp attach_endpoint_handler do
    handler_id = "endpoint-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:phoenix, :endpoint, :stop],
        fn _event, measurements, metadata, pid ->
          send(pid, {:endpoint_stop_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp attach_edge_stage_handler do
    handler_id = "edge-stage-#{System.unique_integer([:positive, :monotonic])}"
    test_pid = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:starcite, :edge, :stage],
        fn _event, measurements, metadata, pid ->
          send(pid, {:edge_stage_event, measurements, metadata})
        end,
        test_pid
      )

    on_exit(fn ->
      :telemetry.detach(handler_id)
    end)
  end

  defp assert_receive_ingest_edge(operation, outcome, error_reason, tenant_id) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_ingest_edge(operation, outcome, error_reason, tenant_id, deadline)
  end

  defp do_assert_receive_ingest_edge(operation, outcome, error_reason, tenant_id, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:ingest_edge_event, %{count: 1},
       %{
         operation: ^operation,
         outcome: ^outcome,
         error_reason: ^error_reason,
         tenant_id: ^tenant_id
       }} ->
        :ok

      {:ingest_edge_event, _measurements, _metadata} ->
        do_assert_receive_ingest_edge(operation, outcome, error_reason, tenant_id, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for ingest edge telemetry operation=#{inspect(operation)} outcome=#{inspect(outcome)} reason=#{inspect(error_reason)}"
        )
    end
  end

  defp assert_receive_requests(operation, expectations)
       when operation in [:append_event, :append_events] and is_list(expectations) do
    expected = MapSet.new(expectations)
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_requests(operation, expected, deadline)
  end

  defp do_assert_receive_requests(_operation, expected, _deadline)
       when is_struct(expected, MapSet) and map_size(expected.map) == 0 do
    :ok
  end

  defp do_assert_receive_requests(operation, expected, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:request_event, %{count: 1, duration_ms: duration_ms},
       %{operation: ^operation, phase: phase, outcome: outcome, error_reason: error_reason}} ->
        assert is_integer(duration_ms)
        assert duration_ms >= 0

        do_assert_receive_requests(
          operation,
          MapSet.delete(expected, {phase, outcome, error_reason}),
          deadline
        )

      {:request_event, _measurements, _metadata} ->
        do_assert_receive_requests(operation, expected, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for request telemetry operation=#{inspect(operation)} expectations=#{inspect(MapSet.to_list(expected))}"
        )
    end
  end

  defp assert_receive_endpoint_stop(method, status)
       when is_binary(method) and is_integer(status) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_endpoint_stop(method, status, deadline)
  end

  defp assert_receive_edge_stage(stage, method)
       when stage in [:controller_entry] and is_binary(method) do
    deadline = System.monotonic_time(:millisecond) + 1_000
    do_assert_receive_edge_stage(stage, method, deadline)
  end

  defp do_assert_receive_edge_stage(stage, method, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:edge_stage_event, %{count: 1, duration_ms: duration_ms},
       %{stage: ^stage, method: ^method}} ->
        assert is_integer(duration_ms)
        assert duration_ms >= 0
        :ok

      {:edge_stage_event, _measurements, _metadata} ->
        do_assert_receive_edge_stage(stage, method, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for edge-stage telemetry stage=#{inspect(stage)} method=#{inspect(method)}"
        )
    end
  end

  defp do_assert_receive_endpoint_stop(method, status, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:endpoint_stop_event, %{duration: duration}, %{conn: %{method: ^method, status: ^status}}} ->
        assert is_integer(duration)
        assert duration >= 0
        :ok

      {:endpoint_stop_event, _measurements, _metadata} ->
        do_assert_receive_endpoint_stop(method, status, deadline)
    after
      remaining ->
        flunk(
          "timed out waiting for Phoenix endpoint stop telemetry method=#{inspect(method)} status=#{inspect(status)}"
        )
    end
  end
end
