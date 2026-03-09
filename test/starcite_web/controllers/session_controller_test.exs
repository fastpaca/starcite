defmodule StarciteWeb.SessionControllerTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Starcite.AuthTestSupport
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @endpoint StarciteWeb.Endpoint
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    Starcite.Runtime.TestHelper.reset()
    previous_auth = Application.get_env(:starcite, @auth_env_key)
    previous_archive_adapter = Application.get_env(:starcite, :archive_adapter)
    Application.put_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.Postgres)
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

      if is_nil(previous_archive_adapter) do
        Application.delete_env(:starcite, :archive_adapter)
      else
        Application.put_env(:starcite, :archive_adapter, previous_archive_adapter)
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
      original_write_node_ids = Application.get_env(:starcite, :write_node_ids)
      original_replication_factor = Application.get_env(:starcite, :write_replication_factor)

      on_exit(fn ->
        Application.put_env(:starcite, :write_node_ids, original_write_node_ids)
        Application.put_env(:starcite, :write_replication_factor, original_replication_factor)
      end)

      Application.put_env(:starcite, :write_node_ids, [Node.self(), :"missing@127.0.0.1"])
      Application.put_env(:starcite, :write_replication_factor, 2)

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
      assert is_integer(body["epoch"]) and body["epoch"] >= 0
      assert body["cursor"] == %{"epoch" => body["epoch"], "seq" => 1}
      assert body["committed_cursor"] == %{"epoch" => body["epoch"], "seq" => 0}
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
end
