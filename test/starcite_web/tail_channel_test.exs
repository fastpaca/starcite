defmodule StarciteWeb.TailChannelTest do
  use ExUnit.Case, async: false

  import Phoenix.ChannelTest

  alias Starcite.AuthTestSupport
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS
  alias StarciteWeb.TailUserSocket

  @endpoint StarciteWeb.Endpoint
  @auth_env_key StarciteWeb.Auth
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup do
    Starcite.Runtime.TestHelper.reset()
    Process.put(:producer_seq_counters, %{})

    previous_auth = Application.get_env(:starcite, @auth_env_key)
    bypass = Bypass.open()
    private_key = AuthTestSupport.generate_rsa_private_key()
    kid = "kid-#{System.unique_integer([:positive, :monotonic])}"
    jwks = AuthTestSupport.jwks_for_private_key(private_key, kid)

    Bypass.expect(bypass, "GET", @jwks_path, fn conn ->
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.resp(200, Jason.encode!(jwks))
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

    on_exit(fn ->
      if is_nil(previous_auth) do
        Application.delete_env(:starcite, @auth_env_key)
      else
        Application.put_env(:starcite, @auth_env_key, previous_auth)
      end

      :ok = JWKS.clear_cache()
    end)

    {:ok, private_key: private_key, kid: kid}
  end

  test "socket connect authenticates once and join authorizes each topic", %{
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    other_session_id = unique_id("ses")

    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")
    {:ok, _} = WritePath.create_session(id: other_session_id, tenant_id: "acme")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "session_id" => session_id
      })

    assert {:ok, socket} =
             connect(TailUserSocket, %{}, connect_info: %{auth_token: token})

    assert socket.assigns.auth_context.session_id == session_id
    assert is_binary(socket.id)

    assert {:ok, _, _joined_socket} =
             subscribe_and_join(socket, "tail:#{session_id}", %{"cursor" => 0})

    assert {:error, %{reason: "forbidden_session"}} =
             subscribe_and_join(socket, "tail:#{other_session_id}", %{"cursor" => 0})
  end

  test "one socket can join multiple tail topics and receive direct broadcasts", %{
    private_key: private_key,
    kid: kid
  } do
    session_one = unique_id("ses")
    session_two = unique_id("ses")

    {:ok, _} = WritePath.create_session(id: session_one, tenant_id: "acme")
    {:ok, _} = WritePath.create_session(id: session_two, tenant_id: "acme")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read"]
      })

    assert {:ok, transport_socket} =
             connect(TailUserSocket, %{}, connect_info: %{auth_token: token})

    assert {:ok, _, _socket_one} =
             subscribe_and_join(transport_socket, "tail:#{session_one}", %{"cursor" => 0})

    assert {:ok, _, _socket_two} =
             subscribe_and_join(transport_socket, "tail:#{session_two}", %{"cursor" => 0})

    {:ok, %{seq: 1}} =
      append_event(session_one, %{
        type: "content",
        payload: %{text: "one"},
        actor: "agent:test"
      })

    assert_push "events", %{
      events: [%{seq: 1, payload: %{text: "one"}, inserted_at: inserted_at}]
    }

    assert is_binary(inserted_at)

    {:ok, %{seq: 1}} =
      append_event(session_two, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test"
      })

    assert_push "events", %{
      events: [%{seq: 1, payload: %{state: "running"}, inserted_at: inserted_at}]
    }

    assert is_binary(inserted_at)
  end

  test "channel append replies with ack and streams the committed event", %{
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read", "session:append"]
      })

    assert {:ok, transport_socket} =
             connect(TailUserSocket, %{}, connect_info: %{auth_token: token})

    assert {:ok, _, joined_socket} =
             subscribe_and_join(transport_socket, "tail:#{session_id}", %{"cursor" => 0})

    ref =
      push(joined_socket, "append", %{
        "type" => "content",
        "payload" => %{"text" => "hello"},
        "producer_id" => "writer-1",
        "producer_seq" => 1
      })

    assert_reply ref, :ok, %{seq: 1, last_seq: 1, deduped: false}

    assert_push "events", %{
      events: [%{seq: 1, payload: %{"text" => "hello"}, inserted_at: inserted_at}]
    }

    assert is_binary(inserted_at)
  end

  test "channel append returns a policy error when append scope is missing", %{
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read"]
      })

    assert {:ok, transport_socket} =
             connect(TailUserSocket, %{}, connect_info: %{auth_token: token})

    assert {:ok, _, joined_socket} =
             subscribe_and_join(transport_socket, "tail:#{session_id}", %{"cursor" => 0})

    ref =
      push(joined_socket, "append", %{
        "type" => "content",
        "payload" => %{"text" => "hello"},
        "producer_id" => "writer-1",
        "producer_seq" => 1
      })

    assert_reply ref, :error, %{
      error: "forbidden_scope",
      message: "Token scope does not allow this operation"
    }
  end

  test "join enforces tenant match for each topic", %{
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")

    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "globex")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read"]
      })

    assert {:ok, transport_socket} =
             connect(TailUserSocket, %{}, connect_info: %{auth_token: token})

    assert {:error, %{reason: "forbidden_tenant"}} =
             subscribe_and_join(transport_socket, "tail:#{session_id}", %{"cursor" => 0})
  end

  defp append_event(id, event, opts \\ [])
       when is_binary(id) and is_map(event) and is_list(opts) do
    producer_id = Map.get(event, :producer_id, "writer:test")

    enriched_event =
      event
      |> Map.put_new(:producer_id, producer_id)
      |> Map.put_new_lazy(:producer_seq, fn -> next_producer_seq(id, producer_id) end)

    WritePath.append_event(id, enriched_event, opts)
  end

  defp next_producer_seq(session_id, producer_id)
       when is_binary(session_id) and is_binary(producer_id) do
    counters = Process.get(:producer_seq_counters, %{})
    key = {session_id, producer_id}
    seq = Map.get(counters, key, 0) + 1
    Process.put(:producer_seq_counters, Map.put(counters, key, seq))
    seq
  end

  defp token_for(private_key, kid, overrides)
       when is_tuple(private_key) and is_binary(kid) and is_map(overrides) do
    now = System.system_time(:second)

    claims =
      %{
        "iss" => @issuer,
        "aud" => @audience,
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "exp" => now + 300
      }
      |> Map.merge(overrides)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)
      |> Map.new()

    AuthTestSupport.sign_rs256(private_key, claims, kid)
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end
end
