defmodule StarciteWeb.TailWebSocketIntegrationTest do
  use ExUnit.Case, async: false

  alias Starcite.AuthTestSupport
  alias Starcite.WebSocketTestClient
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup_all do
    port = pick_available_port()

    {:ok, _pid} =
      start_supervised(
        {Bandit,
         plug: StarciteWeb.Endpoint,
         scheme: :http,
         ip: {127, 0, 0, 1},
         port: port,
         startup_log: false}
      )

    {:ok, port: port}
  end

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

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  test "tail websocket replays from cursor and streams live committed events", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {:ok, _reply} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "replay"},
        actor: "agent:test"
      })

    token =
      token_for(
        private_key,
        kid,
        %{"sub" => "user:user-42", "scopes" => ["session:read"], "tenant_id" => "acme"}
      )

    {:ok, socket, response_headers, buffer} =
      connect_tail_ws(port, session_id, 0, %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    {frame_one, buffer} = recv_text_frame(socket, buffer)
    replay_event = Jason.decode!(frame_one)

    assert replay_event["seq"] == 1
    assert replay_event["type"] == "content"
    assert replay_event["payload"]["text"] == "replay"
    assert String.ends_with?(replay_event["inserted_at"], "Z")

    {:ok, _reply} =
      append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test"
      })

    {frame_two, _buffer} = recv_text_frame(socket, buffer)
    live_event = Jason.decode!(frame_two)

    assert live_event["seq"] == 2
    assert live_event["type"] == "state"
    assert live_event["payload"]["state"] == "running"
    assert String.ends_with?(live_event["inserted_at"], "Z")

    :ok = :gen_tcp.close(socket)
  end

  test "tail websocket emits array frames when batch_size is greater than one", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    for n <- 1..3 do
      {:ok, _reply} =
        append_event(session_id, %{
          type: "content",
          payload: %{text: "replay-#{n}"},
          actor: "agent:test"
        })
    end

    token =
      token_for(
        private_key,
        kid,
        %{"sub" => "user:user-42", "scopes" => ["session:read"], "tenant_id" => "acme"}
      )

    {:ok, socket, response_headers, buffer} =
      connect_tail_ws(port, session_id, 0, %{"access_token" => token, "batch_size" => "2"})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    {frame_one, buffer} = recv_text_frame(socket, buffer)
    replay_batch_one = Jason.decode!(frame_one)
    assert Enum.map(replay_batch_one, &Map.fetch!(&1, "seq")) == [1, 2]

    {frame_two, buffer} = recv_text_frame(socket, buffer)
    replay_batch_two = Jason.decode!(frame_two)
    assert Enum.map(replay_batch_two, &Map.fetch!(&1, "seq")) == [3]

    {:ok, _reply} =
      append_event(session_id, %{
        type: "state",
        payload: %{state: "running"},
        actor: "agent:test"
      })

    {frame_three, _buffer} = recv_text_frame(socket, buffer)
    live_batch = Jason.decode!(frame_three)
    assert Enum.map(live_batch, &Map.fetch!(&1, "seq")) == [4]

    :ok = :gen_tcp.close(socket)
  end

  test "tail websocket closes with token_expired at JWT exp", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    token =
      token_for(
        private_key,
        kid,
        %{
          "sub" => "user:user-42",
          "tenant_id" => "acme",
          "scopes" => ["session:read"],
          "session_id" => session_id,
          "exp" => System.system_time(:second) + 1
        }
      )

    {:ok, socket, response_headers, buffer} =
      connect_tail_ws(port, session_id, 0, %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    {{4001, "token_expired"}, _rest} = recv_close_frame(socket, buffer)
    :ok = :gen_tcp.close(socket)
  end

  test "tail websocket append returns ack, streams the committed event, and serializes conflicts",
       %{
         port: port,
         private_key: private_key,
         kid: kid
       } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    token =
      token_for(
        private_key,
        kid,
        %{
          "sub" => "user:user-42",
          "tenant_id" => "acme",
          "scopes" => ["session:read", "session:append"]
        }
      )

    {:ok, socket, response_headers, buffer} =
      connect_tail_ws(port, session_id, 0, %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    :ok =
      send_text_frame(
        socket,
        %{
          "type" => "append",
          "ref" => "append-1",
          "event" => %{
            "type" => "content",
            "payload" => %{"text" => "hello"},
            "producer_id" => "writer-1",
            "producer_seq" => 1
          }
        }
      )

    {ack_frame, buffer} = recv_text_frame(socket, buffer)

    assert Jason.decode!(ack_frame) == %{
             "type" => "ack",
             "ref" => "append-1",
             "seq" => 1,
             "last_seq" => 1,
             "deduped" => false
           }

    {event_frame, buffer} = recv_text_frame(socket, buffer)
    event = Jason.decode!(event_frame)

    assert event["seq"] == 1
    assert event["payload"]["text"] == "hello"
    assert event["producer_id"] == "writer-1"

    :ok =
      send_text_frame(
        socket,
        %{
          "type" => "append",
          "ref" => "append-2",
          "event" => %{
            "type" => "content",
            "payload" => %{"text" => "conflict"},
            "producer_id" => "writer-1",
            "producer_seq" => 2,
            "expected_seq" => 0
          }
        }
      )

    {error_frame, _buffer} = recv_text_frame(socket, buffer)

    assert Jason.decode!(error_frame) == %{
             "type" => "error",
             "ref" => "append-2",
             "error" => "expected_seq_conflict",
             "message" => "Expected seq 0, current seq is 1"
           }

    :ok = :gen_tcp.close(socket)
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

  defp connect_tail_ws(port, session_id, cursor, query_params)
       when is_integer(port) and port > 0 and is_binary(session_id) and is_integer(cursor) and
              cursor >= 0 and is_map(query_params) do
    path = "/v1/sessions/#{session_id}/tail?" <> build_tail_query(cursor, query_params)
    WebSocketTestClient.connect(port, path)
  end

  defp build_tail_query(cursor, query_params)
       when is_integer(cursor) and cursor >= 0 and is_map(query_params) do
    query_params
    |> Map.put("cursor", Integer.to_string(cursor))
    |> URI.encode_query()
  end

  defdelegate recv_text_frame(socket, buffer), to: WebSocketTestClient
  defdelegate recv_close_frame(socket, buffer), to: WebSocketTestClient

  defp send_text_frame(socket, payload) when is_map(payload) do
    WebSocketTestClient.send_text_frame(socket, Jason.encode!(payload))
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

  defp pick_available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:ip, {127, 0, 0, 1}}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
