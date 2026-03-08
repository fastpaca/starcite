defmodule StarciteWeb.TailChannelWebSocketIntegrationTest do
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

  test "phoenix tail socket appends, replies with ack, streams the event, and serializes replay conflicts",
       %{
         port: port,
         private_key: private_key,
         kid: kid
       } do
    session_id = unique_id("ses")
    topic = "tail:#{session_id}"
    {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    token =
      token_for(private_key, kid, %{
        "sub" => "user:user-42",
        "tenant_id" => "acme",
        "scopes" => ["session:read", "session:append"]
      })

    {:ok, socket, response_headers, buffer} =
      WebSocketTestClient.connect(
        port,
        "/v1/tail/socket/websocket?vsn=2.0.0&access_token=#{URI.encode_www_form(token)}"
      )

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    :ok = send_phoenix_message(socket, "1", "1", topic, "phx_join", %{"cursor" => 0})

    {%{"status" => "ok", "response" => %{}}, buffer} =
      recv_phoenix_reply(socket, buffer, topic, "1")

    :ok =
      send_phoenix_message(socket, "1", "2", topic, "append", %{
        "type" => "content",
        "payload" => %{"text" => "hello"},
        "producer_id" => "writer-1",
        "producer_seq" => 1
      })

    {%{"status" => "ok", "response" => ack}, buffer} =
      recv_phoenix_reply(socket, buffer, topic, "2")

    assert ack == %{"seq" => 1, "last_seq" => 1, "deduped" => false}

    {%{"events" => [event]}, buffer} = recv_phoenix_push(socket, buffer, topic, "events")

    assert event["seq"] == 1
    assert event["payload"]["text"] == "hello"
    assert event["producer_id"] == "writer-1"

    :ok =
      send_phoenix_message(socket, "1", "3", topic, "append", %{
        "type" => "content",
        "payload" => %{"text" => "changed"},
        "producer_id" => "writer-1",
        "producer_seq" => 1
      })

    {%{"status" => "error", "response" => error}, _buffer} =
      recv_phoenix_reply(socket, buffer, topic, "3")

    assert error == %{
             "error" => "producer_replay_conflict",
             "message" => "Producer sequence was already used with different event content"
           }

    :ok = :gen_tcp.close(socket)
  end

  defp send_phoenix_message(socket, join_ref, ref, topic, event, payload)
       when is_binary(join_ref) and is_binary(ref) and is_binary(topic) and is_binary(event) and
              is_map(payload) do
    WebSocketTestClient.send_text_frame(
      socket,
      Jason.encode!([join_ref, ref, topic, event, payload])
    )
  end

  defp recv_phoenix_reply(socket, buffer, topic, ref)
       when is_binary(topic) and is_binary(ref) do
    receive_phoenix_message(socket, buffer, fn
      [_join_ref, ^ref, ^topic, "phx_reply", payload] -> {:match, payload}
      _message -> :skip
    end)
  end

  defp recv_phoenix_push(socket, buffer, topic, event)
       when is_binary(topic) and is_binary(event) do
    receive_phoenix_message(socket, buffer, fn
      [_join_ref, nil, ^topic, ^event, payload] -> {:match, payload}
      [_join_ref, ref, ^topic, ^event, payload] when is_nil(ref) -> {:match, payload}
      _message -> :skip
    end)
  end

  defp receive_phoenix_message(socket, buffer, matcher) when is_function(matcher, 1) do
    {frame, next_buffer} = WebSocketTestClient.recv_text_frame(socket, buffer)
    payload = Jason.decode!(frame)

    case matcher.(payload) do
      {:match, result} -> {result, next_buffer}
      :skip -> receive_phoenix_message(socket, next_buffer, matcher)
    end
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

  defp pick_available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:ip, {127, 0, 0, 1}}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end
