defmodule StarciteWeb.TailChannelWebSocketIntegrationTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Starcite.AuthTestSupport
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @host ~c"127.0.0.1"
  @ws_timeout 2_500
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup_all do
    port = pick_available_port()

    {:ok, _pid} =
      start_supervised(
        {Plug.Cowboy,
         plug: StarciteWeb.Endpoint,
         scheme: :http,
         ip: {127, 0, 0, 1},
         port: port,
         ref: :"tail_channel_socket_test_#{port}"}
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

    Bypass.stub(bypass, "GET", @jwks_path, fn conn ->
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

  test "one websocket can join multiple tail channels and receive replay plus live events", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    topic_a = "tail:#{session_a = unique_id("ses")}"
    topic_b = "tail:#{session_b = unique_id("ses")}"
    {:ok, _} = WritePath.create_session(id: session_a, tenant_id: "acme")
    {:ok, _} = WritePath.create_session(id: session_b, tenant_id: "acme")

    {:ok, _} =
      append_event(session_a, %{
        type: "content",
        payload: %{text: "replay-a"},
        actor: "agent:test"
      })

    {:ok, _} =
      append_event(session_b, %{
        type: "content",
        payload: %{text: "replay-b"},
        actor: "agent:test"
      })

    token =
      token_for(
        private_key,
        kid,
        %{"sub" => "user:user-42", "scopes" => ["session:read"], "tenant_id" => "acme"}
      )

    {:ok, socket, response_headers, buffer} = connect_socket_ws(port, %{"token" => token})
    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    :ok = send_channel_join(socket, "1", topic_a, %{})

    {%{event: "phx_reply", topic: ^topic_a, payload: %{"status" => "ok"}}, buffer} =
      recv_channel_message(socket, buffer)

    {%{event: "events", topic: ^topic_a, payload: %{"events" => [replay_a]}}, buffer} =
      recv_channel_message(socket, buffer)

    assert replay_a["seq"] == 1
    assert replay_a["payload"]["text"] == "replay-a"

    :ok = send_channel_join(socket, "2", topic_b, %{})

    {%{event: "phx_reply", topic: ^topic_b, payload: %{"status" => "ok"}}, buffer} =
      recv_channel_message(socket, buffer)

    {%{event: "events", topic: ^topic_b, payload: %{"events" => [replay_b]}}, buffer} =
      recv_channel_message(socket, buffer)

    assert replay_b["seq"] == 1
    assert replay_b["payload"]["text"] == "replay-b"

    {:ok, _} =
      append_event(session_a, %{
        type: "state",
        payload: %{state: "running-a"},
        actor: "agent:test"
      })

    {:ok, _} =
      append_event(session_b, %{
        type: "state",
        payload: %{state: "running-b"},
        actor: "agent:test"
      })

    {message_one, buffer} = recv_channel_message(socket, buffer)
    {message_two, _buffer} = recv_channel_message(socket, buffer)

    live_messages =
      [message_one, message_two]
      |> Enum.filter(&(&1.event == "events"))
      |> Enum.map(fn %{topic: topic, payload: %{"events" => [event]}} ->
        {topic, event["seq"], event["payload"]}
      end)
      |> Enum.sort()

    assert live_messages == [
             {topic_a, 2, %{"state" => "running-a"}},
             {topic_b, 2, %{"state" => "running-b"}}
           ]

    :ok = :gen_tcp.close(socket)
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
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

  defp connect_socket_ws(port, query_params)
       when is_integer(port) and port > 0 and is_map(query_params) do
    {:ok, socket} =
      :gen_tcp.connect(@host, port, [:binary, active: false, packet: :raw], @ws_timeout)

    key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    query =
      query_params
      |> Map.put("vsn", "2.0.0")
      |> URI.encode_query()

    request = [
      "GET /v1/socket/websocket?#{query} HTTP/1.1\r\n",
      "Host: localhost:#{port}\r\n",
      "Connection: Upgrade\r\n",
      "Upgrade: websocket\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: #{key}\r\n",
      "\r\n"
    ]

    :ok = :gen_tcp.send(socket, request)

    case recv_until_headers(socket, <<>>) do
      {:ok, response_headers, buffer} ->
        {:ok, socket, response_headers, buffer}

      {:error, reason} ->
        :gen_tcp.close(socket)
        {:error, reason}
    end
  end

  defp send_channel_join(socket, ref, topic, payload)
       when is_binary(ref) and is_binary(topic) and is_map(payload) do
    message = Jason.encode!(["#{ref}", ref, topic, "phx_join", payload])
    :gen_tcp.send(socket, encode_client_text_frame(message))
  end

  defp recv_channel_message(socket, buffer) do
    {payload, rest} = recv_text_frame(socket, buffer)
    [join_ref, ref, topic, event, message_payload] = Jason.decode!(payload)

    {%{join_ref: join_ref, ref: ref, topic: topic, event: event, payload: message_payload}, rest}
  end

  defp recv_until_headers(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, _len} ->
        header_bytes = binary_part(buffer, 0, index + 4)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, header_bytes, rest}

      :nomatch ->
        case :gen_tcp.recv(socket, 0, @ws_timeout) do
          {:ok, bytes} -> recv_until_headers(socket, buffer <> bytes)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp recv_text_frame(socket, buffer) do
    case parse_ws_frame(buffer) do
      {:ok, %{opcode: 0x1, payload: payload}, rest} ->
        {payload, rest}

      {:ok, %{opcode: _opcode}, _rest} ->
        raise "expected text frame"

      :more ->
        case :gen_tcp.recv(socket, 0, @ws_timeout) do
          {:ok, bytes} -> recv_text_frame(socket, buffer <> bytes)
          {:error, reason} -> raise "failed to receive websocket frame: #{inspect(reason)}"
        end
    end
  end

  defp parse_ws_frame(<<b1, b2, rest::binary>>) when (b2 &&& 0x80) == 0 and (b2 &&& 0x7F) < 126 do
    length = b2 &&& 0x7F

    if byte_size(rest) < length do
      :more
    else
      <<payload::binary-size(length), tail::binary>> = rest
      {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, tail}
    end
  end

  defp parse_ws_frame(<<b1, 126, length::16, payload::binary-size(length), rest::binary>>) do
    {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, rest}
  end

  defp parse_ws_frame(<<b1, 127, length::64, payload::binary-size(length), rest::binary>>) do
    {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, rest}
  end

  defp parse_ws_frame(_buffer), do: :more

  defp encode_client_text_frame(payload) when is_binary(payload) do
    mask = :crypto.strong_rand_bytes(4)
    payload_size = byte_size(payload)
    masked_payload = mask_payload(payload, mask)
    header = <<0x81>> <> encode_masked_length(payload_size) <> mask
    header <> masked_payload
  end

  defp encode_masked_length(length) when length < 126, do: <<0x80 ||| length>>
  defp encode_masked_length(length) when length < 65_536, do: <<0xFE, length::16>>
  defp encode_masked_length(length), do: <<0xFF, length::64>>

  defp mask_payload(payload, mask) when is_binary(payload) and byte_size(mask) == 4 do
    mask_bytes = :binary.bin_to_list(mask)

    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} -> bxor(byte, Enum.at(mask_bytes, rem(index, 4))) end)
    |> :binary.list_to_bin()
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
