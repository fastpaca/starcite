defmodule StarciteWeb.TailWebSocketIntegrationTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Starcite.AuthTestSupport
  alias Starcite.WritePath
  alias StarciteWeb.Auth.JWKS

  @auth_env_key StarciteWeb.Auth
  @host ~c"127.0.0.1"
  @port 4105
  @ws_timeout 2_500
  @issuer "https://issuer.example"
  @audience "starcite-api"
  @jwks_path "/.well-known/jwks.json"

  setup_all do
    {:ok, _pid} =
      start_supervised(
        {Bandit,
         plug: StarciteWeb.Endpoint,
         scheme: :http,
         ip: {127, 0, 0, 1},
         port: @port,
         startup_log: false}
      )

    :ok
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
      connect_tail_ws(session_id, 0, [], %{"access_token" => token})

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
    private_key: private_key,
    kid: kid
  } do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id, metadata: %{"tenant_id" => "acme"})

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
      connect_tail_ws(session_id, 0, [], %{"access_token" => token, "batch_size" => "2"})

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
      connect_tail_ws(session_id, 0, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    {{4001, "token_expired"}, _rest} = recv_close_frame(socket, buffer)
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

  defp connect_tail_ws(session_id, cursor, headers, query_params)
       when is_binary(session_id) and is_integer(cursor) and cursor >= 0 and is_list(headers) and
              is_map(query_params) do
    {:ok, socket} =
      :gen_tcp.connect(@host, @port, [:binary, active: false, packet: :raw], @ws_timeout)

    key = :crypto.strong_rand_bytes(16) |> Base.encode64()
    query = build_tail_query(cursor, query_params)

    request = [
      "GET /v1/sessions/#{session_id}/tail?#{query} HTTP/1.1\r\n",
      "Host: localhost:#{@port}\r\n",
      "Connection: Upgrade\r\n",
      "Upgrade: websocket\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: #{key}\r\n",
      Enum.map(headers, fn {name, value} -> "#{name}: #{value}\r\n" end),
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

  defp build_tail_query(cursor, query_params)
       when is_integer(cursor) and cursor >= 0 and is_map(query_params) do
    query_params
    |> Map.put("cursor", Integer.to_string(cursor))
    |> URI.encode_query()
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

  defp recv_close_frame(socket, buffer) do
    case parse_ws_frame(buffer) do
      {:ok, %{opcode: 0x8, payload: payload}, rest} ->
        {decode_close_payload(payload), rest}

      {:ok, %{opcode: _opcode}, _rest} ->
        raise "expected close frame"

      :more ->
        case :gen_tcp.recv(socket, 0, @ws_timeout) do
          {:ok, bytes} -> recv_close_frame(socket, buffer <> bytes)
          {:error, reason} -> raise "failed to receive websocket close frame: #{inspect(reason)}"
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

  defp decode_close_payload(<<code::16, reason::binary>>), do: {code, reason}
  defp decode_close_payload(<<code::16>>), do: {code, ""}
  defp decode_close_payload(_payload), do: {1006, "invalid_close_payload"}

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
end
