defmodule StarciteWeb.SessionDiscoveryWebSocketIntegrationTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Starcite.AuthTestSupport
  alias Starcite.DataPlane.RaftAccess
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

      :ok = safe_clear_jwks_cache()
    end)

    {:ok, private_key: private_key, kid: kid}
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  test "stream websocket delivers session_created updates for allowed tenant", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    token = token_for(private_key, kid, %{"tenant_id" => "acme", "scopes" => ["session:read"]})

    {:ok, socket, response_headers, buffer} =
      connect_stream_ws(port, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    session_id = unique_id("ses-discovery")
    assert {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {frame, _buffer} = recv_text_frame(socket, buffer)
    update = Jason.decode!(frame)

    assert update["version"] == 1
    assert update["kind"] == "session_created"
    assert update["state"] == "active"
    assert update["session_id"] == session_id
    assert update["tenant_id"] == "acme"
    assert String.ends_with?(update["occurred_at"], "Z")

    :ok = :gen_tcp.close(socket)
  end

  test "stream websocket filters updates by jwt tenant", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    token = token_for(private_key, kid, %{"tenant_id" => "acme", "scopes" => ["session:read"]})

    {:ok, socket, response_headers, buffer} =
      connect_stream_ws(port, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    assert {:ok, _} = WritePath.create_session(id: unique_id("ses-beta"), tenant_id: "beta")
    refute_text_frame(socket, 250)

    session_id = unique_id("ses-acme")
    assert {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    {frame, _buffer} = recv_text_frame(socket, buffer)
    update = Jason.decode!(frame)
    assert update["tenant_id"] == "acme"
    assert update["session_id"] == session_id

    :ok = :gen_tcp.close(socket)
  end

  test "stream websocket enforces jwt session_id lock", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    allowed_session_id = unique_id("ses-allowed")

    token =
      token_for(private_key, kid, %{
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "session_id" => allowed_session_id
      })

    {:ok, socket, response_headers, buffer} =
      connect_stream_ws(port, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    assert {:ok, _} = WritePath.create_session(id: unique_id("ses-other"), tenant_id: "acme")
    refute_text_frame(socket, 250)

    assert {:ok, _} = WritePath.create_session(id: allowed_session_id, tenant_id: "acme")

    {frame, _buffer} = recv_text_frame(socket, buffer)
    update = Jason.decode!(frame)
    assert update["session_id"] == allowed_session_id
    assert update["tenant_id"] == "acme"

    :ok = :gen_tcp.close(socket)
  end

  test "stream websocket closes with token_expired at jwt exp", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    token =
      token_for(private_key, kid, %{
        "tenant_id" => "acme",
        "scopes" => ["session:read"],
        "exp" => System.system_time(:second) + 1
      })

    {:ok, socket, response_headers, buffer} =
      connect_stream_ws(port, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    {{4001, "token_expired"}, _rest} = recv_close_frame(socket, buffer)
    :ok = :gen_tcp.close(socket)
  end

  test "stream websocket delivers frozen and hydrated lifecycle updates", %{
    port: port,
    private_key: private_key,
    kid: kid
  } do
    token = token_for(private_key, kid, %{"tenant_id" => "acme", "scopes" => ["session:read"]})

    {:ok, socket, response_headers, buffer} =
      connect_stream_ws(port, [], %{"access_token" => token})

    assert String.starts_with?(response_headers, "HTTP/1.1 101")

    session_id = unique_id("ses-freeze-hydrate")
    assert {:ok, _} = WritePath.create_session(id: session_id, tenant_id: "acme")

    # Initial create should be visible to discovery subscribers.
    {created_frame, buffer} = recv_text_frame(socket, buffer)
    created_update = Jason.decode!(created_frame)
    assert created_update["kind"] == "session_created"
    assert created_update["session_id"] == session_id

    {:ok, server_id, _group} = RaftAccess.locate_and_ensure_started(session_id)
    {:ok, session} = RaftAccess.query_session(server_id, session_id)
    snapshot = archive_session_snapshot(session)

    assert {:ok, {:reply, {:ok, %{id: ^session_id}}}, _leader} =
             :ra.process_command(
               {server_id, Node.self()},
               {:freeze_session, session_id, session.last_seq, session.archived_seq,
                session.last_progress_poll},
               2_000
             )

    {frozen_frame, buffer} = recv_text_frame(socket, buffer)
    frozen_update = Jason.decode!(frozen_frame)
    assert frozen_update["kind"] == "session_frozen"
    assert frozen_update["state"] == "frozen"
    assert frozen_update["session_id"] == session_id

    assert {:ok, {:reply, {:ok, :hydrated}}, _leader} =
             :ra.process_command(
               {server_id, Node.self()},
               {:hydrate_session, snapshot},
               2_000
             )

    {hydrated_frame, _buffer} = recv_text_frame(socket, buffer)
    hydrated_update = Jason.decode!(hydrated_frame)
    assert hydrated_update["kind"] == "session_hydrated"
    assert hydrated_update["state"] == "active"
    assert hydrated_update["session_id"] == session_id

    :ok = :gen_tcp.close(socket)
  end

  defp connect_stream_ws(port, headers, query_params)
       when is_integer(port) and port > 0 and is_list(headers) and is_map(query_params) do
    {:ok, socket} =
      :gen_tcp.connect(@host, port, [:binary, active: false, packet: :raw], @ws_timeout)

    key = :crypto.strong_rand_bytes(16) |> Base.encode64()
    query = URI.encode_query(query_params)

    request = [
      "GET /v1/sessions/stream?#{query} HTTP/1.1\r\n",
      "Host: localhost:#{port}\r\n",
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

  defp refute_text_frame(socket, timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    case :gen_tcp.recv(socket, 0, timeout_ms) do
      {:error, :timeout} ->
        :ok

      {:ok, bytes} ->
        raise "expected no websocket frame, received: #{inspect(parse_ws_frame(bytes))}"

      {:error, reason} ->
        raise "expected timeout while waiting for frame, got: #{inspect(reason)}"
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

  defp archive_session_snapshot(session) when is_map(session) do
    %{
      id: session.id,
      title: session.title,
      tenant_id: session.tenant_id,
      creator_principal: session.creator_principal,
      metadata: session.metadata,
      created_at: DateTime.from_naive!(session.inserted_at, "Etc/UTC"),
      last_seq: session.last_seq,
      archived_seq: session.archived_seq,
      retention: session.retention,
      producer_cursors: session.producer_cursors,
      last_progress_poll: session.last_progress_poll,
      snapshot_version: "v1"
    }
  end

  defp pick_available_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, {:active, false}, {:ip, {127, 0, 0, 1}}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp safe_clear_jwks_cache do
    try do
      JWKS.clear_cache()
    rescue
      ArgumentError -> :ok
    end
  end
end
