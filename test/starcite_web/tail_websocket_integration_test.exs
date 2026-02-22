defmodule StarciteWeb.TailWebSocketIntegrationTest do
  use ExUnit.Case, async: false

  import Bitwise

  alias Starcite.WritePath

  @host ~c"127.0.0.1"
  @port 4105
  @ws_timeout 2_000

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
    :ok
  end

  defp unique_id(prefix) do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(6), padding: false)
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}-#{suffix}"
  end

  test "tail websocket replays from cursor and streams live committed events" do
    session_id = unique_id("ses")
    {:ok, _} = WritePath.create_session(id: session_id)

    {:ok, _reply} =
      append_event(session_id, %{
        type: "content",
        payload: %{text: "replay"},
        actor: "agent:test"
      })

    {:ok, socket, response_headers, buffer} = connect_tail_ws(session_id, 0)
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

  defp connect_tail_ws(session_id, cursor) do
    {:ok, socket} =
      :gen_tcp.connect(@host, @port, [:binary, active: false, packet: :raw], @ws_timeout)

    key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    request = [
      "GET /v1/sessions/#{session_id}/tail?cursor=#{cursor} HTTP/1.1\r\n",
      "Host: localhost:#{@port}\r\n",
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
end
