defmodule Starcite.WebSocketTestClient do
  @moduledoc false

  import Bitwise

  @host ~c"127.0.0.1"
  @default_timeout 2_500

  @spec connect(non_neg_integer(), String.t(), [{String.t(), String.t()}]) ::
          {:ok, port(), binary(), binary()} | {:error, term()}
  def connect(port, path, headers \\ [])
      when is_integer(port) and port > 0 and is_binary(path) and is_list(headers) do
    with {:ok, socket} <-
           :gen_tcp.connect(@host, port, [:binary, active: false, packet: :raw], @default_timeout),
         :ok <- :gen_tcp.send(socket, handshake_request(port, path, headers)),
         {:ok, response_headers, buffer} <- recv_until_headers(socket, <<>>) do
      {:ok, socket, response_headers, buffer}
    else
      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec send_text_frame(port(), iodata()) :: :ok | {:error, term()}
  def send_text_frame(socket, payload), do: send_frame(socket, 0x1, IO.iodata_to_binary(payload))

  @spec recv_text_frame(port(), binary()) :: {binary(), binary()}
  def recv_text_frame(socket, buffer) do
    case parse_server_frame(buffer) do
      {:ok, %{opcode: 0x1, payload: payload}, rest} ->
        {payload, rest}

      {:ok, %{opcode: opcode}, _rest} ->
        raise "expected text frame, got opcode #{opcode}"

      :more ->
        case :gen_tcp.recv(socket, 0, @default_timeout) do
          {:ok, bytes} -> recv_text_frame(socket, buffer <> bytes)
          {:error, reason} -> raise "failed to receive websocket frame: #{inspect(reason)}"
        end
    end
  end

  @spec recv_close_frame(port(), binary()) :: {{non_neg_integer(), binary()}, binary()}
  def recv_close_frame(socket, buffer) do
    case parse_server_frame(buffer) do
      {:ok, %{opcode: 0x8, payload: payload}, rest} ->
        {decode_close_payload(payload), rest}

      {:ok, %{opcode: opcode}, _rest} ->
        raise "expected close frame, got opcode #{opcode}"

      :more ->
        case :gen_tcp.recv(socket, 0, @default_timeout) do
          {:ok, bytes} -> recv_close_frame(socket, buffer <> bytes)
          {:error, reason} -> raise "failed to receive websocket close frame: #{inspect(reason)}"
        end
    end
  end

  defp handshake_request(port, path, headers)
       when is_integer(port) and is_binary(path) and is_list(headers) do
    key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    [
      "GET ",
      path,
      " HTTP/1.1\r\n",
      "Host: localhost:",
      Integer.to_string(port),
      "\r\n",
      "Connection: Upgrade\r\n",
      "Upgrade: websocket\r\n",
      "Sec-WebSocket-Version: 13\r\n",
      "Sec-WebSocket-Key: ",
      key,
      "\r\n",
      Enum.map(headers, fn {name, value} -> [name, ": ", value, "\r\n"] end),
      "\r\n"
    ]
  end

  defp recv_until_headers(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, _len} ->
        header_bytes = binary_part(buffer, 0, index + 4)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, header_bytes, rest}

      :nomatch ->
        case :gen_tcp.recv(socket, 0, @default_timeout) do
          {:ok, bytes} -> recv_until_headers(socket, buffer <> bytes)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp send_frame(socket, opcode, payload)
       when is_integer(opcode) and is_binary(payload) do
    :gen_tcp.send(socket, encode_client_frame(opcode, payload))
  end

  defp encode_client_frame(opcode, payload)
       when is_integer(opcode) and is_binary(payload) do
    fin_and_opcode = 0x80 ||| opcode
    payload_length = byte_size(payload)
    mask = :crypto.strong_rand_bytes(4)
    masked_payload = mask_payload(payload, mask)

    case payload_length do
      len when len < 126 ->
        <<fin_and_opcode, 0x80 ||| len, mask::binary, masked_payload::binary>>

      len when len <= 0xFFFF ->
        <<fin_and_opcode, 0x80 ||| 126, len::16, mask::binary, masked_payload::binary>>

      len ->
        <<fin_and_opcode, 0x80 ||| 127, len::64, mask::binary, masked_payload::binary>>
    end
  end

  defp mask_payload(payload, <<m1, m2, m3, m4>>) when is_binary(payload) do
    payload
    |> :binary.bin_to_list()
    |> Enum.with_index()
    |> Enum.map(fn {byte, index} ->
      mask_byte =
        case rem(index, 4) do
          0 -> m1
          1 -> m2
          2 -> m3
          3 -> m4
        end

      Bitwise.bxor(byte, mask_byte)
    end)
    |> :erlang.list_to_binary()
  end

  defp parse_server_frame(<<b1, b2, rest::binary>>)
       when (b2 &&& 0x80) == 0 and (b2 &&& 0x7F) < 126 do
    payload_length = b2 &&& 0x7F

    if byte_size(rest) < payload_length do
      :more
    else
      <<payload::binary-size(payload_length), tail::binary>> = rest
      {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, tail}
    end
  end

  defp parse_server_frame(
         <<b1, 126, payload_length::16, payload::binary-size(payload_length), rest::binary>>
       ) do
    {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, rest}
  end

  defp parse_server_frame(
         <<b1, 127, payload_length::64, payload::binary-size(payload_length), rest::binary>>
       ) do
    {:ok, %{opcode: b1 &&& 0x0F, payload: payload}, rest}
  end

  defp parse_server_frame(_buffer), do: :more

  defp decode_close_payload(<<code::16, reason::binary>>), do: {code, reason}
  defp decode_close_payload(<<code::16>>), do: {code, ""}
  defp decode_close_payload(_payload), do: {1006, "invalid_close_payload"}
end
