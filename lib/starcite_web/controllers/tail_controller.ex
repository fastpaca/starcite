defmodule StarciteWeb.TailController do
  @moduledoc """
  WebSocket tail endpoint for sessions.

  Upgrades `GET /v1/sessions/:id/tail?cursor=N` to a WebSocket stream that:

  1. Replays events where `seq > cursor`
  2. Streams newly committed events in real time
  """

  use StarciteWeb, :controller

  alias Starcite.ReadPath
  alias StarciteWeb.Auth.Policy

  action_fallback StarciteWeb.FallbackController

  @default_tail_frame_batch_size 1
  @max_tail_frame_batch_size 1_000

  plug :ensure_websocket_upgrade_plug when action in [:tail]

  def tail(conn, %{"id" => id} = params) do
    with {:ok, auth} <- fetch_auth(conn),
         {:ok, cursor} <- parse_cursor_param(params),
         {:ok, frame_batch_size} <- parse_frame_batch_size_param(params),
         :ok <- Policy.allowed_to_access_session(auth, id),
         {:ok, session} <- ReadPath.get_session(id),
         :ok <- Policy.allowed_to_read_session(auth, session) do
      conn
      |> WebSockAdapter.upgrade(
        StarciteWeb.TailSocket,
        %{
          session_id: id,
          cursor: cursor,
          frame_batch_size: frame_batch_size,
          auth_expires_at: auth_expires_at(auth)
        },
        timeout: 120_000
      )
      |> halt()
    end
  end

  def tail(_conn, _params), do: {:error, :invalid_session_id}

  defp fetch_auth(%Plug.Conn{assigns: %{auth: auth}}) when is_map(auth), do: {:ok, auth}
  defp fetch_auth(_conn), do: {:error, :unauthorized}

  defp parse_cursor_param(%{"cursor" => cursor}), do: parse_cursor(cursor)
  defp parse_cursor_param(%{}), do: {:ok, 0}

  defp parse_frame_batch_size_param(%{"batch_size" => batch_size}),
    do: parse_frame_batch_size(batch_size)

  defp parse_frame_batch_size_param(%{}), do: {:ok, @default_tail_frame_batch_size}

  defp parse_cursor(nil), do: {:ok, 0}
  defp parse_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: {:ok, cursor}

  defp parse_cursor(cursor) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_cursor}
    end
  end

  defp parse_cursor(_), do: {:error, :invalid_cursor}

  defp parse_frame_batch_size(batch_size)
       when is_integer(batch_size) and batch_size >= @default_tail_frame_batch_size and
              batch_size <= @max_tail_frame_batch_size,
       do: {:ok, batch_size}

  defp parse_frame_batch_size(batch_size) when is_binary(batch_size) do
    case Integer.parse(batch_size) do
      {parsed, ""}
      when parsed >= @default_tail_frame_batch_size and parsed <= @max_tail_frame_batch_size ->
        {:ok, parsed}

      _ ->
        {:error, :invalid_tail_batch_size}
    end
  end

  defp parse_frame_batch_size(_batch_size), do: {:error, :invalid_tail_batch_size}

  defp ensure_websocket_upgrade_plug(conn, _opts) do
    case ensure_websocket_upgrade(conn) do
      :ok ->
        conn

      {:error, reason} ->
        conn
        |> StarciteWeb.FallbackController.call({:error, reason})
        |> halt()
    end
  end

  defp ensure_websocket_upgrade(conn) do
    has_upgrade? =
      conn
      |> Plug.Conn.get_req_header("upgrade")
      |> Enum.any?(fn value -> String.downcase(value) == "websocket" end)

    if has_upgrade?, do: :ok, else: {:error, :invalid_websocket_upgrade}
  end

  defp auth_expires_at(%{expires_at: expires_at})
       when is_integer(expires_at) and expires_at > 0,
       do: expires_at

  defp auth_expires_at(_auth), do: nil
end
