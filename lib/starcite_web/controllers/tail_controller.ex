defmodule StarciteWeb.TailController do
  @moduledoc """
  WebSocket tail endpoint for sessions.

  Upgrades `GET /v1/sessions/:id/tail?cursor=N` to a WebSocket stream that:

  1. Replays events where `seq > cursor`
  2. Streams newly committed events in real time
  """

  use StarciteWeb, :controller

  alias StarciteWeb.Auth.Context
  alias StarciteWeb.{TailAccess, TailParams}

  action_fallback StarciteWeb.FallbackController

  plug :ensure_websocket_upgrade_plug when action in [:tail]

  def tail(conn, %{"id" => id} = params) do
    with {:ok, auth} <- fetch_auth(conn),
         {:ok, %{cursor: cursor, frame_batch_size: frame_batch_size}} <- TailParams.parse(params),
         {:ok, _session} <- TailAccess.authorize_read(auth, id) do
      conn
      |> WebSockAdapter.upgrade(
        StarciteWeb.TailSocket,
        %{
          session_id: id,
          cursor: cursor,
          frame_batch_size: frame_batch_size,
          auth_context: auth
        },
        timeout: 120_000
      )
      |> halt()
    end
  end

  def tail(_conn, _params), do: {:error, :invalid_session_id}

  defp fetch_auth(%Plug.Conn{assigns: %{auth: %Context{} = auth}}), do: {:ok, auth}
  defp fetch_auth(_conn), do: {:error, :unauthorized}

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
end
