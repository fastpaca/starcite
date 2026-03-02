defmodule StarciteWeb.SessionDiscoveryController do
  @moduledoc """
  Service-level WebSocket stream for session lifecycle discovery.
  """

  use StarciteWeb, :controller

  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Auth.Policy

  action_fallback StarciteWeb.FallbackController

  plug :ensure_websocket_upgrade_plug when action in [:stream]

  def stream(conn, _params) do
    with {:ok, auth} <- fetch_auth(conn),
         :ok <- Policy.can_list_sessions(auth) do
      conn
      |> WebSockAdapter.upgrade(
        StarciteWeb.SessionDiscoverySocket,
        %{auth_context: auth},
        timeout: 120_000
      )
      |> halt()
    end
  end

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
