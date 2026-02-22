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
  alias StarciteWeb.Plugs.ServiceAuth

  action_fallback StarciteWeb.FallbackController

  plug :ensure_websocket_upgrade_plug when action in [:tail]

  def tail(conn, %{"id" => id} = params) do
    auth = conn.assigns[:auth] || %{kind: :none}

    with {:ok, cursor} <- parse_cursor_param(params),
         :ok <- Policy.allowed_to_access_session(auth, id),
         {:ok, session} <- ReadPath.get_session(id),
         :ok <- Policy.allowed_to_read_session(auth, session) do
      conn
      |> WebSockAdapter.upgrade(
        StarciteWeb.TailSocket,
        %{
          session_id: id,
          cursor: cursor,
          auth_bearer_token: auth_bearer_token(auth),
          auth_expires_at: auth_expires_at(auth),
          auth_check_interval_ms: auth_check_interval_ms()
        },
        timeout: 120_000
      )
      |> halt()
    end
  end

  def tail(_conn, _params), do: {:error, :invalid_session_id}

  defp parse_cursor_param(%{"cursor" => cursor}), do: parse_cursor(cursor)
  defp parse_cursor_param(%{}), do: {:ok, 0}

  defp parse_cursor(nil), do: {:ok, 0}
  defp parse_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: {:ok, cursor}

  defp parse_cursor(cursor) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_cursor}
    end
  end

  defp parse_cursor(_), do: {:error, :invalid_cursor}

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

  defp auth_bearer_token(%{bearer_token: bearer_token})
       when is_binary(bearer_token) and bearer_token != "",
       do: bearer_token

  defp auth_bearer_token(_auth), do: nil

  defp auth_check_interval_ms do
    config = ServiceAuth.config()
    config.jwks_refresh_ms
  end
end
