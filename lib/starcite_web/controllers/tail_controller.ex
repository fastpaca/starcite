defmodule StarciteWeb.TailController do
  @moduledoc """
  WebSocket tail endpoint for sessions.

  Upgrades `GET /v1/sessions/:id/tail?cursor=N` to a WebSocket stream that:

  1. Replays events where `seq > cursor`
  2. Streams newly committed events in real time
  """

  use StarciteWeb, :controller

  alias Starcite.Runtime

  action_fallback StarciteWeb.FallbackController

  def tail(conn, %{"id" => id} = params) do
    with :ok <- ensure_websocket_upgrade(conn),
         {:ok, cursor} <- parse_cursor(Map.get(params, "cursor")),
         {:ok, _session} <- Runtime.get_session(id) do
      conn
      |> WebSockAdapter.upgrade(StarciteWeb.TailSocket, %{session_id: id, cursor: cursor},
        timeout: 120_000
      )
      |> halt()
    end
  end

  def tail(_conn, _params), do: {:error, :invalid_session_id}

  defp parse_cursor(nil), do: {:ok, 0}
  defp parse_cursor(cursor) when is_integer(cursor) and cursor >= 0, do: {:ok, cursor}

  defp parse_cursor(cursor) when is_binary(cursor) do
    case Integer.parse(cursor) do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _ -> {:error, :invalid_cursor}
    end
  end

  defp parse_cursor(_), do: {:error, :invalid_cursor}

  defp ensure_websocket_upgrade(conn) do
    has_upgrade? =
      conn
      |> Plug.Conn.get_req_header("upgrade")
      |> Enum.any?(fn value -> String.downcase(value) == "websocket" end)

    if has_upgrade?, do: :ok, else: {:error, :invalid_websocket_upgrade}
  end
end
