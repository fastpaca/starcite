defmodule StarciteWeb.Plugs.RedactSensitiveQuery do
  @moduledoc """
  Redacts sensitive query parameters early in the endpoint pipeline.

  For browser WebSocket auth, preserves the raw `access_token` in connection private
  assigns so downstream auth can still validate the JWT while logs and telemetry
  observe a redacted query string.
  """

  @behaviour Plug

  import Plug.Conn, only: [put_private: 3]

  alias Plug.Conn

  @ws_access_token_private_key :starcite_ws_access_token
  @redacted_value "[REDACTED]"

  @spec ws_access_token_private_key() :: atom()
  def ws_access_token_private_key, do: @ws_access_token_private_key

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Conn{query_string: query_string} = conn, _opts) when is_binary(query_string) do
    case URI.decode_query(query_string) do
      %{"access_token" => access_token} = params when is_binary(access_token) ->
        conn
        |> put_private(@ws_access_token_private_key, access_token)
        |> put_redacted_query_string(params)

      _params ->
        conn
    end
  rescue
    _error ->
      conn
  end

  defp put_redacted_query_string(conn, params) when is_map(params) do
    %{conn | query_string: URI.encode_query(Map.put(params, "access_token", @redacted_value))}
  end
end
