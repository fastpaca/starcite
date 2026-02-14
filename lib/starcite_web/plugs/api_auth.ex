defmodule StarciteWeb.Plugs.ApiAuth do
  @moduledoc """
  API authentication boundary for Starcite.

  In `:none` mode this plug is a no-op.
  In `:jwt` mode it validates bearer JWTs using the configured JWKS.
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  require Logger

  alias StarciteWeb.Auth

  @impl true
  def init(opts), do: opts

  @impl true
  def call(conn, _opts) do
    with {:ok, auth_context} <- Auth.authenticate_conn(conn) do
      assign(conn, :auth, auth_context)
    else
      {:error, reason} ->
        Logger.debug("API auth rejected request: #{inspect(reason)}")

        conn
        |> put_resp_header("www-authenticate", unauthorized_header(reason))
        |> put_status(:unauthorized)
        |> json(%{error: "unauthorized", message: "Unauthorized"})
        |> halt()
    end
  end

  defp unauthorized_header(:missing_bearer_token) do
    ~s(Bearer realm="starcite")
  end

  defp unauthorized_header(:invalid_bearer_token) do
    ~s(Bearer realm="starcite", error="invalid_request", error_description="Malformed bearer token")
  end

  defp unauthorized_header(_reason) do
    ~s(Bearer realm="starcite", error="invalid_token")
  end
end
