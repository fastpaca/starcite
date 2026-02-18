defmodule StarciteWeb.Plugs.ServiceAuth do
  @moduledoc """
  Service-level authentication plug.

  In optional mode it attempts service auth and leaves auth unset on failure so
  principal auth can run. In required mode it rejects non-service credentials.
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  require Logger

  alias Plug.Conn
  alias StarciteWeb.Auth.{Config, JWT}

  @impl true
  def init(opts) do
    %{required: Keyword.get(opts, :required, true)}
  end

  @impl true
  def call(conn, %{required: required}) do
    case authenticate_service_conn(conn) do
      {:ok, auth_context} ->
        assign(conn, :auth, auth_context)

      {:error, reason} ->
        if required do
          Logger.debug("Service auth rejected request: #{inspect(reason)}")
          unauthorized(conn, reason)
        else
          assign(conn, :service_auth_error, reason)
        end
    end
  end

  @spec mode() :: :none | :jwt
  def mode do
    config().mode
  end

  @spec config() :: Config.t()
  def config do
    Config.load()
  end

  @spec authenticate_service_conn(Conn.t()) :: {:ok, map()} | {:error, atom()}
  def authenticate_service_conn(%Conn{} = conn) do
    case mode() do
      :none ->
        {:ok, %{kind: :none}}

      :jwt ->
        with {:ok, token} <- bearer_token(conn),
             {:ok, auth_context} <- authenticate_service_token(token) do
          {:ok, auth_context}
        end
    end
  end

  @spec authenticate_service_token(String.t()) :: {:ok, map()} | {:error, atom()}
  def authenticate_service_token(token) when is_binary(token) and token != "" do
    case mode() do
      :none ->
        {:ok, %{kind: :none}}

      :jwt ->
        config = config()

        with {:ok, claims} <- JWT.verify(token, config),
             {:ok, exp} <- claim_exp(claims) do
          {:ok, %{kind: :service, claims: claims, expires_at: exp, bearer_token: token}}
        end
    end
  end

  def authenticate_service_token(_token), do: {:error, :invalid_bearer_token}

  @spec bearer_token(Conn.t()) :: {:ok, String.t()} | {:error, atom()}
  def bearer_token(%Conn{} = conn) do
    case Conn.get_req_header(conn, "authorization") do
      [header] ->
        parse_bearer_token(header)

      [] ->
        {:error, :missing_bearer_token}

      _many ->
        {:error, :invalid_bearer_token}
    end
  end

  defp unauthorized(conn, reason) do
    conn
    |> put_resp_header("www-authenticate", unauthorized_header(reason))
    |> put_status(:unauthorized)
    |> json(%{error: "unauthorized", message: "Unauthorized"})
    |> halt()
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

  defp claim_exp(%{"exp" => exp}) when is_integer(exp) and exp >= 0, do: {:ok, exp}
  defp claim_exp(_claims), do: {:error, :invalid_jwt_claims}

  defp parse_bearer_token(header) when is_binary(header) do
    with [scheme, token] <- String.split(String.trim(header), " ", parts: 2, trim: true),
         true <- String.downcase(scheme) == "bearer",
         true <- token != "",
         false <- String.contains?(token, ","),
         false <- String.contains?(token, " ") do
      {:ok, token}
    else
      _ -> {:error, :invalid_bearer_token}
    end
  end
end
