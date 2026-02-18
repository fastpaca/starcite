defmodule StarciteWeb.Plugs.PrincipalAuth do
  @moduledoc """
  Principal-aware authentication plug.

  This runs after service auth. If service auth already succeeded, it is a no-op.
  Otherwise it validates the bearer token and accepts either:

  - a valid service JWT
  - a valid Starcite principal token
  """

  @behaviour Plug

  import Phoenix.Controller, only: [json: 2]
  import Plug.Conn

  require Logger

  alias StarciteWeb.Auth.PrincipalToken
  alias StarciteWeb.Plugs.ServiceAuth

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{assigns: %{auth: _auth_context}} = conn, _opts), do: conn

  def call(conn, _opts) do
    case authenticate_principal_conn(conn) do
      {:ok, auth_context} ->
        assign(conn, :auth, auth_context)

      {:error, reason} ->
        Logger.debug("Principal auth rejected request: #{inspect(reason)}")
        unauthorized(conn, reason)
    end
  end

  @spec authenticate_principal_conn(Plug.Conn.t()) :: {:ok, map()} | {:error, atom()}
  def authenticate_principal_conn(conn) do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %{kind: :none}}

      :jwt ->
        with {:ok, token} <- ServiceAuth.bearer_token(conn),
             {:ok, principal_context} <- authenticate_principal_token(token) do
          {:ok, %{principal_context | bearer_token: token}}
        else
          {:error, principal_reason} ->
            case conn.assigns do
              %{service_auth_error: service_reason} ->
                {:error, auth_error(service_reason, principal_reason)}

              _ ->
                {:error, principal_reason}
            end
        end
    end
  end

  @spec authenticate_principal_token(String.t()) :: {:ok, map()} | {:error, atom()}
  def authenticate_principal_token(token) when is_binary(token) and token != "" do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %{kind: :none}}

      :jwt ->
        case PrincipalToken.verify(token, ServiceAuth.config()) do
          {:ok, principal_context} ->
            {:ok,
             %{
               kind: :principal,
               claims: principal_context.claims,
               expires_at: principal_context.expires_at,
               bearer_token: nil,
               principal: principal_context.principal,
               scopes: principal_context.scopes,
               session_ids: principal_context.session_ids
             }}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def authenticate_principal_token(_token), do: {:error, :invalid_bearer_token}

  @spec authenticate_token(String.t()) :: {:ok, map()} | {:error, atom()}
  def authenticate_token(token) when is_binary(token) and token != "" do
    case ServiceAuth.mode() do
      :none ->
        {:ok, %{kind: :none}}

      :jwt ->
        case ServiceAuth.authenticate_service_token(token) do
          {:ok, auth_context} ->
            {:ok, auth_context}

          {:error, service_reason} ->
            with {:ok, principal_context} <- authenticate_principal_token(token) do
              {:ok, %{principal_context | bearer_token: token}}
            else
              {:error, principal_reason} ->
                {:error, auth_error(service_reason, principal_reason)}
            end
        end
    end
  end

  def authenticate_token(_token), do: {:error, :invalid_bearer_token}

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

  defp auth_error(:token_expired, _principal_reason), do: :token_expired
  defp auth_error(_service_reason, :token_expired), do: :token_expired
  defp auth_error(service_reason, _principal_reason), do: service_reason
end
