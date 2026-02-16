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
             auth_result <- authenticate_principal_token(token) do
          service_reason = conn.assigns[:service_auth_error]
          merge_auth_result(token, service_reason, auth_result)
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
               session_ids: principal_context.session_ids,
               owner_principal_ids: principal_context.owner_principal_ids
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
            case authenticate_principal_token(token) do
              {:ok, principal_context} ->
                {:ok, %{principal_context | bearer_token: token}}

              {:error, principal_reason} ->
                {:error, choose_auth_error(service_reason, principal_reason)}
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

  defp choose_auth_error(:token_expired, _principal_reason), do: :token_expired
  defp choose_auth_error(_jwt_reason, :token_expired), do: :token_expired
  defp choose_auth_error(jwt_reason, _principal_reason), do: jwt_reason

  defp merge_auth_result(_token, _service_reason, {:error, :token_expired}),
    do: {:error, :token_expired}

  defp merge_auth_result(token, _service_reason, {:ok, principal_context}) do
    {:ok, %{principal_context | bearer_token: token}}
  end

  defp merge_auth_result(_token, nil, {:error, principal_reason}),
    do: {:error, principal_reason}

  defp merge_auth_result(_token, service_reason, {:error, principal_reason})
       when is_atom(service_reason) and is_atom(principal_reason) do
    {:error, choose_auth_error(service_reason, principal_reason)}
  end
end
