defmodule StarciteWeb.AuthTokenController do
  @moduledoc """
  Issues short-lived Starcite principal tokens using service credentials.
  """

  use StarciteWeb, :controller

  alias StarciteWeb.Auth.{Policy, PrincipalToken}
  alias StarciteWeb.Plugs.ServiceAuth

  action_fallback StarciteWeb.FallbackController

  @doc """
  Issue a short-lived principal token.
  """
  def issue(
        conn,
        %{
          "principal" => %{
            "tenant_id" => _tenant_id,
            "id" => _principal_id,
            "type" => _principal_type
          },
          "scopes" => _scopes
        } = params
      ) do
    with {:ok, auth} <- fetch_auth(conn),
         :ok <- Policy.can_issue_token(auth, params),
         {:ok, issued} <- PrincipalToken.issue(params, ServiceAuth.config()) do
      principal = issued.principal

      json(conn, %{
        token: issued.token,
        token_type: "Bearer",
        expires_at: issued.expires_at,
        expires_in: issued.expires_in,
        principal: %{
          tenant_id: principal.tenant_id,
          id: principal.id,
          type: Atom.to_string(principal.type)
        },
        scopes: issued.scopes,
        session_ids: issued.session_ids
      })
    end
  end

  def issue(_conn, _params), do: {:error, :invalid_issue_request}

  defp fetch_auth(%Plug.Conn{assigns: %{auth: auth}}) when is_map(auth), do: {:ok, auth}
  defp fetch_auth(_conn), do: {:error, :unauthorized}
end
