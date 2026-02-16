defmodule StarciteWeb.AuthTokenController do
  @moduledoc """
  Issues short-lived Starcite principal tokens using service credentials.
  """

  use StarciteWeb, :controller

  alias StarciteWeb.Auth.Context
  alias StarciteWeb.Plugs.{PrincipalAuth, ServiceAuth}

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
    auth_context = conn.assigns[:auth] || %Context{}

    # Token issuance is service-only. Principals are not allowed to mint other principals.
    with :ok <- ServiceAuth.require_service(auth_context),
         {:ok, token} <- PrincipalAuth.issue_principal_token(auth_context, params) do
      json(conn, token)
    end
  end

  def issue(_conn, _params), do: {:error, :invalid_issue_request}
end
