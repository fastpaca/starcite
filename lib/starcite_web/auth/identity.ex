defmodule StarciteWeb.Auth.Identity do
  @moduledoc false

  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Context

  @spec from_jwt_claims(map()) :: {:ok, Context.t()} | {:error, :invalid_jwt_claims}
  def from_jwt_claims(claims) when is_map(claims) do
    with {:ok, exp} <- claim_exp(claims),
         {:ok, tenant_id} <- claim_tenant_id(claims),
         {:ok, scopes} <- claim_scopes(claims),
         {:ok, session_id} <- claim_session_id(claims),
         {:ok, principal} <- claim_principal(claims, tenant_id) do
      {:ok,
       %Context{
         kind: :jwt,
         principal: principal,
         scopes: scopes,
         session_id: session_id,
         expires_at: exp
       }}
    end
  end

  def from_jwt_claims(_claims), do: {:error, :invalid_jwt_claims}

  defp claim_exp(%{"exp" => exp}) when is_integer(exp) and exp >= 0, do: {:ok, exp}
  defp claim_exp(_claims), do: {:error, :invalid_jwt_claims}

  defp claim_tenant_id(%{"tenant_id" => tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: {:ok, tenant_id}

  defp claim_tenant_id(_claims), do: {:error, :invalid_jwt_claims}

  defp claim_scopes(claims) when is_map(claims) do
    with {:ok, scope_values} <- scope_values(Map.get(claims, "scope")),
         {:ok, scopes_values} <- scopes_values(Map.get(claims, "scopes")) do
      scopes = Enum.uniq(scope_values ++ scopes_values)
      if scopes == [], do: {:error, :invalid_jwt_claims}, else: {:ok, scopes}
    end
  end

  defp claim_session_id(%{"session_id" => session_id})
       when is_binary(session_id) and session_id != "",
       do: {:ok, session_id}

  defp claim_session_id(%{"session_id" => nil}), do: {:ok, nil}
  defp claim_session_id(%{"session_id" => _invalid}), do: {:error, :invalid_jwt_claims}
  defp claim_session_id(_claims), do: {:ok, nil}

  defp claim_principal(%{"sub" => subject}, tenant_id)
       when is_binary(subject) and subject != "" and is_binary(tenant_id) and tenant_id != "" do
    with [principal_type, principal_id] <- String.split(subject, ":", parts: 2),
         true <- principal_type != "" and principal_id != "",
         {:ok, type} <- principal_type_from_subject(principal_type),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, type) do
      {:ok, principal}
    else
      _other -> {:error, :invalid_jwt_claims}
    end
  end

  defp claim_principal(_claims, _tenant_id), do: {:error, :invalid_jwt_claims}

  defp principal_type_from_subject("user"), do: {:ok, :user}
  defp principal_type_from_subject("agent"), do: {:ok, :agent}
  defp principal_type_from_subject("service"), do: {:ok, :service}
  defp principal_type_from_subject("svc"), do: {:ok, :service}
  defp principal_type_from_subject("org"), do: {:ok, :service}
  defp principal_type_from_subject(_principal_type), do: {:error, :invalid_jwt_claims}

  defp scope_values(nil), do: {:ok, []}

  defp scope_values(scope) when is_binary(scope) do
    {:ok, String.split(scope, ~r/\s+/, trim: true)}
  end

  defp scope_values(_scope), do: {:error, :invalid_jwt_claims}

  defp scopes_values(nil), do: {:ok, []}

  defp scopes_values(scopes) when is_list(scopes) do
    normalized = Enum.uniq(scopes)

    if Enum.all?(normalized, &is_non_empty_string/1) do
      {:ok, normalized}
    else
      {:error, :invalid_jwt_claims}
    end
  end

  defp scopes_values(_scopes), do: {:error, :invalid_jwt_claims}

  defp is_non_empty_string(value), do: is_binary(value) and value != ""
end
