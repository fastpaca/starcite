defmodule StarciteWeb.Auth.PrincipalToken do
  @moduledoc """
  Issues and verifies short-lived principal tokens.

  Security contract:

  - principal identity is mandatory and explicit
  - actor is derived from principal identity
  - agent principals are always session-scoped
  - invalid shapes fail closed
  """

  alias Starcite.Auth.Principal

  @claims_version 1

  @type issued :: %{
          token: String.t(),
          expires_at: pos_integer(),
          expires_in: pos_integer(),
          principal: Principal.t(),
          scopes: [String.t()],
          session_ids: [String.t()] | nil
        }

  @type verified :: %{
          claims: map(),
          expires_at: pos_integer(),
          principal: Principal.t(),
          scopes: [String.t()],
          session_ids: [String.t()] | nil
        }

  @type issue_attrs :: map()

  @spec issue(issue_attrs(), map()) :: {:ok, issued()} | {:error, :invalid_issue_request}
  def issue(
        %{"principal" => principal_attrs, "scopes" => scopes} = attrs,
        %{
          principal_token_salt: principal_token_salt,
          principal_token_default_ttl_seconds: default_ttl_seconds,
          principal_token_max_ttl_seconds: max_ttl_seconds,
          jwt_leeway_seconds: _jwt_leeway_seconds
        }
      )
      when is_binary(principal_token_salt) and principal_token_salt != "" and
             is_integer(default_ttl_seconds) and default_ttl_seconds > 0 and
             is_integer(max_ttl_seconds) and max_ttl_seconds > 0 do
    now = System.system_time(:second)

    with {:ok, principal} <- principal_from_attrs(principal_attrs, :invalid_issue_request),
         {:ok, normalized_scopes} <- normalize_string_list(scopes, :invalid_issue_request),
         {:ok, session_ids} <-
           normalize_optional_string_list(attrs["session_ids"], :invalid_issue_request),
         :ok <- ensure_session_binding(principal.type, session_ids, :invalid_issue_request),
         {:ok, ttl_seconds} <-
           ttl_seconds(attrs["ttl_seconds"], default_ttl_seconds, max_ttl_seconds) do
      expires_at = now + ttl_seconds

      claims =
        %{
          "v" => @claims_version,
          "typ" => "principal",
          "tenant_id" => principal.tenant_id,
          "sub" => principal.id,
          "principal_type" => Atom.to_string(principal.type),
          "actor" => Principal.actor(principal),
          "scopes" => normalized_scopes,
          "session_ids" => session_ids,
          "iat" => now,
          "exp" => expires_at
        }
        |> drop_nil_claims()

      token = Phoenix.Token.sign(StarciteWeb.Endpoint, principal_token_salt, claims)

      {:ok,
       %{
         token: token,
         expires_at: expires_at,
         expires_in: ttl_seconds,
         principal: principal,
         scopes: normalized_scopes,
         session_ids: session_ids
       }}
    else
      _ -> {:error, :invalid_issue_request}
    end
  end

  def issue(_attrs, _config), do: {:error, :invalid_issue_request}

  @spec verify(String.t(), map()) ::
          {:ok, verified()} | {:error, :invalid_bearer_token | :token_expired}
  def verify(
        token,
        %{
          principal_token_salt: principal_token_salt,
          principal_token_max_ttl_seconds: max_ttl_seconds,
          jwt_leeway_seconds: leeway_seconds
        }
      )
      when is_binary(token) and token != "" and is_binary(principal_token_salt) and
             principal_token_salt != "" and is_integer(max_ttl_seconds) and max_ttl_seconds > 0 and
             is_integer(leeway_seconds) and leeway_seconds >= 0 do
    max_age = max_ttl_seconds + leeway_seconds

    case Phoenix.Token.verify(StarciteWeb.Endpoint, principal_token_salt, token, max_age: max_age) do
      {:ok, claims} ->
        case verify_claims(claims, leeway_seconds) do
          {:ok, verified} -> {:ok, verified}
          {:error, :token_expired} -> {:error, :token_expired}
          {:error, _reason} -> {:error, :invalid_bearer_token}
        end

      {:error, :expired} ->
        {:error, :token_expired}

      {:error, _reason} ->
        {:error, :invalid_bearer_token}
    end
  end

  def verify(_token, _config), do: {:error, :invalid_bearer_token}

  defp verify_claims(
         %{
           "v" => @claims_version,
           "typ" => "principal",
           "tenant_id" => tenant_id,
           "sub" => principal_id,
           "principal_type" => principal_type,
           "actor" => actor,
           "scopes" => scopes,
           "exp" => expires_at
         } = claims,
         leeway_seconds
       )
       when is_integer(expires_at) and expires_at > 0 do
    with :ok <- validate_exp(expires_at, leeway_seconds),
         {:ok, principal} <-
           principal_from_values(tenant_id, principal_id, principal_type, :invalid_bearer_token),
         :ok <- validate_actor(actor, principal),
         {:ok, normalized_scopes} <- normalize_string_list(scopes, :invalid_bearer_token),
         {:ok, normalized_session_ids} <-
           normalize_optional_string_list(Map.get(claims, "session_ids"), :invalid_bearer_token),
         :ok <-
           ensure_session_binding(principal.type, normalized_session_ids, :invalid_bearer_token) do
      {:ok,
       %{
         claims: claims,
         expires_at: expires_at,
         principal: principal,
         scopes: normalized_scopes,
         session_ids: normalized_session_ids
       }}
    end
  end

  defp verify_claims(_claims, _leeway_seconds), do: {:error, :invalid_bearer_token}

  defp principal_from_attrs(
         %{"tenant_id" => tenant_id, "id" => principal_id, "type" => principal_type},
         error_atom
       ) do
    principal_from_values(tenant_id, principal_id, principal_type, error_atom)
  end

  defp principal_from_attrs(_principal_attrs, error_atom), do: {:error, error_atom}

  defp principal_from_values(tenant_id, principal_id, "user", _error_atom)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" do
    Principal.new(tenant_id, principal_id, :user)
  end

  defp principal_from_values(tenant_id, principal_id, "agent", _error_atom)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(principal_id) and
              principal_id != "" do
    Principal.new(tenant_id, principal_id, :agent)
  end

  defp principal_from_values(_tenant_id, _principal_id, _principal_type, error_atom),
    do: {:error, error_atom}

  defp normalize_string_list(values, error_atom) when is_list(values) do
    normalized = Enum.uniq(values)

    if normalized != [] and Enum.all?(normalized, &is_non_empty_string/1) do
      {:ok, normalized}
    else
      {:error, error_atom}
    end
  end

  defp normalize_string_list(_values, error_atom), do: {:error, error_atom}

  defp normalize_optional_string_list(nil, _error_atom), do: {:ok, nil}

  defp normalize_optional_string_list(values, error_atom) do
    normalize_string_list(values, error_atom)
  end

  defp ttl_seconds(nil, default_ttl_seconds, _max_ttl_seconds), do: {:ok, default_ttl_seconds}

  defp ttl_seconds(ttl_seconds, _default_ttl_seconds, max_ttl_seconds)
       when is_integer(ttl_seconds) and ttl_seconds > 0 and ttl_seconds <= max_ttl_seconds do
    {:ok, ttl_seconds}
  end

  defp ttl_seconds(_ttl_seconds, _default_ttl_seconds, _max_ttl_seconds),
    do: {:error, :invalid_issue_request}

  defp ensure_session_binding(:agent, [session_id], _error_atom)
       when is_binary(session_id) and session_id != "",
       do: :ok

  defp ensure_session_binding(:agent, _session_ids, error_atom), do: {:error, error_atom}
  defp ensure_session_binding(:user, _session_ids, _error_atom), do: :ok

  defp validate_exp(expires_at, leeway_seconds)
       when is_integer(expires_at) and is_integer(leeway_seconds) and leeway_seconds >= 0 do
    now = System.system_time(:second)
    if expires_at + leeway_seconds >= now, do: :ok, else: {:error, :token_expired}
  end

  defp validate_actor(actor, %Principal{} = principal) when is_binary(actor) do
    if actor == Principal.actor(principal), do: :ok, else: {:error, :invalid_bearer_token}
  end

  defp validate_actor(_actor, _principal), do: {:error, :invalid_bearer_token}

  defp is_non_empty_string(value), do: is_binary(value) and value != ""

  defp drop_nil_claims(claims) do
    Enum.reduce(claims, %{}, fn
      {_key, nil}, acc -> acc
      {key, value}, acc -> Map.put(acc, key, value)
    end)
  end
end
