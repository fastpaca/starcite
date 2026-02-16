defmodule StarciteWeb.Auth.PrincipalToken do
  @moduledoc """
  Issues and verifies short-lived principal tokens.

  This module exists to let a trusted service caller mint a temporary token for a
  specific tenant principal (user/agent) so browser or plugin clients can
  call Starcite directly without receiving long-lived service credentials.

  Security contract:

  - Input shape is intentionally strict and fail-fast.
  - Actor identity is derived from principal type/id, never accepted from caller input.
  - Tokens carry bounded TTL and are signed with `Phoenix.Token`.
  - Verification revalidates claim shape, expiry, and actor/principal consistency.
  """

  alias Starcite.Auth.Principal

  @claims_version 1

  @type issued :: %{
          token: String.t(),
          expires_at: pos_integer(),
          expires_in: pos_integer(),
          principal: Principal.t(),
          scopes: [String.t()],
          session_ids: [String.t()] | nil,
          owner_principal_ids: [String.t()] | nil
        }

  # Keep a single, opinionated issue payload shape to minimize auth surface area.
  @type issue_attrs :: %{
          required("principal") => %{
            required("tenant_id") => String.t(),
            required("id") => String.t(),
            required("type") => String.t()
          },
          required("scopes") => [String.t()],
          optional("session_ids") => [String.t()],
          optional("owner_principal_ids") => [String.t()],
          optional("ttl_seconds") => pos_integer()
        }

  @spec issue(issue_attrs(), map()) :: {:ok, issued()} | {:error, atom()}
  def issue(
        %{
          "principal" => %{
            "tenant_id" => tenant_id,
            "id" => principal_id,
            "type" => principal_type
          },
          "scopes" => scopes
        } = attrs,
        %{
          principal_token_salt: _principal_token_salt,
          principal_token_default_ttl_seconds: _default_ttl_seconds,
          principal_token_max_ttl_seconds: _max_ttl_seconds,
          jwt_leeway_seconds: _jwt_leeway_seconds
        } = config
      ) do
    now = System.system_time(:second)

    with {:ok, tenant_id} <- required_string(tenant_id, :invalid_issue_request),
         {:ok, principal_id} <- required_string(principal_id, :invalid_issue_request),
         {:ok, principal_type} <- principal_type(principal_type, :invalid_issue_request),
         {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type),
         {:ok, scopes} <- scopes(scopes, :invalid_issue_request),
         {:ok, session_ids} <- session_ids(attrs["session_ids"], :invalid_issue_request),
         {:ok, owner_principal_ids} <-
           owner_principal_ids(attrs["owner_principal_ids"], :invalid_issue_request),
         :ok <- validate_session_binding(principal_type, session_ids),
         {:ok, ttl_seconds} <- ttl_seconds(attrs["ttl_seconds"], config) do
      expires_at = now + ttl_seconds
      # Actor is derived from principal identity, never accepted as caller input.
      actor = Principal.actor(principal)

      claims =
        %{
          "v" => @claims_version,
          "typ" => "principal",
          "tenant_id" => principal.tenant_id,
          "sub" => principal.id,
          "principal_type" => Atom.to_string(principal.type),
          "actor" => actor,
          "scopes" => scopes,
          "session_ids" => session_ids,
          "owner_principal_ids" => owner_principal_ids,
          "iat" => now,
          "exp" => expires_at
        }
        |> drop_nil_claims()

      token =
        Phoenix.Token.sign(
          StarciteWeb.Endpoint,
          config.principal_token_salt,
          claims
        )

      {:ok,
       %{
         token: token,
         expires_at: expires_at,
         expires_in: ttl_seconds,
         principal: principal,
         scopes: scopes,
         session_ids: session_ids,
         owner_principal_ids: owner_principal_ids
       }}
    end
  end

  def issue(_attrs, _config), do: {:error, :invalid_issue_request}

  @spec verify(String.t(), map()) ::
          {:ok,
           %{
             claims: map(),
             expires_at: pos_integer(),
             principal: Principal.t(),
             scopes: [String.t()],
             session_ids: [String.t()] | nil,
             owner_principal_ids: [String.t()] | nil
           }}
          | {:error, atom()}
  def verify(token, config) when is_binary(token) and token != "" and is_map(config) do
    # Enforce an absolute max age even if claim parsing logic changes in the future.
    max_age = config.principal_token_max_ttl_seconds + config.jwt_leeway_seconds

    case Phoenix.Token.verify(
           StarciteWeb.Endpoint,
           config.principal_token_salt,
           token,
           max_age: max_age
         ) do
      {:ok, claims} when is_map(claims) ->
        with :ok <- claim_version(claims),
             :ok <- claim_type(claims),
             {:ok, expires_at} <- claim_exp(claims),
             :ok <- validate_exp(expires_at, config.jwt_leeway_seconds),
             {:ok, tenant_id} <- required_string(claims["tenant_id"], :invalid_bearer_token),
             {:ok, principal_id} <- required_string(claims["sub"], :invalid_bearer_token),
             {:ok, principal_type} <-
               principal_type(claims["principal_type"], :invalid_bearer_token),
             {:ok, principal} <- Principal.new(tenant_id, principal_id, principal_type),
             {:ok, actor} <- required_string(claims["actor"], :invalid_bearer_token),
             # Guard against forged tokens with inconsistent actor/sub/type identity.
             :ok <- validate_actor(actor, principal),
             {:ok, scopes} <- scopes(claims["scopes"], :invalid_bearer_token),
             {:ok, session_ids} <- session_ids(claims["session_ids"], :invalid_bearer_token),
             {:ok, owner_principal_ids} <-
               owner_principal_ids(claims["owner_principal_ids"], :invalid_bearer_token) do
          {:ok,
           %{
             claims: claims,
             expires_at: expires_at,
             principal: principal,
             scopes: scopes,
             session_ids: session_ids,
             owner_principal_ids: owner_principal_ids
           }}
        end

      {:error, :expired} ->
        {:error, :token_expired}

      {:error, _reason} ->
        {:error, :invalid_bearer_token}
    end
  end

  def verify(_token, _config), do: {:error, :invalid_bearer_token}

  defp claim_version(%{"v" => @claims_version}), do: :ok
  defp claim_version(_claims), do: {:error, :invalid_bearer_token}

  defp claim_type(%{"typ" => "principal"}), do: :ok
  defp claim_type(_claims), do: {:error, :invalid_bearer_token}

  defp claim_exp(%{"exp" => exp}) when is_integer(exp) and exp > 0, do: {:ok, exp}
  defp claim_exp(_claims), do: {:error, :invalid_bearer_token}

  defp validate_exp(expires_at, leeway_seconds)
       when is_integer(expires_at) and expires_at > 0 and is_integer(leeway_seconds) and
              leeway_seconds >= 0 do
    now = System.system_time(:second)
    if expires_at + leeway_seconds >= now, do: :ok, else: {:error, :token_expired}
  end

  defp principal_type("user", _error_reason), do: {:ok, :user}
  defp principal_type("agent", _error_reason), do: {:ok, :agent}
  defp principal_type(_value, error_reason), do: {:error, error_reason}

  defp scopes(scopes, error_reason) when is_list(scopes) do
    case Enum.all?(scopes, fn value -> is_binary(value) and value != "" end) do
      true when scopes != [] -> {:ok, Enum.uniq(scopes)}
      _other -> {:error, error_reason}
    end
  end

  defp scopes(_scopes, error_reason), do: {:error, error_reason}

  defp session_ids(nil, _error_reason), do: {:ok, nil}

  defp session_ids(session_ids, error_reason) when is_list(session_ids) do
    case Enum.all?(session_ids, fn value -> is_binary(value) and value != "" end) do
      true when session_ids != [] -> {:ok, Enum.uniq(session_ids)}
      _other -> {:error, error_reason}
    end
  end

  defp session_ids(_session_ids, error_reason), do: {:error, error_reason}

  defp owner_principal_ids(nil, _error_reason), do: {:ok, nil}

  defp owner_principal_ids(owner_principal_ids, error_reason) when is_list(owner_principal_ids) do
    case Enum.all?(owner_principal_ids, fn value -> is_binary(value) and value != "" end) do
      true when owner_principal_ids != [] -> {:ok, Enum.uniq(owner_principal_ids)}
      _other -> {:error, error_reason}
    end
  end

  defp owner_principal_ids(_owner_principal_ids, error_reason), do: {:error, error_reason}

  defp ttl_seconds(nil, config) when is_map(config),
    do: {:ok, config.principal_token_default_ttl_seconds}

  defp ttl_seconds(ttl_seconds, config)
       when is_integer(ttl_seconds) and ttl_seconds > 0 and is_map(config) do
    if ttl_seconds <= config.principal_token_max_ttl_seconds do
      {:ok, ttl_seconds}
    else
      {:error, :invalid_issue_request}
    end
  end

  defp ttl_seconds(_ttl_seconds, _config), do: {:error, :invalid_issue_request}

  defp required_string(value, _error_reason) when is_binary(value) and value != "",
    do: {:ok, value}

  defp required_string(_value, error_reason), do: {:error, error_reason}

  defp validate_session_binding(:agent, [session_id])
       when is_binary(session_id) and session_id != "",
       do: :ok

  # Agent principals are session-scoped capabilities, never tenant-wide identities.
  defp validate_session_binding(:agent, _session_ids), do: {:error, :invalid_issue_request}
  defp validate_session_binding(_principal_type, _session_ids), do: :ok

  defp validate_actor(actor, %Principal{} = principal) when is_binary(actor) do
    if actor == Principal.actor(principal) do
      :ok
    else
      {:error, :invalid_bearer_token}
    end
  end

  defp drop_nil_claims(claims) when is_map(claims) do
    Enum.reduce(claims, %{}, fn
      {_key, nil}, acc -> acc
      {key, value}, acc -> Map.put(acc, key, value)
    end)
  end
end
