defmodule StarciteWeb.Auth.JWT do
  @moduledoc false

  alias Joken.Signer
  alias StarciteWeb.Auth.JWKS

  @spec verify(String.t(), map()) :: {:ok, map()} | {:error, atom()}
  def verify(token, config) when is_binary(token) and token != "" and is_map(config) do
    with {:ok, header} <- peek_header(token),
         {:ok, kid} <- kid_from_header(header),
         {:ok, signer} <- JWKS.fetch_signing_key(config, kid),
         {:ok, claims} <- verify_signature(token, signer),
         :ok <- validate_claims(claims, config) do
      {:ok, claims}
    end
  end

  def verify(_token, _config), do: {:error, :invalid_jwt}

  defp peek_header(token) when is_binary(token) do
    # Non-JWT bearer tokens must fail closed here and return unauthorized.
    try do
      case Joken.peek_header(token) do
        {:ok, header} when is_map(header) -> {:ok, header}
        {:error, _reason} -> {:error, :invalid_jwt_header}
      end
    rescue
      _error -> {:error, :invalid_jwt_header}
    end
  end

  defp kid_from_header(%{"kid" => kid}) when is_binary(kid) and kid != "", do: {:ok, kid}
  defp kid_from_header(_header), do: {:error, :invalid_jwt_header}

  defp verify_signature(token, %Signer{} = signer) when is_binary(token) do
    # Defensive rescue prevents malformed tokens from bubbling as 500s.
    try do
      case Joken.verify(token, signer) do
        {:ok, claims} when is_map(claims) -> {:ok, claims}
        {:error, :signature_error} -> {:error, :invalid_jwt_signature}
        {:error, _reason} -> {:error, :invalid_jwt}
      end
    rescue
      _error -> {:error, :invalid_jwt}
    end
  end

  defp validate_claims(%{"iss" => issuer, "aud" => audience, "exp" => exp} = claims, config)
       when is_binary(issuer) and is_integer(exp) and exp >= 0 do
    with :ok <- validate_issuer(issuer, config.issuer),
         :ok <- validate_audience(audience, config.audience),
         :ok <- validate_expiration(exp, config.jwt_leeway_seconds),
         :ok <- validate_not_before(Map.get(claims, "nbf"), config.jwt_leeway_seconds),
         :ok <- validate_issued_at(Map.get(claims, "iat"), config.jwt_leeway_seconds) do
      :ok
    end
  end

  defp validate_claims(_claims, _config), do: {:error, :invalid_jwt_claims}

  defp validate_issuer(issuer, issuer), do: :ok
  defp validate_issuer(_actual, _expected), do: {:error, :invalid_jwt_issuer}

  defp validate_audience(audience, expected_audience) when is_binary(audience) do
    if audience == expected_audience, do: :ok, else: {:error, :invalid_jwt_audience}
  end

  defp validate_audience(audience, expected_audience) when is_list(audience) do
    if Enum.all?(audience, &is_binary/1) and Enum.member?(audience, expected_audience) do
      :ok
    else
      {:error, :invalid_jwt_audience}
    end
  end

  defp validate_audience(_audience, _expected_audience), do: {:error, :invalid_jwt_audience}

  defp validate_expiration(exp, leeway_seconds)
       when is_integer(exp) and exp >= 0 and is_integer(leeway_seconds) and leeway_seconds >= 0 do
    now = System.system_time(:second)

    if exp + leeway_seconds >= now do
      :ok
    else
      {:error, :token_expired}
    end
  end

  defp validate_not_before(nil, _leeway_seconds), do: :ok

  defp validate_not_before(nbf, leeway_seconds)
       when is_integer(nbf) and nbf >= 0 and is_integer(leeway_seconds) and leeway_seconds >= 0 do
    now = System.system_time(:second)

    if nbf - leeway_seconds <= now do
      :ok
    else
      {:error, :token_not_yet_valid}
    end
  end

  defp validate_not_before(_nbf, _leeway_seconds), do: {:error, :invalid_jwt_claims}

  defp validate_issued_at(nil, _leeway_seconds), do: :ok

  defp validate_issued_at(iat, leeway_seconds)
       when is_integer(iat) and iat >= 0 and is_integer(leeway_seconds) and leeway_seconds >= 0 do
    now = System.system_time(:second)

    if iat <= now + leeway_seconds do
      :ok
    else
      {:error, :invalid_jwt_claims}
    end
  end

  defp validate_issued_at(_iat, _leeway_seconds), do: {:error, :invalid_jwt_claims}
end
