defmodule StarciteWeb.Auth.JWT do
  @moduledoc false

  alias StarciteWeb.Auth.JWKS

  @supported_algs %{
    "RS256" => :sha256
  }

  @spec verify(String.t(), map()) :: {:ok, map()} | {:error, atom()}
  def verify(token, config) when is_binary(token) and token != "" and is_map(config) do
    with {:ok, header_segment, claims_segment, signature_segment} <- split_jwt(token),
         {:ok, header} <- decode_json_segment(header_segment),
         {:ok, claims} <- decode_json_segment(claims_segment),
         {:ok, digest_type} <- digest_type_for_header(header),
         {:ok, kid} <- kid_from_header(header),
         {:ok, signing_key} <- JWKS.fetch_signing_key(config, kid),
         {:ok, signature} <- decode_base64url(signature_segment),
         :ok <-
           verify_signature(
             header_segment <> "." <> claims_segment,
             signature,
             signing_key,
             digest_type
           ),
         :ok <- validate_claims(claims, config) do
      {:ok, claims}
    end
  end

  def verify(_token, _config), do: {:error, :invalid_jwt}

  defp split_jwt(token) when is_binary(token) do
    case String.split(token, ".") do
      [header_segment, claims_segment, signature_segment]
      when header_segment != "" and claims_segment != "" and signature_segment != "" ->
        {:ok, header_segment, claims_segment, signature_segment}

      _ ->
        {:error, :invalid_jwt}
    end
  end

  defp decode_json_segment(segment) when is_binary(segment) do
    with {:ok, decoded} <- decode_base64url(segment),
         {:ok, parsed} <- Jason.decode(decoded),
         true <- is_map(parsed) do
      {:ok, parsed}
    else
      _ -> {:error, :invalid_jwt}
    end
  end

  defp decode_base64url(value) when is_binary(value) and value != "" do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> {:error, :invalid_jwt}
    end
  end

  defp digest_type_for_header(%{"alg" => alg}) when is_binary(alg) do
    case Map.fetch(@supported_algs, alg) do
      {:ok, digest_type} -> {:ok, digest_type}
      :error -> {:error, :unsupported_jwt_alg}
    end
  end

  defp digest_type_for_header(_header), do: {:error, :invalid_jwt_header}

  defp kid_from_header(%{"kid" => kid}) when is_binary(kid) and kid != "", do: {:ok, kid}
  defp kid_from_header(_header), do: {:error, :invalid_jwt_header}

  defp verify_signature(signing_input, signature, signing_key, digest_type)
       when is_binary(signing_input) and is_binary(signature) do
    if :public_key.verify(signing_input, digest_type, signature, signing_key) do
      :ok
    else
      {:error, :invalid_jwt_signature}
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
