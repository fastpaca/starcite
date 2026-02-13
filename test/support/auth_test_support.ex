defmodule Starcite.AuthTestSupport do
  @moduledoc false

  @spec generate_rsa_private_key() :: tuple()
  def generate_rsa_private_key do
    :public_key.generate_key({:rsa, 2048, 65_537})
  end

  @spec jwks_for_private_key(tuple(), String.t()) :: map()
  def jwks_for_private_key(private_key, kid)
      when is_tuple(private_key) and is_binary(kid) and kid != "" do
    {:RSAPrivateKey, :"two-prime", modulus, exponent, _private_exponent, _prime1, _prime2, _exp1,
     _exp2, _coefficient, _other_prime_infos} = private_key

    %{
      "keys" => [
        %{
          "kty" => "RSA",
          "kid" => kid,
          "alg" => "RS256",
          "use" => "sig",
          "n" => base64url_uint(modulus),
          "e" => base64url_uint(exponent)
        }
      ]
    }
  end

  @spec sign_rs256(tuple(), map(), String.t()) :: String.t()
  def sign_rs256(private_key, claims, kid)
      when is_tuple(private_key) and is_map(claims) and is_binary(kid) and kid != "" do
    header = %{"alg" => "RS256", "typ" => "JWT", "kid" => kid}

    header_segment = encode_json_segment(header)
    claims_segment = encode_json_segment(claims)
    signing_input = header_segment <> "." <> claims_segment

    signature =
      :public_key.sign(signing_input, :sha256, private_key)
      |> Base.url_encode64(padding: false)

    signing_input <> "." <> signature
  end

  defp encode_json_segment(value) do
    value
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  defp base64url_uint(int) when is_integer(int) and int > 0 do
    int
    |> :binary.encode_unsigned()
    |> Base.url_encode64(padding: false)
  end
end
