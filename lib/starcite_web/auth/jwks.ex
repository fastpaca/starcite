defmodule StarciteWeb.Auth.JWKS do
  @moduledoc false

  require Logger

  @cache_table :starcite_auth_jwks_cache
  @request_headers [{"accept", "application/json"}]

  @type signing_key :: tuple()

  @spec fetch_signing_key(map(), String.t()) :: {:ok, signing_key()} | {:error, atom()}
  def fetch_signing_key(config, kid)
      when is_map(config) and is_binary(kid) and kid != "" do
    with {:ok, keys_by_kid} <- keys_by_kid(config, false) do
      case Map.fetch(keys_by_kid, kid) do
        {:ok, signing_key} ->
          {:ok, signing_key}

        :error ->
          with {:ok, refreshed_keys} <- keys_by_kid(config, true),
               {:ok, signing_key} <- key_by_kid(refreshed_keys, kid) do
            {:ok, signing_key}
          end
      end
    end
  end

  def fetch_signing_key(_config, _kid), do: {:error, :invalid_jwks_config}

  @doc false
  def clear_cache do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ok

      table ->
        true = :ets.delete_all_objects(table)
        :ok
    end
  end

  defp key_by_kid(keys_by_kid, kid) when is_map(keys_by_kid) and is_binary(kid) do
    case Map.fetch(keys_by_kid, kid) do
      {:ok, signing_key} -> {:ok, signing_key}
      :error -> {:error, :unknown_jwt_kid}
    end
  end

  defp keys_by_kid(%{jwks_url: url, jwks_refresh_ms: refresh_ms}, force_refresh)
       when is_binary(url) and url != "" and is_integer(refresh_ms) and refresh_ms > 0 and
              is_boolean(force_refresh) do
    now_ms = System.system_time(:millisecond)

    cond do
      force_refresh ->
        refresh_keys(url, refresh_ms, now_ms)

      true ->
        case read_cached(url, now_ms) do
          {:ok, keys_by_kid} -> {:ok, keys_by_kid}
          :miss -> refresh_keys(url, refresh_ms, now_ms)
        end
    end
  end

  defp keys_by_kid(_config, _force_refresh), do: {:error, :invalid_jwks_config}

  defp read_cached(url, now_ms) when is_binary(url) and is_integer(now_ms) do
    table = ensure_cache_table()

    case :ets.lookup(table, url) do
      [{^url, expires_at_ms, keys_by_kid}]
      when is_integer(expires_at_ms) and expires_at_ms > now_ms and is_map(keys_by_kid) ->
        {:ok, keys_by_kid}

      _ ->
        :miss
    end
  end

  defp refresh_keys(url, refresh_ms, now_ms)
       when is_binary(url) and is_integer(refresh_ms) and refresh_ms > 0 and is_integer(now_ms) do
    with {:ok, body} <- fetch_jwks(url),
         {:ok, keys_by_kid} <- parse_jwks(body) do
      expires_at_ms = now_ms + refresh_ms
      table = ensure_cache_table()
      true = :ets.insert(table, {url, expires_at_ms, keys_by_kid})
      {:ok, keys_by_kid}
    else
      {:error, reason} ->
        Logger.warning("JWKS refresh failed for #{url}: #{inspect(reason)}")
        {:error, :jwks_unavailable}
    end
  end

  defp fetch_jwks(url) when is_binary(url) and url != "" do
    case Req.get(url: url, headers: @request_headers, decode_body: false, retry: false) do
      {:ok, %Req.Response{status: 200, body: body}} when is_binary(body) ->
        {:ok, body}

      {:ok, %Req.Response{status: status}} when is_integer(status) ->
        {:error, {:jwks_http_status, status}}

      {:error, reason} ->
        {:error, {:jwks_request_failed, reason}}
    end
  end

  defp parse_jwks(body) when is_binary(body) do
    with {:ok, parsed} <- Jason.decode(body),
         %{"keys" => keys} <- parsed,
         true <- is_list(keys) do
      keys_by_kid =
        Enum.reduce(keys, %{}, fn key, acc ->
          case parse_jwk(key) do
            {:ok, kid, signing_key} -> Map.put(acc, kid, signing_key)
            :skip -> acc
            {:error, _reason} -> acc
          end
        end)

      if map_size(keys_by_kid) > 0 do
        {:ok, keys_by_kid}
      else
        {:error, :invalid_jwks}
      end
    else
      _ -> {:error, :invalid_jwks}
    end
  end

  defp parse_jwk(%{"kty" => "RSA", "kid" => kid, "n" => n, "e" => e} = key)
       when is_binary(kid) and kid != "" and is_binary(n) and n != "" and is_binary(e) and e != "" do
    with :ok <- validate_key_use(key),
         :ok <- validate_key_alg(key),
         {:ok, modulus} <- decode_jwk_integer(n),
         {:ok, exponent} <- decode_jwk_integer(e) do
      {:ok, kid, {:RSAPublicKey, modulus, exponent}}
    else
      :skip -> :skip
      {:error, _reason} = error -> error
    end
  end

  defp parse_jwk(_key), do: :skip

  defp validate_key_use(%{} = key) do
    case Map.get(key, "use") do
      nil -> :ok
      "sig" -> :ok
      _other -> :skip
    end
  end

  defp validate_key_alg(%{} = key) do
    case Map.get(key, "alg") do
      nil -> :ok
      "RS256" -> :ok
      _other -> :skip
    end
  end

  defp decode_jwk_integer(value) when is_binary(value) and value != "" do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} when byte_size(decoded) > 0 ->
        {:ok, :binary.decode_unsigned(decoded)}

      _ ->
        {:error, :invalid_jwks}
    end
  end

  defp ensure_cache_table do
    case :ets.whereis(@cache_table) do
      :undefined ->
        try do
          :ets.new(@cache_table, [
            :named_table,
            :set,
            :public,
            {:read_concurrency, true},
            {:write_concurrency, true}
          ])
        rescue
          ArgumentError -> :ok
        end

        @cache_table

      table ->
        table
    end
  end
end
