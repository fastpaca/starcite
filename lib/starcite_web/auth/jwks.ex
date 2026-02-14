defmodule StarciteWeb.Auth.JWKS do
  @moduledoc false

  alias Joken.Signer

  require Logger

  @cache_table :starcite_auth_jwks_cache
  @request_headers [{"accept", "application/json"}]

  @type signing_key :: Signer.t()

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
    # Security policy: fail closed. If JWT auth is enabled, we require every JWKS key
    # to match our RS256 signing contract and reject the entire JWKS on any invalid or
    # duplicate key instead of accepting a partial set.
    with {:ok, parsed} <- Jason.decode(body),
         %{"keys" => keys} <- parsed,
         true <- is_list(keys),
         true <- keys != [],
         {:ok, keys_by_kid} <- keys_to_map(keys) do
      {:ok, keys_by_kid}
    else
      _ -> {:error, :invalid_jwks}
    end
  end

  defp keys_to_map(keys) when is_list(keys) do
    Enum.reduce_while(keys, {:ok, %{}}, fn key, {:ok, acc} ->
      with {:ok, kid, signing_key} <- parse_jwk(key),
           :ok <- ensure_unique_kid(acc, kid) do
        {:cont, {:ok, Map.put(acc, kid, signing_key)}}
      else
        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp ensure_unique_kid(keys_by_kid, kid) when is_map(keys_by_kid) and is_binary(kid) do
    if Map.has_key?(keys_by_kid, kid) do
      {:error, :duplicate_jwt_kid}
    else
      :ok
    end
  end

  defp parse_jwk(
         %{
           "kty" => "RSA",
           "kid" => kid,
           "use" => "sig",
           "alg" => "RS256",
           "n" => n,
           "e" => e
         } = key
       )
       when is_binary(kid) and kid != "" and is_binary(n) and n != "" and is_binary(e) and e != "" do
    build_signer(kid, key)
  end

  defp parse_jwk(_key), do: {:error, :invalid_jwks}

  defp build_signer(kid, jwk_map) when is_binary(kid) and is_map(jwk_map) do
    try do
      {:ok, kid, Signer.create("RS256", jwk_map)}
    rescue
      _error ->
        {:error, :invalid_jwks}
    catch
      _class, _reason ->
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
