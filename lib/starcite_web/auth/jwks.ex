defmodule StarciteWeb.Auth.JWKS do
  @moduledoc false

  alias Joken.Signer
  alias Starcite.Observability.Telemetry
  alias StarciteWeb.Auth.JWKSRefresher

  require Logger

  @request_headers [{"accept", "application/json"}]
  @cache_prefix :starcite_auth_jwks
  @hard_expiry_multiplier 5
  @min_hard_expiry_ms :timer.minutes(5)
  @finch StarciteWeb.Auth.JWKSFinch

  @type signing_key :: Signer.t()

  @spec fetch_signing_key(map(), String.t()) :: {:ok, signing_key()} | {:error, atom()}
  def fetch_signing_key(config, kid)
      when is_map(config) and is_binary(kid) and kid != "" do
    started_at = System.monotonic_time()

    {source, result} =
      case cached_signing_key(config, kid) do
        {:ok, _freshness, signing_key} ->
          schedule_refresh_if_due(config)
          {:cache, {:ok, signing_key}}

        :miss ->
          case JWKSRefresher.fetch(config, kid) do
            {:ok, signing_key} ->
              {:refresh, {:ok, signing_key}}

            {:error, reason} ->
              {:none, {:error, reason}}
          end
      end

    Telemetry.auth(
      :jwks_fetch,
      :jwt,
      jwks_outcome(result),
      elapsed_ms(started_at),
      jwks_error_reason(result),
      source
    )

    result
  end

  def fetch_signing_key(_config, _kid), do: {:error, :invalid_jwks_config}

  @spec cached_signing_key(map(), String.t()) :: {:ok, :fresh | :stale, signing_key()} | :miss
  def cached_signing_key(%{jwks_url: url}, kid)
      when is_binary(url) and url != "" and is_binary(kid) and kid != "" do
    now_ms = System.system_time(:millisecond)

    case {:persistent_term.get(meta_key(url), nil),
          :persistent_term.get(signing_key_key(url, kid), nil)} do
      {%{expire_at_ms: expire_at_ms, refresh_at_ms: refresh_at_ms}, signing_key}
      when is_integer(expire_at_ms) and is_integer(refresh_at_ms) and
             is_struct(signing_key, Signer) and
             expire_at_ms > now_ms ->
        freshness = if refresh_at_ms > now_ms, do: :fresh, else: :stale
        {:ok, freshness, signing_key}

      _ ->
        :miss
    end
  end

  def cached_signing_key(_config, _kid), do: :miss

  @spec refresh_url(map()) :: :ok | {:error, atom()}
  def refresh_url(%{jwks_url: url, jwks_refresh_ms: refresh_ms})
      when is_binary(url) and url != "" and is_integer(refresh_ms) and refresh_ms > 0 do
    now_ms = System.system_time(:millisecond)

    with {:ok, body} <- fetch_jwks(url),
         {:ok, key_entries} <- parse_jwks(body) do
      write_keys(url, key_entries, now_ms, refresh_ms)
    else
      {:error, reason} ->
        Logger.warning("JWKS refresh failed for #{url}: #{inspect(reason)}")
        {:error, :jwks_unavailable}
    end
  end

  def refresh_url(_config), do: {:error, :invalid_jwks_config}

  @doc false
  def clear_cache do
    :persistent_term.get()
    |> Enum.each(fn
      {{@cache_prefix, _kind, _url} = key, _value} ->
        :persistent_term.erase(key)

      {{@cache_prefix, _kind, _url, _kid} = key, _value} ->
        :persistent_term.erase(key)

      {_other_key, _value} ->
        :ok
    end)

    if Process.whereis(JWKSRefresher) do
      :ok = JWKSRefresher.clear()
    end

    :ok
  end

  defp schedule_refresh_if_due(%{jwks_url: url} = config) when is_binary(url) and url != "" do
    case :persistent_term.get(meta_key(url), nil) do
      %{refresh_at_ms: refresh_at_ms} when is_integer(refresh_at_ms) ->
        if refresh_at_ms <= System.system_time(:millisecond) do
          :ok = JWKSRefresher.refresh_async(config)
        end

      _ ->
        :ok
    end

    :ok
  end

  defp schedule_refresh_if_due(_config), do: :ok

  defp meta_key(url) when is_binary(url) and url != "" do
    {@cache_prefix, :jwks_meta, url}
  end

  defp signing_key_key(url, kid)
       when is_binary(url) and url != "" and is_binary(kid) and kid != "" do
    {@cache_prefix, :signing_key, url, kid}
  end

  defp write_keys(url, key_entries, now_ms, refresh_ms)
       when is_binary(url) and is_list(key_entries) and is_integer(now_ms) and
              is_integer(refresh_ms) and
              refresh_ms > 0 do
    stale_kids =
      case :persistent_term.get(meta_key(url), nil) do
        %{kids: kids} when is_list(kids) ->
          kids

        _ ->
          []
      end

    delete_stale_keys(url, stale_kids)

    Enum.each(key_entries, fn {kid, signing_key} ->
      :persistent_term.put(signing_key_key(url, kid), signing_key)
    end)

    :persistent_term.put(meta_key(url), %{
      kids: Enum.map(key_entries, &elem(&1, 0)),
      refresh_at_ms: now_ms + refresh_ms,
      expire_at_ms: now_ms + max(refresh_ms * @hard_expiry_multiplier, @min_hard_expiry_ms)
    })

    :ok
  end

  defp delete_stale_keys(url, stale_kids) when is_binary(url) and is_list(stale_kids) do
    Enum.each(stale_kids, fn kid ->
      :persistent_term.erase(signing_key_key(url, kid))
    end)

    :ok
  end

  defp fetch_jwks(url) when is_binary(url) and url != "" do
    case Req.get(
           url: url,
           headers: @request_headers,
           decode_body: false,
           retry: false,
           finch: @finch,
           pool_timeout: 1_000,
           receive_timeout: 2_000
         ) do
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
         true <- is_list(keys),
         true <- keys != [],
         {:ok, key_entries} <- parse_keys(keys) do
      {:ok, key_entries}
    else
      _ -> {:error, :invalid_jwks}
    end
  end

  defp parse_keys(keys) when is_list(keys) do
    Enum.reduce_while(keys, {:ok, {MapSet.new(), []}}, fn key, {:ok, {seen, acc}} ->
      with {:ok, kid, signing_key} <- parse_jwk(key),
           :ok <- ensure_unique_kid(seen, kid) do
        {:cont, {:ok, {MapSet.put(seen, kid), [{kid, signing_key} | acc]}}}
      else
        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, {_seen, entries}} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp ensure_unique_kid(seen, kid) when is_struct(seen, MapSet) and is_binary(kid) do
    if MapSet.member?(seen, kid) do
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

  defp elapsed_ms(started_at) when is_integer(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :millisecond)
  end

  defp jwks_outcome({:ok, _signing_key}), do: :ok
  defp jwks_outcome({:error, _reason}), do: :error

  defp jwks_error_reason({:ok, _signing_key}), do: :none
  defp jwks_error_reason({:error, reason}) when is_atom(reason), do: reason
  defp jwks_error_reason({:error, _reason}), do: :error
end
