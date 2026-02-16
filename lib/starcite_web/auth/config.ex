defmodule StarciteWeb.Auth.Config do
  @moduledoc false

  @default_jwt_leeway_seconds 30
  @default_jwks_refresh_ms :timer.seconds(60)
  @default_principal_token_salt "principal-token-v1"
  @default_principal_token_default_ttl_seconds 600
  @default_principal_token_max_ttl_seconds 900

  @type mode :: :none | :jwt

  @type t :: %{
          mode: mode(),
          issuer: String.t() | nil,
          audience: String.t() | nil,
          jwks_url: String.t() | nil,
          jwt_leeway_seconds: non_neg_integer(),
          jwks_refresh_ms: pos_integer(),
          principal_token_salt: String.t(),
          principal_token_default_ttl_seconds: pos_integer(),
          principal_token_max_ttl_seconds: pos_integer()
        }

  @spec load() :: t()
  def load do
    Application.get_env(:starcite, StarciteWeb.Auth, [])
    |> normalize_config()
  end

  defp normalize_config(raw) when is_map(raw) do
    raw
    |> Map.to_list()
    |> normalize_config()
  end

  defp normalize_config(raw) when is_list(raw) do
    mode = Keyword.get(raw, :mode, :none)

    jwt_leeway_seconds =
      non_neg_integer!(
        Keyword.get(raw, :jwt_leeway_seconds),
        :jwt_leeway_seconds,
        @default_jwt_leeway_seconds
      )

    jwks_refresh_ms =
      pos_integer!(Keyword.get(raw, :jwks_refresh_ms), :jwks_refresh_ms, @default_jwks_refresh_ms)

    principal_token_salt =
      optional_non_empty_string!(
        Keyword.get(raw, :principal_token_salt),
        :principal_token_salt,
        @default_principal_token_salt
      )

    principal_token_default_ttl_seconds =
      pos_integer!(
        Keyword.get(raw, :principal_token_default_ttl_seconds),
        :principal_token_default_ttl_seconds,
        @default_principal_token_default_ttl_seconds
      )

    principal_token_max_ttl_seconds =
      pos_integer!(
        Keyword.get(raw, :principal_token_max_ttl_seconds),
        :principal_token_max_ttl_seconds,
        @default_principal_token_max_ttl_seconds
      )

    if principal_token_default_ttl_seconds > principal_token_max_ttl_seconds do
      raise ArgumentError,
            "invalid Starcite auth principal token ttl defaults: default (#{principal_token_default_ttl_seconds}) exceeds max (#{principal_token_max_ttl_seconds})"
    end

    case mode do
      :none ->
        %{
          mode: :none,
          issuer: nil,
          audience: nil,
          jwks_url: nil,
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms,
          principal_token_salt: principal_token_salt,
          principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
          principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
        }

      :jwt ->
        %{
          mode: :jwt,
          issuer: required_string!(Keyword.get(raw, :issuer), :issuer),
          audience: required_string!(Keyword.get(raw, :audience), :audience),
          jwks_url: required_string!(Keyword.get(raw, :jwks_url), :jwks_url),
          jwt_leeway_seconds: jwt_leeway_seconds,
          jwks_refresh_ms: jwks_refresh_ms,
          principal_token_salt: principal_token_salt,
          principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
          principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
        }

      other ->
        raise ArgumentError, "invalid Starcite auth mode: #{inspect(other)}"
    end
  end

  defp normalize_config(_raw) do
    normalize_config([])
  end

  defp required_string!(value, _field) when is_binary(value) and value != "", do: value

  defp required_string!(value, field) do
    raise ArgumentError, "invalid Starcite auth value for #{field}: #{inspect(value)}"
  end

  defp optional_non_empty_string!(nil, _field, default), do: default

  defp optional_non_empty_string!(value, _field, _default) when is_binary(value) and value != "",
    do: value

  defp optional_non_empty_string!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth value for #{field}: #{inspect(value)}"
  end

  defp non_neg_integer!(nil, _field, default), do: default
  defp non_neg_integer!(value, _field, _default) when is_integer(value) and value >= 0, do: value

  defp non_neg_integer!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end

  defp pos_integer!(nil, _field, default), do: default
  defp pos_integer!(value, _field, _default) when is_integer(value) and value > 0, do: value

  defp pos_integer!(value, field, _default) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end
end
