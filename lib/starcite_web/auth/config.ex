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
    opts = auth_opts(Application.get_env(:starcite, StarciteWeb.Auth, []))

    mode = auth_mode(Keyword.get(opts, :mode, :none))

    jwt_leeway_seconds =
      non_neg_integer(
        Keyword.get(opts, :jwt_leeway_seconds, @default_jwt_leeway_seconds),
        :jwt_leeway_seconds
      )

    jwks_refresh_ms =
      positive_integer(
        Keyword.get(opts, :jwks_refresh_ms, @default_jwks_refresh_ms),
        :jwks_refresh_ms
      )

    principal_token_salt =
      non_empty_string(
        Keyword.get(opts, :principal_token_salt, @default_principal_token_salt),
        :principal_token_salt
      )

    principal_token_default_ttl_seconds =
      positive_integer(
        Keyword.get(
          opts,
          :principal_token_default_ttl_seconds,
          @default_principal_token_default_ttl_seconds
        ),
        :principal_token_default_ttl_seconds
      )

    principal_token_max_ttl_seconds =
      positive_integer(
        Keyword.get(
          opts,
          :principal_token_max_ttl_seconds,
          @default_principal_token_max_ttl_seconds
        ),
        :principal_token_max_ttl_seconds
      )

    if principal_token_default_ttl_seconds > principal_token_max_ttl_seconds do
      raise ArgumentError,
            "invalid Starcite auth principal token ttl defaults: default (#{principal_token_default_ttl_seconds}) exceeds max (#{principal_token_max_ttl_seconds})"
    end

    base = %{
      jwt_leeway_seconds: jwt_leeway_seconds,
      jwks_refresh_ms: jwks_refresh_ms,
      principal_token_salt: principal_token_salt,
      principal_token_default_ttl_seconds: principal_token_default_ttl_seconds,
      principal_token_max_ttl_seconds: principal_token_max_ttl_seconds
    }

    case mode do
      :none ->
        Map.merge(base, %{
          mode: :none,
          issuer: nil,
          audience: nil,
          jwks_url: nil
        })

      :jwt ->
        Map.merge(base, %{
          mode: :jwt,
          issuer: non_empty_string(Keyword.get(opts, :issuer), :issuer),
          audience: non_empty_string(Keyword.get(opts, :audience), :audience),
          jwks_url: non_empty_string(Keyword.get(opts, :jwks_url), :jwks_url)
        })
    end
  end

  defp auth_opts(opts) when is_list(opts), do: opts
  defp auth_opts(opts) when is_map(opts), do: Map.to_list(opts)
  defp auth_opts(_opts), do: []

  defp auth_mode(:none), do: :none
  defp auth_mode(:jwt), do: :jwt

  defp auth_mode(other) do
    raise ArgumentError, "invalid Starcite auth mode: #{inspect(other)}"
  end

  defp non_empty_string(value, _field) when is_binary(value) and value != "", do: value

  defp non_empty_string(value, field) do
    raise ArgumentError, "invalid Starcite auth value for #{field}: #{inspect(value)}"
  end

  defp non_neg_integer(value, _field) when is_integer(value) and value >= 0, do: value

  defp non_neg_integer(value, field) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end

  defp positive_integer(value, _field) when is_integer(value) and value > 0, do: value

  defp positive_integer(value, field) do
    raise ArgumentError, "invalid Starcite auth integer for #{field}: #{inspect(value)}"
  end
end
