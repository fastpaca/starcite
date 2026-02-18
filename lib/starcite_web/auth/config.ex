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
    opts = opts_map(Application.get_env(:starcite, StarciteWeb.Auth, []))

    mode =
      case Map.get(opts, :mode, :none) do
        mode when mode in [:none, :jwt] -> mode
        other -> raise ArgumentError, "invalid Starcite auth mode: #{inspect(other)}"
      end

    base = %{
      mode: mode,
      issuer: nil,
      audience: nil,
      jwks_url: nil,
      jwt_leeway_seconds: Map.get(opts, :jwt_leeway_seconds, @default_jwt_leeway_seconds),
      jwks_refresh_ms: Map.get(opts, :jwks_refresh_ms, @default_jwks_refresh_ms),
      principal_token_salt: Map.get(opts, :principal_token_salt, @default_principal_token_salt),
      principal_token_default_ttl_seconds:
        Map.get(
          opts,
          :principal_token_default_ttl_seconds,
          @default_principal_token_default_ttl_seconds
        ),
      principal_token_max_ttl_seconds:
        Map.get(opts, :principal_token_max_ttl_seconds, @default_principal_token_max_ttl_seconds)
    }

    case mode do
      :none ->
        base

      :jwt ->
        %{
          base
          | issuer: Map.get(opts, :issuer),
            audience: Map.get(opts, :audience),
            jwks_url: Map.get(opts, :jwks_url)
        }
    end
  end

  defp opts_map(opts) when is_list(opts), do: Map.new(opts)
  defp opts_map(opts) when is_map(opts), do: opts
  defp opts_map(_opts), do: %{}
end
