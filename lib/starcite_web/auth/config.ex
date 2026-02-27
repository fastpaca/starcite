defmodule StarciteWeb.Auth.Config do
  @moduledoc false

  @default_jwt_leeway_seconds 1
  @default_jwks_refresh_ms :timer.seconds(60)

  @type t :: %{
          mode: :none | :jwt,
          issuer: String.t() | nil,
          audience: String.t() | nil,
          jwks_url: String.t() | nil,
          jwt_leeway_seconds: non_neg_integer(),
          jwks_refresh_ms: pos_integer()
        }

  @spec load() :: t()
  def load do
    opts = opts_map(Application.get_env(:starcite, StarciteWeb.Auth, []))
    mode = mode!(Map.get(opts, :mode, :jwt))

    base = %{
      mode: mode,
      jwt_leeway_seconds: Map.get(opts, :jwt_leeway_seconds, @default_jwt_leeway_seconds),
      jwks_refresh_ms: Map.get(opts, :jwks_refresh_ms, @default_jwks_refresh_ms)
    }

    case mode do
      :none ->
        Map.merge(base, %{issuer: nil, audience: nil, jwks_url: nil})

      :jwt ->
        Map.merge(base, %{
          issuer: required_non_empty(opts, :issuer),
          audience: required_non_empty(opts, :audience),
          jwks_url: required_non_empty(opts, :jwks_url)
        })
    end
  end

  defp opts_map(opts) when is_list(opts), do: Map.new(opts)
  defp opts_map(opts) when is_map(opts), do: opts
  defp opts_map(_opts), do: %{}

  defp mode!(:none), do: :none
  defp mode!("none"), do: :none
  defp mode!(:jwt), do: :jwt
  defp mode!("jwt"), do: :jwt

  defp mode!(other) do
    raise ArgumentError, "invalid Starcite auth config mode #{inspect(other)} (expected none|jwt)"
  end

  defp required_non_empty(opts, key) when is_map(opts) and is_atom(key) do
    case Map.get(opts, key) do
      value when is_binary(value) ->
        case String.trim(value) do
          "" -> raise ArgumentError, "missing Starcite auth config #{inspect(key)}"
          trimmed -> trimmed
        end

      _other ->
        raise ArgumentError, "missing Starcite auth config #{inspect(key)}"
    end
  end
end
