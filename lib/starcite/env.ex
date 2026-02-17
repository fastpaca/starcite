defmodule Starcite.Env do
  @moduledoc """
  Shared strict environment parsing helpers.

  Parsers in this module are intentionally narrow and raise on invalid input so
  misconfiguration fails fast during boot.
  """

  @bool_map %{
    "1" => true,
    "true" => true,
    "yes" => true,
    "on" => true,
    "0" => false,
    "false" => false,
    "no" => false,
    "off" => false
  }

  @spec parse_bool!(binary(), binary()) :: boolean()
  def parse_bool!(raw, env_key) when is_binary(raw) and is_binary(env_key) do
    normalized = raw |> String.trim() |> String.downcase()

    case @bool_map do
      %{^normalized => value} -> value
      _ -> raise ArgumentError, "invalid boolean for #{env_key}: #{inspect(raw)}"
    end
  end
end
