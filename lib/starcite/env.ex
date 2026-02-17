defmodule Starcite.Env do
  @moduledoc false

  @spec parse_bool!(binary(), binary()) :: boolean()
  def parse_bool!(raw, env_key) when is_binary(raw) and is_binary(env_key) do
    case String.downcase(String.trim(raw)) do
      "1" -> true
      "true" -> true
      "yes" -> true
      "on" -> true
      "0" -> false
      "false" -> false
      "no" -> false
      "off" -> false
      _ -> raise ArgumentError, "invalid boolean for #{env_key}: #{inspect(raw)}"
    end
  end
end
