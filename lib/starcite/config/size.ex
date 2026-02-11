defmodule Starcite.Config.Size do
  @moduledoc false

  @kib 1_024
  @mib 1_048_576
  @gib 1_073_741_824
  @tib 1_099_511_627_776
  @default_examples "256MB, 4G, 1024M"

  @spec env_bytes_or_default!(String.t(), term(), keyword()) :: pos_integer()
  def env_bytes_or_default!(env_key, default, opts \\ []) when is_binary(env_key) do
    case System.get_env(env_key) do
      nil -> parse_bytes!(default, env_key, opts)
      raw -> parse_bytes!(raw, env_key, opts)
    end
  end

  @spec parse_bytes!(term(), String.t(), keyword()) :: pos_integer()
  def parse_bytes!(value, _env_key, _opts) when is_integer(value) and value > 0 do
    # Integer config values are interpreted as MB.
    value * @mib
  end

  def parse_bytes!(raw, env_key, opts) when is_binary(raw) and is_binary(env_key) do
    case Regex.run(~r/^\s*(\d+)\s*([a-zA-Z]*)\s*$/, raw) do
      [_, amount_raw, unit_raw] ->
        amount = String.to_integer(amount_raw)
        unit = String.upcase(unit_raw)
        multiplier = unit_multiplier(unit)

        case {amount, multiplier} do
          {amount, multiplier}
          when is_integer(amount) and amount > 0 and is_integer(multiplier) ->
            amount * multiplier

          _ ->
            invalid_size!(env_key, raw, opts)
        end

      _ ->
        invalid_size!(env_key, raw, opts)
    end
  end

  def parse_bytes!(value, env_key, opts) do
    invalid_size!(env_key, value, opts)
  end

  defp unit_multiplier(""), do: @mib
  defp unit_multiplier("B"), do: 1
  defp unit_multiplier("K"), do: @kib
  defp unit_multiplier("KB"), do: @kib
  defp unit_multiplier("KIB"), do: @kib
  defp unit_multiplier("M"), do: @mib
  defp unit_multiplier("MB"), do: @mib
  defp unit_multiplier("MIB"), do: @mib
  defp unit_multiplier("G"), do: @gib
  defp unit_multiplier("GB"), do: @gib
  defp unit_multiplier("GIB"), do: @gib
  defp unit_multiplier("T"), do: @tib
  defp unit_multiplier("TB"), do: @tib
  defp unit_multiplier("TIB"), do: @tib
  defp unit_multiplier(_unit), do: :invalid

  defp invalid_size!(env_key, raw, opts) when is_binary(env_key) do
    examples = Keyword.get(opts, :examples, @default_examples)

    raise ArgumentError,
          "invalid size for #{env_key}: #{inspect(raw)} (examples: #{examples})"
  end
end
