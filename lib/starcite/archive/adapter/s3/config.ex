defmodule Starcite.Archive.Adapter.S3.Config do
  @moduledoc false

  @default_prefix "starcite"
  @default_chunk_size 256
  @default_max_write_retries 4
  @default_path_style true

  @spec build!(keyword(), keyword()) :: map()
  def build!(runtime_opts, start_opts) when is_list(runtime_opts) and is_list(start_opts) do
    opts = runtime_opts |> Keyword.merge(start_opts) |> Keyword.new()

    chunk_size = cache_chunk_size!()
    ensure_chunk_size!(opts[:chunk_size], chunk_size)

    path_style = boolean!(Keyword.get(opts, :path_style, @default_path_style), :path_style)

    %{
      bucket: required_binary!(opts[:bucket], :bucket),
      prefix: normalize_prefix(Keyword.get(opts, :prefix, @default_prefix)),
      chunk_size: chunk_size,
      max_write_retries:
        positive_integer!(
          Keyword.get(opts, :max_write_retries, @default_max_write_retries),
          :max_write_retries
        ),
      request_opts:
        [
          region: optional_binary(opts[:region], :region),
          access_key_id: optional_binary(opts[:access_key_id], :access_key_id),
          secret_access_key: optional_binary(opts[:secret_access_key], :secret_access_key),
          security_token: optional_binary(opts[:security_token], :security_token),
          virtual_host: not path_style
        ]
        |> maybe_put_endpoint(opts[:endpoint])
        |> Enum.reject(fn {_k, v} -> is_nil(v) end)
    }
  end

  defp ensure_chunk_size!(nil, _cache_chunk_size), do: :ok

  defp ensure_chunk_size!(value, cache_chunk_size) when value == cache_chunk_size, do: :ok

  defp ensure_chunk_size!(value, cache_chunk_size) do
    raise ArgumentError,
          "invalid :chunk_size=#{inspect(value)} (S3 archive uses one object per cache line: #{cache_chunk_size})"
  end

  defp maybe_put_endpoint(opts, nil), do: opts

  defp maybe_put_endpoint(opts, endpoint) do
    endpoint = required_binary!(endpoint, :endpoint)

    case URI.parse(endpoint) do
      %URI{scheme: scheme, host: host, port: port, path: path}
      when scheme in ["http", "https"] and is_binary(host) and host != "" and
             path in [nil, "", "/"] ->
        opts ++ [scheme: "#{scheme}://", host: host, port: port]

      _ ->
        raise ArgumentError,
              "invalid :endpoint for S3 archive adapter: #{inspect(endpoint)} (expected http(s)://host[:port])"
    end
  end

  defp cache_chunk_size! do
    Application.get_env(:starcite, :event_store_cache_chunk_size, @default_chunk_size)
    |> positive_integer!(:event_store_cache_chunk_size)
  end

  defp required_binary!(value, _key) when is_binary(value) and value != "", do: value

  defp required_binary!(value, key) do
    raise ArgumentError,
          "missing required :#{key} for S3 archive adapter (got: #{inspect(value)})"
  end

  defp optional_binary(nil, _key), do: nil
  defp optional_binary(value, _key) when is_binary(value) and value != "", do: value

  defp optional_binary(value, key) do
    raise ArgumentError,
          "invalid :#{key} for S3 archive adapter: #{inspect(value)} (expected non-empty binary or nil)"
  end

  defp positive_integer!(value, _key) when is_integer(value) and value > 0, do: value

  defp positive_integer!(value, key) do
    raise ArgumentError,
          "invalid :#{key} for S3 archive adapter: #{inspect(value)} (expected positive integer)"
  end

  defp boolean!(value, _key) when is_boolean(value), do: value

  defp boolean!(value, key) do
    raise ArgumentError,
          "invalid :#{key} for S3 archive adapter: #{inspect(value)} (expected boolean)"
  end

  defp normalize_prefix(prefix) when is_binary(prefix) do
    prefix
    |> String.trim()
    |> String.trim("/")
    |> case do
      "" -> @default_prefix
      normalized -> normalized
    end
  end

  defp normalize_prefix(prefix) do
    raise ArgumentError,
          "invalid :prefix for S3 archive adapter: #{inspect(prefix)} (expected binary)"
  end
end
