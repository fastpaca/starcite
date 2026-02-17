defmodule Starcite.Archive.Adapter.S3.Config do
  @moduledoc """
  Thin config builder for the S3 archive adapter.

  Runtime env parsing/coercion happens in `runtime.exs`; this module only merges
  options and shapes them for ExAws.

  Chunk size is fixed to `:event_store_cache_chunk_size` so each S3 object maps
  to one cache line.
  """

  @default_prefix "starcite"
  @default_chunk_size 256
  @default_max_write_retries 4
  @default_path_style true
  @default_client_mod Starcite.Archive.Adapter.S3.Client

  @spec build!(keyword(), keyword()) :: map()
  def build!(runtime_opts, start_opts) when is_list(runtime_opts) and is_list(start_opts) do
    opts = runtime_opts |> Keyword.merge(start_opts) |> Keyword.new()

    %{
      bucket: required_bucket!(opts),
      prefix: normalize_prefix(Keyword.get(opts, :prefix, @default_prefix)),
      chunk_size: cache_chunk_size(),
      max_write_retries: Keyword.get(opts, :max_write_retries, @default_max_write_retries),
      client_mod: Keyword.get(opts, :client_mod, @default_client_mod),
      request_opts: request_opts(opts)
    }
  end

  defp request_opts(opts) do
    path_style = Keyword.get(opts, :path_style, @default_path_style)

    [
      region: opts[:region],
      access_key_id: opts[:access_key_id],
      secret_access_key: opts[:secret_access_key],
      security_token: opts[:security_token],
      virtual_host: not path_style
    ]
    |> Keyword.merge(endpoint_opts(Keyword.get(opts, :endpoint)))
    |> Enum.reject(fn {_k, v} -> is_nil(v) end)
  end

  defp endpoint_opts(nil), do: []

  defp endpoint_opts(endpoint) when is_binary(endpoint) do
    case URI.parse(endpoint) do
      %URI{scheme: scheme, host: host, port: port, path: path}
      when scheme in ["http", "https"] and is_binary(host) and host != "" and
             path in [nil, "", "/"] ->
        [scheme: "#{scheme}://", host: host, port: port]

      _ ->
        raise ArgumentError,
              "invalid :endpoint for S3 archive adapter: #{inspect(endpoint)} (expected http(s)://host[:port])"
    end
  end

  defp endpoint_opts(endpoint) do
    raise ArgumentError,
          "invalid :endpoint for S3 archive adapter: #{inspect(endpoint)} (expected http(s)://host[:port])"
  end

  defp required_bucket!(opts) do
    case opts[:bucket] do
      bucket when is_binary(bucket) and bucket != "" ->
        bucket

      value ->
        raise ArgumentError,
              "missing required :bucket for S3 archive adapter (got: #{inspect(value)})"
    end
  end

  defp cache_chunk_size do
    Application.get_env(:starcite, :event_store_cache_chunk_size, @default_chunk_size)
  end

  defp normalize_prefix(prefix) do
    case prefix |> String.trim() |> String.trim("/") do
      "" -> @default_prefix
      normalized -> normalized
    end
  end
end
