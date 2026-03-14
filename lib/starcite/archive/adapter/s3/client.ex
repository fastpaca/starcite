defmodule Starcite.Archive.Adapter.S3.Client do
  @moduledoc """
  Thin typed wrapper around ExAws S3 operations used by the archive adapter.

  This module only translates ExAws HTTP responses into small result tuples so
  the adapter can stay focused on archive semantics.
  """

  alias ExAws.S3

  @type config :: %{bucket: String.t(), request_opts: keyword()}
  @type get_result ::
          {:ok, {binary(), binary() | nil}} | {:ok, :not_found} | {:error, :unavailable}
  @type put_result :: :ok | {:error, :precondition_failed | :unavailable}
  @type list_keys_page_result ::
          {:ok,
           %{
             keys: [String.t()],
             next_continuation_token: String.t() | nil,
             truncated?: boolean()
           }}
          | {:error, :unavailable}

  @spec get_object(config(), String.t()) :: get_result()
  def get_object(config, key) do
    case request(config, S3.get_object(config.bucket, key)) do
      {:ok, %{status_code: status, body: body, headers: headers}}
      when status in 200..299 and is_binary(body) ->
        {:ok, {body, etag(headers)}}

      {:error, {:http_error, 404, _error}} ->
        {:ok, :not_found}

      {:error, _reason} ->
        {:error, :unavailable}
    end
  end

  @spec put_object(config(), String.t(), binary(), keyword()) :: put_result()
  def put_object(config, key, body, opts \\ []) do
    case request(config, S3.put_object(config.bucket, key, body, opts)) do
      {:ok, %{status_code: status}} when status in 200..299 ->
        :ok

      {:error, {:http_error, status, _error}} when status in [409, 412] ->
        {:error, :precondition_failed}

      {:error, _reason} ->
        {:error, :unavailable}
    end
  end

  @spec list_keys(config(), String.t()) :: {:ok, [String.t()]} | {:error, :unavailable}
  def list_keys(config, prefix) do
    do_list_keys(config, prefix, nil, [])
  end

  @spec list_keys_page(config(), String.t(), keyword()) :: list_keys_page_result()
  def list_keys_page(config, prefix, opts \\ []) do
    max_keys = Keyword.get(opts, :max_keys, 1_000)

    request_opts =
      [prefix: prefix, max_keys: max_keys]
      |> maybe_put_opt(:start_after, Keyword.get(opts, :start_after))
      |> maybe_put_opt(:continuation_token, Keyword.get(opts, :continuation_token))

    case request(config, S3.list_objects_v2(config.bucket, request_opts)) do
      {:ok, %{body: body}} when is_map(body) ->
        {:ok,
         %{
           keys:
             body
             |> Map.get(:contents, [])
             |> Enum.map(&Map.get(&1, :key))
             |> Enum.reject(&(&1 in [nil, ""])),
           next_continuation_token: blank_to_nil(Map.get(body, :next_continuation_token)),
           truncated?: truthy?(Map.get(body, :is_truncated))
         }}

      {:error, _reason} ->
        {:error, :unavailable}

      _other ->
        {:error, :unavailable}
    end
  end

  defp request(config, operation) do
    ExAws.request(operation, config.request_opts)
  rescue
    _ -> {:error, :unavailable}
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp etag(headers) do
    Enum.find_value(headers, fn
      {name, value} when is_binary(name) -> if String.downcase(name) == "etag", do: value
      _ -> nil
    end)
  end

  defp do_list_keys(config, prefix, continuation_token, acc) do
    case list_keys_page(config, prefix, continuation_token: continuation_token) do
      {:ok, %{keys: keys, next_continuation_token: nil}} ->
        {:ok, Enum.reverse(keys, acc)}

      {:ok, %{keys: keys, next_continuation_token: next_token}} ->
        do_list_keys(config, prefix, next_token, Enum.reverse(keys, acc))

      {:error, :unavailable} ->
        {:error, :unavailable}
    end
  end

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, _key, ""), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp blank_to_nil(nil), do: nil
  defp blank_to_nil(""), do: nil
  defp blank_to_nil(value), do: value

  defp truthy?(value) when value in [true, "true", "TRUE", "True"], do: true
  defp truthy?(_value), do: false
end
