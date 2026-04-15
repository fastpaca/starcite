defmodule Starcite.Storage.EventArchive.S3.Client do
  @moduledoc """
  Thin wrapper around ExAws S3 operations used during legacy S3 cutover.
  """

  alias ExAws.S3

  @type config :: %{bucket: String.t(), request_opts: keyword()}
  @type get_result ::
          {:ok, {binary(), binary() | nil}} | {:ok, :not_found} | {:error, :unavailable}
  @type put_result :: :ok | {:error, :precondition_failed | :unavailable}

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
end
