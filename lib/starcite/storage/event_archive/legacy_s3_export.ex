defmodule Starcite.Storage.EventArchive.LegacyS3Export do
  @moduledoc """
  Decodes archived NDJSON chunks exported from the legacy S3 archive layout.

  This module does not talk to S3 directly. It expects an exported directory
  tree containing files under either:

  - `events/v1/<tenant>/<session>/<chunk>.ndjson`
  - `events/v1/<session>/<chunk>.ndjson`
  """

  @current_event_schema_version 2

  @type export_file :: %{
          required(:path) => String.t(),
          optional(:tenant_id) => String.t(),
          required(:session_id) => String.t(),
          required(:chunk_start) => pos_integer()
        }

  @spec exported_files(String.t()) :: {:ok, [export_file()]} | {:error, term()}
  def exported_files(root) when is_binary(root) and root != "" do
    if File.dir?(root) do
      files =
        root
        |> Path.join("**/*.ndjson")
        |> Path.wildcard()
        |> Enum.map(&export_file!/1)
        |> Enum.sort_by(fn %{session_id: session_id, chunk_start: chunk_start} = file ->
          {session_id, chunk_start, Map.get(file, :tenant_id, "")}
        end)

      {:ok, files}
    else
      {:error, :archive_export_root_missing}
    end
  rescue
    _ -> {:error, :archive_export_invalid}
  end

  @spec read_file(export_file(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def read_file(file, opts \\ [])

  def read_file(%{path: path, session_id: session_id} = file, opts)
      when is_binary(path) and path != "" and is_binary(session_id) and session_id != "" and
             is_list(opts) do
    with {:ok, tenant_id} <- read_tenant_id(file, opts) do
      do_read_file(path, session_id, tenant_id)
    end
  end

  def read_file(_file, _opts), do: {:error, :archive_export_invalid}

  defp do_read_file(path, session_id, tenant_id)
       when is_binary(path) and path != "" and is_binary(session_id) and session_id != "" and
              is_binary(tenant_id) and tenant_id != "" do
    case File.open(path, [:read]) do
      {:ok, device} ->
        try do
          device
          |> IO.stream(:line)
          |> Enum.reduce_while({:ok, []}, fn line, {:ok, rows} ->
            trimmed = String.trim(line)

            if trimmed == "" do
              {:cont, {:ok, rows}}
            else
              case decode_event_line(trimmed, session_id, tenant_id) do
                {:ok, row} ->
                  {:cont, {:ok, [row | rows]}}

                {:error, _reason} = error ->
                  {:halt, error}
              end
            end
          end)
          |> case do
            {:ok, rows} -> {:ok, Enum.reverse(rows)}
            {:error, _reason} = error -> error
          end
        after
          File.close(device)
        end

      {:error, _reason} ->
        {:error, :archive_export_invalid}
    end
  rescue
    _ -> {:error, :archive_export_invalid}
  end

  @spec export_file!(String.t()) :: export_file()
  def export_file!(path) when is_binary(path) and path != "" do
    case Path.split(path) |> Enum.take(-5) do
      ["events", "v1", tenant_segment, session_segment, chunk_file] ->
        %{
          path: path,
          tenant_id: Base.url_decode64!(tenant_segment, padding: false),
          session_id: Base.url_decode64!(session_segment, padding: false),
          chunk_start: chunk_start!(chunk_file)
        }

      _other ->
        case Path.split(path) |> Enum.take(-4) do
          ["events", "v1", session_segment, chunk_file] ->
            %{
              path: path,
              session_id: Base.url_decode64!(session_segment, padding: false),
              chunk_start: chunk_start!(chunk_file)
            }

          _other ->
            raise ArgumentError, "unexpected legacy archive export path: #{inspect(path)}"
        end
    end
  end

  defp read_tenant_id(%{tenant_id: tenant_id}, opts)
       when is_binary(tenant_id) and tenant_id != "" and is_list(opts) do
    case Keyword.get(opts, :tenant_id) do
      nil ->
        {:ok, tenant_id}

      ^tenant_id ->
        {:ok, tenant_id}

      _other ->
        {:error, :archive_export_invalid}
    end
  end

  defp read_tenant_id(%{}, opts) when is_list(opts) do
    case Keyword.get(opts, :tenant_id) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" -> {:ok, tenant_id}
      _ -> {:error, :archive_export_missing_tenant}
    end
  end

  defp chunk_start!(chunk_file) when is_binary(chunk_file) do
    chunk_file
    |> String.trim_trailing(".ndjson")
    |> String.to_integer()
  rescue
    _ ->
      raise ArgumentError, "unexpected legacy archive export chunk file: #{inspect(chunk_file)}"
  end

  defp decode_event_line(line, session_id, expected_tenant_id)
       when is_binary(line) and line != "" and is_binary(session_id) and session_id != "" and
              is_binary(expected_tenant_id) and expected_tenant_id != "" do
    with {:ok, decoded} <- Jason.decode(line),
         :ok <- validate_schema_version(decoded),
         {:ok, tenant_id} <- normalize_tenant_id(decoded, expected_tenant_id),
         {:ok, inserted_at} <- parse_inserted_at(Map.get(decoded, "inserted_at")),
         {:ok, row} <- normalize_event_row(decoded, session_id, tenant_id, inserted_at) do
      {:ok, row}
    else
      _ -> {:error, :archive_export_invalid}
    end
  rescue
    _ -> {:error, :archive_export_invalid}
  end

  defp validate_schema_version(%{"schema_version" => version})
       when is_integer(version) and version >= 1 and version <= @current_event_schema_version,
       do: :ok

  defp validate_schema_version(%{"schema_version" => version})
       when is_integer(version) and version > @current_event_schema_version,
       do: {:error, :archive_export_invalid}

  defp validate_schema_version(%{"schema_version" => _invalid_version}),
    do: {:error, :archive_export_invalid}

  defp validate_schema_version(%{}), do: :ok
  defp validate_schema_version(_decoded), do: {:error, :archive_export_invalid}

  defp normalize_tenant_id(decoded, expected_tenant_id)
       when is_map(decoded) and is_binary(expected_tenant_id) and expected_tenant_id != "" do
    case Map.get(decoded, "tenant_id") do
      ^expected_tenant_id ->
        {:ok, expected_tenant_id}

      tenant_id when tenant_id in [nil, ""] ->
        {:ok, expected_tenant_id}

      _other_tenant_id ->
        {:error, :archive_export_invalid}
    end
  end

  defp parse_inserted_at(inserted_at) when is_binary(inserted_at) do
    case NaiveDateTime.from_iso8601(inserted_at) do
      {:ok, naive_datetime} ->
        {:ok, naive_datetime}

      {:error, _reason} ->
        case DateTime.from_iso8601(inserted_at) do
          {:ok, datetime, _offset} -> {:ok, DateTime.to_naive(datetime)}
          _ -> {:error, :archive_export_invalid}
        end
    end
  end

  defp parse_inserted_at(_inserted_at), do: {:error, :archive_export_invalid}

  defp normalize_event_row(
         %{
           "seq" => seq,
           "type" => type,
           "payload" => payload,
           "actor" => actor,
           "producer_id" => producer_id,
           "producer_seq" => producer_seq,
           "source" => source,
           "metadata" => metadata,
           "refs" => refs,
           "idempotency_key" => idempotency_key
         },
         session_id,
         tenant_id,
         inserted_at
       )
       when is_integer(seq) and seq > 0 and is_binary(type) and type != "" and is_map(payload) and
              is_binary(actor) and actor != "" and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 and
              (is_binary(source) or is_nil(source)) and
              is_map(metadata) and is_map(refs) and
              (is_binary(idempotency_key) or is_nil(idempotency_key)) do
    {:ok,
     %{
       session_id: session_id,
       seq: seq,
       type: type,
       payload: payload,
       actor: actor,
       producer_id: producer_id,
       producer_seq: producer_seq,
       tenant_id: tenant_id,
       source: source,
       metadata: metadata,
       refs: refs,
       idempotency_key: idempotency_key,
       inserted_at: inserted_at
     }}
  end

  defp normalize_event_row(_decoded, _session_id, _tenant_id, _inserted_at),
    do: {:error, :archive_export_invalid}
end
