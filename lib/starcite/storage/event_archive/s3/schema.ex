defmodule Starcite.Storage.EventArchive.S3.Schema do
  @moduledoc """
  Versioned object schema for legacy S3 archive blobs.

  Legacy blobs without an explicit `schema_version` are treated as v1 and can be
  decoded for backward compatibility. New cutover writes keep using the current
  tenant-scoped schema so the cutover window remains reversible.
  """

  @event_schema_version 2
  @baseline_event_schema_version 1

  @spec event_schema_version() :: pos_integer()
  def event_schema_version, do: @event_schema_version

  @spec current_versions() :: %{required(atom()) => pos_integer()}
  def current_versions do
    %{event_chunk: @event_schema_version}
  end

  @spec baseline_versions() :: %{required(atom()) => pos_integer()}
  def baseline_versions do
    %{event_chunk: @baseline_event_schema_version}
  end

  @spec encode_event_chunk([map()]) :: binary()
  def encode_event_chunk(events) when is_list(events) do
    Enum.map_join(events, "\n", fn event ->
      event
      |> Map.delete(:schema_version)
      |> Map.delete("schema_version")
      |> Map.put(:schema_version, @event_schema_version)
      |> Jason.encode!()
    end)
  end

  @spec decode_event_chunk(binary(), String.t() | nil) ::
          {:ok, [map()], boolean()} | {:error, :archive_read_unavailable}
  def decode_event_chunk(body, expected_tenant_id)
      when is_binary(body) and (is_nil(expected_tenant_id) or is_binary(expected_tenant_id)) do
    body
    |> String.split("\n", trim: true)
    |> Enum.reduce_while({:ok, [], false}, fn line, {:ok, acc, migration_required} ->
      case decode_event_line(line, expected_tenant_id) do
        {:ok, event, line_migration_required} ->
          {:cont, {:ok, [event | acc], migration_required or line_migration_required}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, events, migration_required} ->
        {:ok, Enum.reverse(events), migration_required}

      {:error, _reason} = error ->
        error
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp decode_event_line(line, expected_tenant_id) when is_binary(line) do
    with {:ok, decoded} <- Jason.decode(line),
         {:ok, migrated_payload, schema_migration_required} <- migrate_event_payload(decoded),
         {:ok, event, tenant_migration_required} <-
           decode_event_payload(migrated_payload, expected_tenant_id) do
      {:ok, event, schema_migration_required or tenant_migration_required}
    else
      _ -> {:error, :archive_read_unavailable}
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp migrate_event_payload(%{"schema_version" => version} = payload)
       when is_integer(version) and version == @event_schema_version do
    {:ok, payload, false}
  end

  defp migrate_event_payload(%{"schema_version" => version} = payload)
       when is_integer(version) and version > 0 and version < @event_schema_version do
    {:ok, Map.put(payload, "schema_version", @event_schema_version), true}
  end

  defp migrate_event_payload(%{"schema_version" => version})
       when is_integer(version) and version > @event_schema_version do
    {:error, :archive_read_unavailable}
  end

  defp migrate_event_payload(%{"schema_version" => _invalid_version}),
    do: {:error, :archive_read_unavailable}

  defp migrate_event_payload(payload) when is_map(payload) do
    {:ok, Map.put(payload, "schema_version", @event_schema_version), true}
  end

  defp migrate_event_payload(_payload), do: {:error, :archive_read_unavailable}

  defp decode_event_payload(
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
           "idempotency_key" => idempotency_key,
           "inserted_at" => inserted_at
         } = decoded,
         expected_tenant_id
       ) do
    with {:ok, tenant_id, tenant_migration_required} <-
           normalize_event_tenant(decoded, expected_tenant_id),
         {:ok, parsed_inserted_at} <- parse_inserted_at(inserted_at) do
      {:ok,
       %{
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
         inserted_at: parsed_inserted_at
       }, tenant_migration_required}
    else
      {:error, _reason} = error -> error
    end
  end

  defp decode_event_payload(_decoded, _expected_tenant_id),
    do: {:error, :archive_read_unavailable}

  defp normalize_event_tenant(decoded, expected_tenant_id)
       when is_map(decoded) and is_binary(expected_tenant_id) and expected_tenant_id != "" do
    case Map.get(decoded, "tenant_id") do
      ^expected_tenant_id ->
        {:ok, expected_tenant_id, false}

      tenant_id when tenant_id in [nil, ""] ->
        {:ok, expected_tenant_id, true}

      _other_tenant_id ->
        {:error, :archive_read_unavailable}
    end
  end

  defp normalize_event_tenant(decoded, _expected_tenant_id) when is_map(decoded) do
    case Map.get(decoded, "tenant_id") do
      tenant_id when is_binary(tenant_id) and tenant_id != "" ->
        {:ok, tenant_id, false}

      tenant_id when tenant_id in [nil, ""] ->
        {:error, :archive_read_unavailable}

      _invalid_tenant_id ->
        {:error, :archive_read_unavailable}
    end
  end

  defp parse_inserted_at(inserted_at) when is_binary(inserted_at) do
    inserted_at
    |> NaiveDateTime.from_iso8601()
    |> case do
      {:ok, parsed} -> {:ok, NaiveDateTime.truncate(parsed, :second)}
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp parse_inserted_at(%NaiveDateTime{} = inserted_at) do
    {:ok, NaiveDateTime.truncate(inserted_at, :second)}
  end

  defp parse_inserted_at(%DateTime{} = inserted_at) do
    {:ok, inserted_at |> DateTime.to_naive() |> NaiveDateTime.truncate(:second)}
  end

  defp parse_inserted_at(_inserted_at), do: {:error, :archive_read_unavailable}
end
