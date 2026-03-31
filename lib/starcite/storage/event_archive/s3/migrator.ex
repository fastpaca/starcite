defmodule Starcite.Storage.EventArchive.S3.Migrator do
  @moduledoc """
  Manual S3 schema migrator for archive objects.

  The migrator scans archive prefixes and rewrites legacy object payloads to the
  current schema versions defined in `Starcite.Storage.EventArchive.S3.Schema`.
  """

  alias Starcite.Storage.EventArchive.S3.{Layout, Schema}

  @type stats :: %{
          required(:dry_run) => boolean(),
          required(:event_scanned) => non_neg_integer(),
          required(:event_migrations_needed) => non_neg_integer(),
          required(:event_rewritten) => non_neg_integer()
        }

  @spec run(map(), keyword()) :: {:ok, stats()} | {:error, term()}
  def run(config, opts \\ []) when is_map(config) and is_list(opts) do
    dry_run = Keyword.get(opts, :dry_run, false)

    with {:ok, stats} <- migrate_event_chunks(config, dry_run, base_stats(dry_run)) do
      {:ok, stats}
    end
  end

  defp migrate_event_chunks(config, dry_run, stats)
       when is_boolean(dry_run) and is_map(stats) do
    with {:ok, keys} <- list_keys(config, Layout.event_prefix(config)) do
      keys
      |> Enum.reduce_while({:ok, stats}, fn key, {:ok, stats} ->
        expected_tenant_id = expected_tenant_for_event_key(config, key)

        case migrate_event_chunk(config, key, expected_tenant_id, dry_run, stats) do
          {:ok, stats} -> {:cont, {:ok, stats}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp migrate_event_chunk(config, key, expected_tenant_id, dry_run, stats)
       when is_binary(key) and is_boolean(dry_run) and is_map(stats) do
    stats = increment(stats, :event_scanned)

    with {:ok, body, etag} <- get_object(config, key),
         {:ok, events, migration_required} <- Schema.decode_event_chunk(body, expected_tenant_id),
         {:ok, stats} <-
           maybe_rewrite(
             config,
             key,
             Schema.encode_event_chunk(events),
             etag,
             "application/x-ndjson",
             dry_run,
             migration_required,
             :event_migrations_needed,
             :event_rewritten,
             stats
           ) do
      {:ok, stats}
    else
      {:error, _reason} = error -> error
    end
  end

  defp maybe_rewrite(
         _config,
         _key,
         _body,
         _etag,
         _content_type,
         _dry_run,
         false,
         _needed_key,
         _rewritten_key,
         stats
       ),
       do: {:ok, stats}

  defp maybe_rewrite(
         config,
         key,
         body,
         etag,
         content_type,
         dry_run,
         true,
         needed_key,
         rewritten_key,
         stats
       ) do
    stats = increment(stats, needed_key)

    if dry_run do
      {:ok, stats}
    else
      case client(config).put_object(config, key, body,
             content_type: content_type,
             if_match: etag
           ) do
        :ok ->
          {:ok, increment(stats, rewritten_key)}

        {:error, :precondition_failed} ->
          {:error, {:migration_precondition_failed, key}}

        {:error, :unavailable} ->
          {:error, :archive_write_unavailable}
      end
    end
  end

  defp expected_tenant_for_event_key(config, key) when is_binary(key) do
    case event_key_segments(config, key) do
      [tenant_segment, _session_segment, _chunk_segment] ->
        decode_base64url(tenant_segment)

      _ ->
        nil
    end
  end

  defp event_key_segments(config, key) when is_binary(key) do
    prefix = Layout.event_prefix(config)

    if String.starts_with?(key, prefix) do
      key
      |> String.replace_prefix(prefix, "")
      |> String.split("/", trim: true)
    else
      []
    end
  end

  defp decode_base64url(segment) when is_binary(segment) and segment != "" do
    case Base.url_decode64(segment, padding: false) do
      {:ok, value} when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp decode_base64url(_segment), do: nil

  defp list_keys(config, prefix) when is_binary(prefix) do
    case client(config).list_keys(config, prefix) do
      {:ok, keys} -> {:ok, keys}
      {:error, :unavailable} -> {:error, :archive_read_unavailable}
    end
  end

  defp get_object(config, key) when is_binary(key) do
    case client(config).get_object(config, key) do
      {:ok, {body, etag}} when is_binary(body) and is_binary(etag) ->
        {:ok, body, etag}

      {:ok, {body, _etag}} when is_binary(body) ->
        {:error, {:missing_object_etag, key}}

      {:ok, :not_found} ->
        {:error, {:object_not_found, key}}

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp increment(stats, key) when is_map(stats) and is_atom(key) do
    Map.update!(stats, key, &(&1 + 1))
  end

  defp base_stats(dry_run) when is_boolean(dry_run) do
    %{
      dry_run: dry_run,
      event_scanned: 0,
      event_migrations_needed: 0,
      event_rewritten: 0
    }
  end

  defp client(%{client_mod: client_mod}), do: client_mod
end
