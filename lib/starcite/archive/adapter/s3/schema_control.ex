defmodule Starcite.Archive.Adapter.S3.SchemaControl do
  @moduledoc """
  Control-plane for S3 archive schema compatibility and manual migrations.

  Startup path only validates compatibility. Migrations are manual via
  `SchemaControl.migrate/2` and the corresponding Mix task.
  """

  alias Starcite.Archive.Adapter.S3.{Layout, Migrator, Schema}

  @manifest_version 1
  @migration_status_running "running"
  @migration_status_succeeded "succeeded"
  @migration_status_failed "failed"

  @spec ensure_startup_compatibility(map()) :: :ok | {:error, term()}
  def ensure_startup_compatibility(config) when is_map(config) do
    with {:ok, manifest, _etag} <- fetch_manifest(config) do
      case manifest_state(manifest) do
        :unsupported -> {:error, :archive_schema_version_unsupported}
        :ready -> :ok
        :needs_migration -> {:error, :archive_schema_migration_required}
        :migrating -> {:error, :archive_schema_migration_in_progress}
        :invalid -> {:error, :archive_schema_manifest_invalid}
      end
    end
  end

  @spec migrate(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def migrate(config, opts \\ []) when is_map(config) and is_list(opts) do
    dry_run = Keyword.get(opts, :dry_run, false)
    actor = Keyword.get(opts, :actor, "manual")

    if dry_run do
      Migrator.run(config, dry_run: true)
    else
      run_manual_migration(config, actor)
    end
  end

  defp run_manual_migration(config, actor) when is_binary(actor) and actor != "" do
    with {:ok, manifest, etag} <- fetch_manifest(config) do
      case manifest_state(manifest) do
        :unsupported ->
          {:error, :archive_schema_version_unsupported}

        :ready ->
          {:ok, zero_stats(false)}

        :migrating ->
          {:error, :archive_schema_migration_in_progress}

        :invalid ->
          {:error, :archive_schema_manifest_invalid}

        :needs_migration ->
          run_managed_migration(config, manifest, etag, actor)
      end
    end
  end

  defp run_managed_migration(config, manifest, etag, actor)
       when is_map(manifest) and is_binary(actor) and actor != "" do
    with {:ok, migration_id} <- start_migration(config, manifest, etag, actor) do
      case Migrator.run(config) do
        {:ok, stats} ->
          with :ok <- finalize_migration_success(config, migration_id, stats) do
            {:ok, stats}
          end

        {:error, reason} ->
          _ = finalize_migration_failure(config, reason)
          {:error, {:archive_schema_migration_failed, reason}}
      end
    else
      :conflict ->
        {:error, :archive_schema_migration_in_progress}

      {:error, _reason} = error ->
        error
    end
  end

  defp start_migration(config, manifest, etag, actor)
       when is_map(config) and is_map(manifest) and is_binary(actor) and actor != "" do
    migration_record =
      migration_record(actor, manifest.current_versions, Schema.current_versions())

    next_manifest =
      manifest
      |> Map.put(:state, "migrating")
      |> Map.put(:active_migration, migration_record)
      |> Map.put(:updated_at, now_iso8601())

    case put_manifest(config, next_manifest, etag) do
      :ok ->
        {:ok, migration_record.id}

      :conflict ->
        :conflict

      {:error, _reason} = error ->
        error
    end
  end

  defp finalize_migration_success(config, migration_id, stats)
       when is_binary(migration_id) and is_map(stats) do
    with {:ok, manifest, etag} <- fetch_manifest(config),
         true <- migration_matches?(manifest, migration_id),
         :ok <- put_manifest(config, success_manifest(manifest, stats), etag) do
      :ok
    else
      false -> {:error, :archive_schema_manifest_conflict}
      :conflict -> {:error, :archive_schema_manifest_conflict}
      {:error, _reason} = error -> error
    end
  end

  defp finalize_migration_failure(config, reason) do
    with {:ok, manifest, etag} <- fetch_manifest(config),
         :ok <- put_manifest(config, failed_manifest(manifest, reason), etag) do
      :ok
    else
      :conflict -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp manifest_state(manifest) when is_map(manifest) do
    target_versions = Schema.current_versions()

    case version_relation(manifest.current_versions, target_versions) do
      :ahead ->
        :unsupported

      :equal ->
        if manifest.state == "ready", do: :ready, else: :migrating

      :behind ->
        if manifest.state == "migrating", do: :migrating, else: :needs_migration

      :invalid ->
        :invalid
    end
  end

  defp success_manifest(manifest, stats) when is_map(manifest) and is_map(stats) do
    migration =
      migration_summary(manifest.active_migration)
      |> Map.put(:status, @migration_status_succeeded)
      |> Map.put(:completed_at, now_iso8601())
      |> Map.put(:stats, stats)

    manifest
    |> Map.put(:state, "ready")
    |> Map.put(:current_versions, Schema.current_versions())
    |> Map.put(:active_migration, nil)
    |> Map.put(:last_migration, migration)
    |> Map.put(:updated_at, now_iso8601())
  end

  defp failed_manifest(manifest, reason) when is_map(manifest) do
    migration =
      migration_summary(manifest.active_migration)
      |> Map.put(:status, @migration_status_failed)
      |> Map.put(:completed_at, now_iso8601())
      |> Map.put(:error, inspect(reason))

    manifest
    |> Map.put(:state, "ready")
    |> Map.put(:active_migration, nil)
    |> Map.put(:last_migration, migration)
    |> Map.put(:updated_at, now_iso8601())
  end

  defp migration_summary(nil), do: %{id: migration_id(), started_at: now_iso8601()}

  defp migration_summary(migration) when is_map(migration) do
    %{
      id: Map.get(migration, :id, migration_id()),
      actor: Map.get(migration, :actor, "unknown"),
      node: Map.get(migration, :node, Atom.to_string(node())),
      from_versions: Map.get(migration, :from_versions, Schema.baseline_versions()),
      to_versions: Map.get(migration, :to_versions, Schema.current_versions()),
      started_at: Map.get(migration, :started_at, now_iso8601())
    }
  end

  defp migration_record(actor, from_versions, to_versions)
       when is_binary(actor) and is_map(from_versions) and is_map(to_versions) do
    %{
      id: migration_id(),
      actor: actor,
      node: Atom.to_string(node()),
      status: @migration_status_running,
      from_versions: from_versions,
      to_versions: to_versions,
      started_at: now_iso8601()
    }
  end

  defp migration_matches?(manifest, migration_id)
       when is_map(manifest) and is_binary(migration_id) and migration_id != "" do
    case manifest.active_migration do
      %{id: ^migration_id} -> true
      _ -> false
    end
  end

  defp version_relation(current_versions, target_versions)
       when is_map(current_versions) and is_map(target_versions) do
    if Map.keys(target_versions) != Map.keys(current_versions) do
      :invalid
    else
      has_ahead? =
        Enum.any?(target_versions, fn {kind, target_version} ->
          current_version = Map.fetch!(current_versions, kind)
          current_version > target_version
        end)

      has_behind? =
        Enum.any?(target_versions, fn {kind, target_version} ->
          current_version = Map.fetch!(current_versions, kind)
          current_version < target_version
        end)

      cond do
        has_ahead? -> :ahead
        has_behind? -> :behind
        true -> :equal
      end
    end
  end

  defp fetch_manifest(config) when is_map(config) do
    key = Layout.schema_meta_key(config)

    case client(config).get_object(config, key) do
      {:ok, :not_found} ->
        {:ok, default_manifest(), nil}

      {:ok, {body, etag}} when is_binary(body) and is_binary(etag) ->
        with {:ok, manifest} <- decode_manifest(body) do
          {:ok, manifest, etag}
        end

      {:ok, {body, _etag}} when is_binary(body) ->
        with {:ok, manifest} <- decode_manifest(body) do
          {:ok, manifest, nil}
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp put_manifest(config, manifest, nil) when is_map(manifest) do
    key = Layout.schema_meta_key(config)
    body = encode_manifest(manifest)

    case client(config).put_object(config, key, body,
           content_type: "application/json",
           if_none_match: "*"
         ) do
      :ok -> :ok
      {:error, :precondition_failed} -> :conflict
      {:error, :unavailable} -> {:error, :archive_write_unavailable}
    end
  end

  defp put_manifest(config, manifest, etag)
       when is_map(manifest) and is_binary(etag) and etag != "" do
    key = Layout.schema_meta_key(config)
    body = encode_manifest(manifest)

    case client(config).put_object(config, key, body,
           content_type: "application/json",
           if_match: etag
         ) do
      :ok -> :ok
      {:error, :precondition_failed} -> :conflict
      {:error, :unavailable} -> {:error, :archive_write_unavailable}
    end
  end

  defp put_manifest(_config, _manifest, _etag),
    do: {:error, :archive_schema_manifest_etag_missing}

  defp default_manifest do
    %{
      manifest_version: @manifest_version,
      state: "ready",
      current_versions: Schema.baseline_versions(),
      updated_at: now_iso8601(),
      active_migration: nil,
      last_migration: nil
    }
  end

  defp decode_manifest(body) when is_binary(body) do
    with {:ok, decoded} <- Jason.decode(body),
         {:ok, manifest} <- normalize_manifest(decoded) do
      {:ok, manifest}
    else
      _ -> {:error, :archive_schema_manifest_invalid}
    end
  end

  defp normalize_manifest(decoded) when is_map(decoded) do
    with {:ok, manifest_version} <- normalize_manifest_version(decoded),
         {:ok, state} <- normalize_manifest_state(decoded),
         {:ok, current_versions} <- normalize_manifest_versions(decoded),
         {:ok, active_migration} <- normalize_optional_map(decoded, "active_migration"),
         {:ok, last_migration} <- normalize_optional_map(decoded, "last_migration") do
      {:ok,
       %{
         manifest_version: manifest_version,
         state: state,
         current_versions: current_versions,
         updated_at: Map.get(decoded, "updated_at", now_iso8601()),
         active_migration: normalize_migration_map(active_migration),
         last_migration: normalize_migration_map(last_migration)
       }}
    else
      _ -> {:error, :archive_schema_manifest_invalid}
    end
  end

  defp normalize_manifest(_decoded), do: {:error, :archive_schema_manifest_invalid}

  defp normalize_manifest_version(%{"manifest_version" => version})
       when is_integer(version) and version > 0 and version <= @manifest_version,
       do: {:ok, version}

  defp normalize_manifest_version(%{}), do: {:ok, @manifest_version}

  defp normalize_manifest_state(%{"state" => state}) when state in ["ready", "migrating"],
    do: {:ok, state}

  defp normalize_manifest_state(%{}), do: {:ok, "ready"}

  defp normalize_manifest_versions(%{"current_versions" => versions}) when is_map(versions) do
    baseline_versions = Schema.baseline_versions()

    versions
    |> Enum.reduce_while({:ok, %{}}, fn {kind, _value}, {:ok, acc} ->
      case normalize_version_key(kind) do
        {:ok, key} -> {:cont, {:ok, Map.put_new(acc, key, Map.get(versions, kind))}}
        {:error, _reason} -> {:halt, {:error, :archive_schema_manifest_invalid}}
      end
    end)
    |> case do
      {:ok, normalized} ->
        baseline_versions
        |> Enum.reduce_while({:ok, %{}}, fn {kind, baseline_version}, {:ok, acc} ->
          case Map.get(normalized, kind, baseline_version) do
            version when is_integer(version) and version > 0 ->
              {:cont, {:ok, Map.put(acc, kind, version)}}

            _invalid_version ->
              {:halt, {:error, :archive_schema_manifest_invalid}}
          end
        end)

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_manifest_versions(%{}), do: {:ok, Schema.baseline_versions()}

  defp normalize_optional_map(decoded, key) when is_map(decoded) and is_binary(key) do
    case Map.get(decoded, key) do
      nil -> {:ok, nil}
      map when is_map(map) -> {:ok, map}
      _invalid -> {:error, :archive_schema_manifest_invalid}
    end
  end

  defp normalize_migration_map(nil), do: nil

  defp normalize_migration_map(map) when is_map(map) do
    %{
      id: Map.get(map, "id", Map.get(map, :id, migration_id())),
      actor: Map.get(map, "actor", Map.get(map, :actor, "unknown")),
      node: Map.get(map, "node", Map.get(map, :node, Atom.to_string(node()))),
      status: Map.get(map, "status", Map.get(map, :status, @migration_status_running)),
      from_versions:
        normalize_version_map(Map.get(map, "from_versions", Map.get(map, :from_versions, %{}))),
      to_versions:
        normalize_version_map(Map.get(map, "to_versions", Map.get(map, :to_versions, %{}))),
      started_at: Map.get(map, "started_at", Map.get(map, :started_at, now_iso8601())),
      completed_at: Map.get(map, "completed_at", Map.get(map, :completed_at)),
      stats: Map.get(map, "stats", Map.get(map, :stats)),
      error: Map.get(map, "error", Map.get(map, :error))
    }
  end

  defp normalize_version_map(raw) when is_map(raw) do
    baseline_versions = Schema.baseline_versions()

    baseline_versions
    |> Enum.reduce(%{}, fn {kind, baseline_version}, acc ->
      value = Map.get(raw, Atom.to_string(kind), Map.get(raw, kind, baseline_version))

      if is_integer(value) and value > 0 do
        Map.put(acc, kind, value)
      else
        Map.put(acc, kind, baseline_version)
      end
    end)
  end

  defp normalize_version_map(_raw), do: Schema.baseline_versions()

  defp normalize_version_key(kind) when is_binary(kind) do
    case kind do
      "event_chunk" -> {:ok, :event_chunk}
      "session" -> {:ok, :session}
      "session_tenant_index" -> {:ok, :session_tenant_index}
      _ -> {:error, :archive_schema_manifest_invalid}
    end
  end

  defp normalize_version_key(kind) when is_atom(kind) do
    case kind do
      :event_chunk -> {:ok, :event_chunk}
      :session -> {:ok, :session}
      :session_tenant_index -> {:ok, :session_tenant_index}
      _ -> {:error, :archive_schema_manifest_invalid}
    end
  end

  defp encode_manifest(manifest) when is_map(manifest) do
    Jason.encode!(%{
      manifest_version: @manifest_version,
      state: manifest.state,
      current_versions: encode_versions(manifest.current_versions),
      updated_at: manifest.updated_at,
      active_migration: encode_migration(manifest.active_migration),
      last_migration: encode_migration(manifest.last_migration)
    })
  end

  defp encode_migration(nil), do: nil

  defp encode_migration(migration) when is_map(migration) do
    map = normalize_migration_map(migration)

    migration = %{
      id: map.id,
      actor: map.actor,
      node: map.node,
      status: map.status,
      from_versions: encode_versions(map.from_versions),
      to_versions: encode_versions(map.to_versions),
      started_at: map.started_at
    }

    migration
    |> maybe_put("completed_at", map.completed_at)
    |> maybe_put("stats", map.stats)
    |> maybe_put("error", map.error)
  end

  defp encode_versions(versions) when is_map(versions) do
    %{
      "event_chunk" => Map.fetch!(versions, :event_chunk),
      "session" => Map.fetch!(versions, :session),
      "session_tenant_index" => Map.fetch!(versions, :session_tenant_index)
    }
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp zero_stats(dry_run) when is_boolean(dry_run) do
    %{
      dry_run: dry_run,
      index_scanned: 0,
      index_migrations_needed: 0,
      index_rewritten: 0,
      session_scanned: 0,
      session_migrations_needed: 0,
      session_rewritten: 0,
      event_scanned: 0,
      event_migrations_needed: 0,
      event_rewritten: 0
    }
  end

  defp now_iso8601 do
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_iso8601()
  end

  defp migration_id, do: "mig-#{System.unique_integer([:positive, :monotonic])}"

  defp client(%{client_mod: client_mod}), do: client_mod
end
