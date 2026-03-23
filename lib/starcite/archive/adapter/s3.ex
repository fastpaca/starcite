defmodule Starcite.Archive.Adapter.S3 do
  @moduledoc """
  S3-backed archive adapter.

  Storage layout:
  - Events: `<prefix>/events/v1/<base64url(tenant_id)>/<base64url(session_id)>/<chunk_start>.ndjson`
  - Sessions: `<prefix>/sessions/v1/<base64url(tenant_id)>/<base64url(session_id)>.json`
  - Session tenant index: `<prefix>/session-tenants/v1/<base64url(session_id)>.json`

  Event objects are newline-delimited JSON (NDJSON), one event per line, with
  one object per cache-line chunk. Session objects are plain JSON maps.

  Writes are idempotent by `(session_id, seq)` via read/merge/conditional-write
  using ETag preconditions.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  alias Starcite.Auth.Principal
  alias __MODULE__.{Config, Layout, Schema, SchemaControl}

  @config_key {__MODULE__, :config}
  @tenant_cache_table __MODULE__.SessionTenantCache

  @impl true
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    runtime_opts = Application.get_env(:starcite, :archive_adapter_opts, [])
    config = Config.build!(runtime_opts, opts)

    case SchemaControl.ensure_startup_compatibility(config) do
      :ok ->
        :persistent_term.put(@config_key, config)
        {:ok, config}

      {:error, reason} ->
        {:stop, {:schema_control_failed, reason}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :persistent_term.erase(@config_key)
    :ok
  end

  @impl true
  def write_events(rows), do: write_events(rows, config!())

  @impl true
  def read_events(session_id, from_seq, to_seq),
    do: read_events(session_id, from_seq, to_seq, config!())

  @impl true
  def upsert_session(%{
        id: id,
        title: title,
        tenant_id: tenant_id,
        creator_principal: creator_principal,
        metadata: metadata,
        archived_seq: archived_seq,
        created_at: created_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and
             (is_struct(creator_principal, Principal) or is_map(creator_principal)) and
             is_map(metadata) and is_integer(archived_seq) and archived_seq >= 0 and
             (is_struct(created_at, DateTime) or (is_binary(created_at) and created_at != "")) do
    config = config!()

    session = %{
      id: id,
      title: title,
      tenant_id: tenant_id,
      creator_principal: creator_principal,
      metadata: metadata,
      archived_seq: archived_seq,
      created_at: created_at
    }

    with :ok <- ensure_session_tenant_index(config, id, tenant_id),
         result <- put_session(config, tenant_id, id, session) do
      case result do
        :ok -> :ok
        {:error, :precondition_failed} -> :ok
        {:error, :unavailable} -> {:error, :archive_write_unavailable}
      end
    end
  end

  @impl true
  def update_session_archived_seq(session_id, tenant_id, archived_seq)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(archived_seq) and archived_seq >= 0 do
    config = config!()

    with {:ok, key, session} <- load_session_for_update(config, session_id, tenant_id),
         updated_session <-
           session
           |> Map.put(:tenant_id, tenant_id)
           |> Map.put(:archived_seq, archived_seq),
         result <- put_session_by_key(config, key, updated_session) do
      case result do
        :ok -> :ok
        {:error, :unavailable} -> {:error, :archive_write_unavailable}
        {:error, _reason} -> {:error, :archive_write_unavailable}
      end
    else
      {:error, :unavailable} ->
        {:error, :archive_write_unavailable}

      {:error, _reason} ->
        {:error, :archive_write_unavailable}
    end
  end

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata} = query) do
    config = config!()
    tenant_id = Map.get(query, :tenant_id)

    with {:ok, keys} <- list_session_keys(config) do
      list_sessions_for_keys(keys, limit, cursor, metadata, tenant_id, config)
    end
  end

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata} = query) do
    config = config!()
    tenant_id = Map.get(query, :tenant_id)

    with {:ok, keys} <- session_keys_for_ids(ids, tenant_id, config) do
      list_sessions_for_keys(keys, limit, cursor, metadata, tenant_id, config)
    end
  end

  defp write_events([], _config), do: {:ok, 0}

  defp write_events(rows, config) do
    rows
    |> Layout.group_event_rows(config.chunk_size)
    |> Enum.reduce_while({:ok, 0}, fn {{tenant_id, session_id, chunk_start}, chunk_rows},
                                      {:ok, total} ->
      case write_chunk(tenant_id, session_id, chunk_start, chunk_rows, config, 1) do
        {:ok, inserted} -> {:cont, {:ok, total + inserted}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp write_chunk(tenant_id, session_id, chunk_start, rows, config, attempt) do
    with {:ok, expected_tenant_id} <- chunk_tenant_id(rows),
         true <- expected_tenant_id == tenant_id,
         :ok <- ensure_session_tenant_index(config, session_id, expected_tenant_id),
         {:ok, existing_events, etag} <-
           fetch_chunk(session_id, expected_tenant_id, chunk_start, config),
         {:ok, normalized_existing_events} <-
           normalize_existing_chunk_tenant(existing_events, expected_tenant_id),
         {merged, inserted} <- merge_chunk(normalized_existing_events, rows),
         {:ok, inserted} <-
           put_chunk(expected_tenant_id, session_id, chunk_start, merged, etag, inserted, config) do
      {:ok, inserted}
    else
      {:error, :precondition_failed} when attempt <= config.max_write_retries ->
        write_chunk(tenant_id, session_id, chunk_start, rows, config, attempt + 1)

      false ->
        {:error, :archive_write_unavailable}

      {:error, _reason} ->
        {:error, :archive_write_unavailable}
    end
  end

  defp merge_chunk(existing_events, rows) do
    existing_by_seq = Map.new(existing_events, &{&1.seq, &1})

    incoming_by_seq =
      rows
      |> Enum.sort_by(& &1.seq)
      |> Enum.reduce(%{}, fn row, acc ->
        Map.put_new(acc, row.seq, event_from_row(row))
      end)

    inserted =
      Enum.count(incoming_by_seq, fn {seq, _event} -> not Map.has_key?(existing_by_seq, seq) end)

    merged =
      incoming_by_seq
      |> Map.merge(existing_by_seq)
      |> Map.values()
      |> Enum.sort_by(& &1.seq)

    {merged, inserted}
  end

  defp chunk_tenant_id([%{tenant_id: tenant_id} | rest])
       when is_binary(tenant_id) and tenant_id != "" do
    if Enum.all?(rest, &match?(%{tenant_id: ^tenant_id}, &1)) do
      {:ok, tenant_id}
    else
      {:error, :archive_write_unavailable}
    end
  end

  defp chunk_tenant_id(_rows), do: {:error, :archive_write_unavailable}

  defp normalize_existing_chunk_tenant(events, expected_tenant_id)
       when is_list(events) and is_binary(expected_tenant_id) and expected_tenant_id != "" do
    events
    |> Enum.reduce_while({:ok, []}, fn event, {:ok, acc} ->
      case Map.get(event, :tenant_id) do
        ^expected_tenant_id ->
          {:cont, {:ok, [event | acc]}}

        tenant_id when tenant_id in [nil, ""] ->
          {:cont, {:ok, [Map.put(event, :tenant_id, expected_tenant_id) | acc]}}

        _other_tenant_id ->
          {:halt, {:error, :archive_write_unavailable}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  defp put_chunk(_tenant_id, _session_id, _chunk_start, _events, _etag, 0, _config), do: {:ok, 0}

  defp put_chunk(tenant_id, session_id, chunk_start, events, etag, inserted, config) do
    key = Layout.event_chunk_key(config, tenant_id, session_id, chunk_start)
    body = Schema.encode_event_chunk(events)

    case client(config).put_object(config, key, body, event_put_opts(etag)) do
      :ok -> {:ok, inserted}
      {:error, :precondition_failed} -> {:error, :precondition_failed}
      {:error, :unavailable} -> {:error, :archive_write_unavailable}
    end
  end

  defp read_events(session_id, from_seq, to_seq, config) do
    chunk_starts = Layout.chunk_starts_for_range(from_seq, to_seq, config.chunk_size)

    with {:ok, tenant_id} <- resolve_session_tenant(config, session_id),
         {:ok, chunks} <- read_chunks(session_id, tenant_id, chunk_starts, config) do
      events =
        chunks
        |> List.flatten()
        |> Enum.filter(fn event -> event.seq >= from_seq and event.seq <= to_seq end)

      {:ok, events}
    end
  end

  defp read_chunks(session_id, tenant_id, chunk_starts, config) do
    chunk_starts
    |> Enum.reduce_while({:ok, []}, fn chunk_start, {:ok, acc} ->
      case fetch_chunk(session_id, tenant_id, chunk_start, config) do
        {:ok, events, _etag} -> {:cont, {:ok, [events | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_chunk(session_id, tenant_id, chunk_start, config)
       when is_binary(session_id) and is_integer(chunk_start) and chunk_start > 0 do
    keys = event_chunk_keys(config, session_id, tenant_id, chunk_start)
    fetch_chunk_for_keys(keys, tenant_id, config)
  end

  defp fetch_chunk_for_keys([], _expected_tenant_id, _config), do: {:ok, [], nil}

  defp fetch_chunk_for_keys([key | rest], expected_tenant_id, config) do
    case client(config).get_object(config, key) do
      {:ok, :not_found} ->
        fetch_chunk_for_keys(rest, expected_tenant_id, config)

      {:ok, {body, etag}} ->
        with {:ok, events, _migration_required} <-
               Schema.decode_event_chunk(body, expected_tenant_id) do
          {:ok, events, etag}
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp put_session(config, tenant_id, session_id, session) do
    key = Layout.session_key(config, tenant_id, session_id)
    body = Schema.encode_session(session)

    client(config).put_object(config, key, body,
      content_type: "application/json",
      if_none_match: "*"
    )
  end

  defp put_session_by_key(config, key, session)
       when is_binary(key) and key != "" and is_map(session) do
    body = Schema.encode_session(session)

    client(config).put_object(config, key, body, content_type: "application/json")
  end

  defp load_session_for_update(config, session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    config
    |> session_keys_for_id(tenant_id, session_id)
    |> Enum.reduce_while({:ok, nil, nil}, fn key, _acc ->
      case load_session(key, config) do
        {:ok, nil} ->
          {:cont, {:ok, nil, nil}}

        {:ok, session} ->
          {:halt, {:ok, key, session}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, key, session} when is_binary(key) and is_map(session) ->
        {:ok, key, session}

      {:error, _reason} = error ->
        error

      _ ->
        {:error, :session_not_found}
    end
  end

  defp put_session_tenant_index(config, session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    key = Layout.session_tenant_index_key(config, session_id)
    body = Schema.encode_session_tenant_index(tenant_id)

    case client(config).put_object(config, key, body,
           content_type: "application/json",
           if_none_match: "*"
         ) do
      :ok ->
        cache_session_tenant(config, session_id, tenant_id)
        :ok

      {:error, :precondition_failed} ->
        case fetch_session_tenant_index(config, session_id) do
          {:ok, ^tenant_id} ->
            cache_session_tenant(config, session_id, tenant_id)
            :ok

          {:ok, _other_tenant_id} ->
            {:error, :archive_write_unavailable}

          {:error, _reason} ->
            {:error, :archive_write_unavailable}
        end

      {:error, :unavailable} ->
        {:error, :archive_write_unavailable}
    end
  end

  defp fetch_session_tenant_index(config, session_id)
       when is_binary(session_id) and session_id != "" do
    key = Layout.session_tenant_index_key(config, session_id)

    case client(config).get_object(config, key) do
      {:ok, :not_found} ->
        {:ok, nil}

      {:ok, {body, _etag}} ->
        case Schema.decode_session_tenant_index(body) do
          {:ok, tenant_id, _migration_required} ->
            cache_session_tenant(config, session_id, tenant_id)
            {:ok, tenant_id}

          {:error, _reason} = error ->
            error
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp resolve_session_tenant(config, session_id)
       when is_binary(session_id) and session_id != "" do
    case cached_session_tenant(config, session_id) do
      nil -> fetch_session_tenant_index(config, session_id)
      tenant_id -> {:ok, tenant_id}
    end
  end

  defp ensure_session_tenant_index(config, session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    case cached_session_tenant(config, session_id) do
      ^tenant_id ->
        :ok

      nil ->
        put_session_tenant_index(config, session_id, tenant_id)

      _other_tenant_id ->
        {:error, :archive_write_unavailable}
    end
  end

  defp cached_session_tenant(config, session_id)
       when is_binary(session_id) and session_id != "" do
    case :ets.lookup(tenant_cache_table(), tenant_cache_key(config, session_id)) do
      [{_key, tenant_id}] -> tenant_id
      [] -> nil
    end
  end

  defp cache_session_tenant(config, session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    true = :ets.insert(tenant_cache_table(), {tenant_cache_key(config, session_id), tenant_id})
    :ok
  end

  defp tenant_cache_key(config, session_id)
       when is_binary(session_id) and session_id != "" do
    {config.bucket, config.prefix, session_id}
  end

  defp tenant_cache_table do
    case :ets.whereis(@tenant_cache_table) do
      :undefined ->
        try do
          :ets.new(@tenant_cache_table, [
            :named_table,
            :set,
            :public,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> @tenant_cache_table
        end

      tid ->
        tid
    end
  end

  defp event_chunk_keys(config, session_id, tenant_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    case tenant_id do
      tenant_id when is_binary(tenant_id) and tenant_id != "" ->
        [
          Layout.event_chunk_key(config, tenant_id, session_id, chunk_start),
          Layout.legacy_event_chunk_key(config, session_id, chunk_start)
        ]

      _ ->
        [Layout.legacy_event_chunk_key(config, session_id, chunk_start)]
    end
  end

  defp session_keys_for_ids(ids, tenant_id, config) when is_list(ids) do
    session_ids = ids |> Enum.uniq() |> Enum.reject(&(&1 == ""))

    case tenant_id do
      tenant_id when is_binary(tenant_id) and tenant_id != "" ->
        {:ok,
         session_ids
         |> Enum.flat_map(&session_keys_for_id(config, tenant_id, &1))
         |> Enum.uniq()}

      nil ->
        session_ids
        |> Enum.reduce_while({:ok, []}, fn session_id, {:ok, keys_acc} ->
          case resolve_session_tenant(config, session_id) do
            {:ok, resolved_tenant_id} ->
              keys = session_keys_for_id(config, resolved_tenant_id, session_id)
              {:cont, {:ok, keys ++ keys_acc}}

            {:error, _reason} = error ->
              {:halt, error}
          end
        end)
        |> case do
          {:ok, keys} -> {:ok, Enum.uniq(keys)}
          {:error, _reason} = error -> error
        end

      _invalid_tenant_filter ->
        {:ok, []}
    end
  end

  defp session_keys_for_id(config, tenant_id, session_id)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" do
    [
      Layout.session_key(config, tenant_id, session_id),
      Layout.legacy_session_key(config, session_id)
    ]
  end

  defp session_keys_for_id(config, _tenant_id, session_id)
       when is_binary(session_id) and session_id != "" do
    [Layout.legacy_session_key(config, session_id)]
  end

  defp list_session_keys(config) do
    case client(config).list_keys(config, Layout.session_prefix(config)) do
      {:ok, keys} -> {:ok, keys}
      {:error, :unavailable} -> {:error, :archive_read_unavailable}
    end
  end

  defp list_sessions_for_keys(keys, limit, cursor, metadata, tenant_id, config) do
    with {:ok, sessions} <- load_sessions(Enum.uniq(keys), config) do
      {:ok, session_page(sessions, limit, cursor, metadata, tenant_id)}
    end
  end

  defp load_sessions(keys, config) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, sessions} ->
      case load_session(key, config) do
        {:ok, nil} -> {:cont, {:ok, sessions}}
        {:ok, session} -> {:cont, {:ok, [session | sessions]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp load_session(key, config) do
    case client(config).get_object(config, key) do
      {:ok, :not_found} ->
        {:ok, nil}

      {:ok, {body, _etag}} ->
        case Schema.decode_session(body) do
          {:ok, session, _migration_required} ->
            {:ok, session}

          {:error, _reason} = error ->
            error
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp session_page(sessions, limit, cursor, metadata_filters, tenant_id_filter) do
    filtered =
      sessions
      |> dedupe_sessions()
      |> Enum.sort_by(& &1.id)
      |> Enum.filter(fn session ->
        (is_nil(cursor) or session.id > cursor) and
          (is_nil(tenant_id_filter) or session.tenant_id == tenant_id_filter) and
          Enum.all?(metadata_filters, fn {key, expected} -> session.metadata[key] == expected end)
      end)

    {page_sessions, rest} = Enum.split(filtered, limit)

    %{
      sessions: page_sessions,
      next_cursor:
        if(rest == [] or page_sessions == [], do: nil, else: List.last(page_sessions).id)
    }
  end

  defp dedupe_sessions(sessions) when is_list(sessions) do
    sessions
    |> Enum.reduce(%{}, fn %{id: id} = session, acc ->
      Map.put_new(acc, id, session)
    end)
    |> Map.values()
  end

  defp event_put_opts(nil), do: [content_type: "application/x-ndjson", if_none_match: "*"]
  defp event_put_opts(etag), do: [content_type: "application/x-ndjson", if_match: etag]

  defp event_from_row(row), do: Map.delete(row, :session_id)

  defp config!, do: :persistent_term.get(@config_key)
  defp client(%{client_mod: client_mod}), do: client_mod
end
