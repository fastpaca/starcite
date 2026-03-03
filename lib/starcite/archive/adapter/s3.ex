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
  def upsert_session(
        %{
          id: id,
          title: title,
          tenant_id: tenant_id,
          creator_principal: creator_principal,
          metadata: metadata,
          created_at: created_at
        } = session
      )
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             is_binary(tenant_id) and tenant_id != "" and
             (is_struct(creator_principal, Principal) or is_map(creator_principal)) and
             is_map(metadata) do
    config = config!()
    runtime_snapshot_explicit? = runtime_snapshot_explicit?(session)
    runtime = normalize_runtime_snapshot(session)

    session =
      %{
        id: id,
        title: title,
        tenant_id: tenant_id,
        creator_principal: creator_principal,
        metadata: metadata,
        created_at: created_at
      }
      |> Map.merge(runtime)

    upsert_session_snapshot(config, tenant_id, id, session, 1, runtime_snapshot_explicit?)
  end

  defp upsert_session_snapshot(
         config,
         tenant_id,
         session_id,
         incoming,
         attempt,
         runtime_snapshot_explicit?
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" and is_map(incoming) and is_integer(attempt) and attempt > 0 and
              is_boolean(runtime_snapshot_explicit?) do
    with {:ok, resolved_tenant_id} <- fetch_session_tenant_index(config, session_id) do
      lookup_tenant_id = resolved_tenant_id || tenant_id

      with {:ok, existing, etag} <- fetch_session_snapshot(config, lookup_tenant_id, session_id) do
        if runtime_newer?(incoming, existing) do
          case tenant_ownership_status(resolved_tenant_id, tenant_id) do
            :ok ->
              with :ok <- put_session_tenant_index(config, session_id, tenant_id) do
                case put_session(config, tenant_id, session_id, incoming, etag) do
                  :ok ->
                    :ok

                  {:error, :precondition_failed} when attempt <= config.max_write_retries ->
                    upsert_session_snapshot(
                      config,
                      tenant_id,
                      session_id,
                      incoming,
                      attempt + 1,
                      runtime_snapshot_explicit?
                    )

                  {:error, :precondition_failed} ->
                    :ok

                  {:error, :unavailable} ->
                    {:error, :archive_write_unavailable}
                end
              end

            :conflict ->
              {:error, :archive_write_unavailable}
          end
        else
          case tenant_ownership_status(resolved_tenant_id, tenant_id) do
            :ok ->
              :ok

            :conflict when runtime_snapshot_explicit? ->
              :ok

            :conflict ->
              {:error, :archive_write_unavailable}
          end
        end
      else
        {:error, :archive_read_unavailable} ->
          {:error, :archive_write_unavailable}
      end
    else
      {:error, :archive_read_unavailable} ->
        {:error, :archive_write_unavailable}
    end
  end

  defp tenant_ownership_status(nil, incoming_tenant_id)
       when is_binary(incoming_tenant_id) and incoming_tenant_id != "",
       do: :ok

  defp tenant_ownership_status(existing_tenant_id, existing_tenant_id)
       when is_binary(existing_tenant_id) and existing_tenant_id != "",
       do: :ok

  defp tenant_ownership_status(existing_tenant_id, incoming_tenant_id)
       when is_binary(existing_tenant_id) and existing_tenant_id != "" and
              is_binary(incoming_tenant_id) and incoming_tenant_id != "",
       do: :conflict

  defp runtime_snapshot_explicit?(session) when is_map(session) do
    Enum.any?(
      [
        :last_seq,
        :archived_seq,
        :retention,
        :producer_cursors,
        :last_progress_poll,
        :snapshot_version
      ],
      fn key ->
        Map.has_key?(session, key) or Map.has_key?(session, Atom.to_string(key))
      end
    )
  end

  defp fetch_session_snapshot(config, tenant_id, session_id)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(session_id) and
              session_id != "" do
    current_key = Layout.session_key(config, tenant_id, session_id)
    legacy_key = Layout.legacy_session_key(config, session_id)

    with {:ok, current} <- fetch_session_snapshot_key(config, current_key),
         {:ok, legacy} <- fetch_session_snapshot_key(config, legacy_key) do
      case current do
        {session, etag} ->
          {:ok, session, etag}

        nil ->
          case legacy do
            {session, _etag} -> {:ok, session, nil}
            nil -> {:ok, nil, nil}
          end
      end
    end
  end

  defp fetch_session_snapshot_key(config, key) when is_binary(key) do
    case client(config).get_object(config, key) do
      {:ok, :not_found} ->
        {:ok, nil}

      {:ok, {body, etag}} ->
        case Schema.decode_session(body) do
          {:ok, session, _migration_required} -> {:ok, {session, etag}}
          {:error, _reason} -> {:error, :archive_read_unavailable}
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp runtime_newer?(_incoming, nil), do: true

  defp runtime_newer?(incoming, existing) when is_map(incoming) and is_map(existing) do
    runtime_tuple(incoming) > runtime_tuple(existing)
  end

  defp runtime_tuple(session) when is_map(session) do
    {
      session_runtime_value(session, :last_seq),
      session_runtime_value(session, :last_progress_poll),
      session_runtime_value(session, :archived_seq)
    }
  end

  defp session_runtime_value(session, key) when is_map(session) and is_atom(key) do
    case Map.get(session, key, Map.get(session, Atom.to_string(key), 0)) do
      value when is_integer(value) and value >= 0 -> value
      _ -> 0
    end
  end

  defp normalize_runtime_snapshot(session) when is_map(session) do
    last_seq = non_neg_integer_snapshot(session, :last_seq, 0)
    archived_seq = non_neg_integer_snapshot(session, :archived_seq, 0)
    last_progress_poll = non_neg_integer_snapshot(session, :last_progress_poll, 0)
    retention = map_snapshot(session, :retention, %{})
    producer_cursors = map_snapshot(session, :producer_cursors, %{}) |> encode_producer_cursors()
    snapshot_version = snapshot_version(session)

    if archived_seq <= last_seq do
      %{
        last_seq: last_seq,
        archived_seq: archived_seq,
        retention: retention,
        producer_cursors: producer_cursors,
        last_progress_poll: last_progress_poll,
        snapshot_version: snapshot_version
      }
    else
      raise ArgumentError,
            "invalid session runtime snapshot: archived_seq=#{archived_seq} > last_seq=#{last_seq}"
    end
  end

  defp non_neg_integer_snapshot(session, key, default)
       when is_map(session) and is_atom(key) and is_integer(default) and default >= 0 do
    case Map.get(session, key, default) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "invalid session #{key}: #{inspect(value)}"
    end
  end

  defp map_snapshot(session, key, default)
       when is_map(session) and is_atom(key) and is_map(default) do
    case Map.get(session, key, default) do
      value when is_map(value) -> value
      value -> raise ArgumentError, "invalid session #{key}: #{inspect(value)}"
    end
  end

  defp snapshot_version(session) when is_map(session) do
    case Map.get(session, :snapshot_version) do
      nil -> nil
      value when is_binary(value) and value != "" -> value
      value -> raise ArgumentError, "invalid session snapshot_version: #{inspect(value)}"
    end
  end

  defp encode_producer_cursors(cursors) when map_size(cursors) == 0, do: %{}

  defp encode_producer_cursors(cursors) when is_map(cursors) do
    Enum.reduce(cursors, %{}, fn
      {producer_id, cursor}, acc
      when is_binary(producer_id) and producer_id != "" and is_map(cursor) ->
        producer_seq = cursor_integer!(cursor, :producer_seq)
        session_seq = cursor_integer!(cursor, :session_seq)
        hash = cursor_hash!(cursor)

        Map.put(acc, producer_id, %{
          producer_seq: producer_seq,
          session_seq: session_seq,
          hash: Base.url_encode64(hash, padding: false)
        })

      entry, _acc ->
        raise ArgumentError, "invalid producer cursor entry: #{inspect(entry)}"
    end)
  end

  defp cursor_integer!(cursor, key) when is_map(cursor) and is_atom(key) do
    case Map.get(cursor, key) || Map.get(cursor, Atom.to_string(key)) do
      value when is_integer(value) and value > 0 -> value
      value -> raise ArgumentError, "invalid producer cursor #{key}: #{inspect(value)}"
    end
  end

  defp cursor_hash!(cursor) when is_map(cursor) do
    case Map.get(cursor, :hash) || Map.get(cursor, "hash") do
      value when is_binary(value) and value != "" -> value
      value -> raise ArgumentError, "invalid producer cursor hash: #{inspect(value)}"
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
         :ok <- put_session_tenant_index(config, session_id, expected_tenant_id),
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

  defp put_session(config, tenant_id, session_id, session, nil) do
    key = Layout.session_key(config, tenant_id, session_id)
    body = Schema.encode_session(session)

    client(config).put_object(config, key, body,
      content_type: "application/json",
      if_none_match: "*"
    )
  end

  defp put_session(config, tenant_id, session_id, session, etag)
       when is_binary(etag) and etag != "" do
    key = Layout.session_key(config, tenant_id, session_id)
    body = Schema.encode_session(session)

    client(config).put_object(config, key, body,
      content_type: "application/json",
      if_match: etag
    )
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
        :ok

      {:error, :precondition_failed} ->
        case fetch_session_tenant_index(config, session_id) do
          {:ok, ^tenant_id} -> :ok
          {:ok, _other_tenant_id} -> {:error, :archive_write_unavailable}
          {:error, _reason} -> {:error, :archive_write_unavailable}
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
    fetch_session_tenant_index(config, session_id)
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
      Layout.legacy_session_key(config, session_id),
      Layout.session_key(config, tenant_id, session_id)
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
