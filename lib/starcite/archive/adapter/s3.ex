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
  @ordered_list_batch_factor 4

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

    with :ok <- put_session_tenant_index(config, id, tenant_id),
         result <- put_session(config, tenant_id, id, session),
         :ok <- normalize_session_put_result(result),
         :ok <- put_session_order_indexes(config, id, tenant_id) do
      :ok
    else
      {:error, :unavailable} ->
        {:error, :archive_write_unavailable}

      {:error, _reason} ->
        {:error, :archive_write_unavailable}
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

    case session_order_ready?(config) do
      {:ok, true} ->
        list_sessions_from_ordered_index(limit, cursor, metadata, tenant_id, config)

      {:ok, false} ->
        with {:ok, keys} <- list_session_keys(config) do
          list_sessions_for_keys_with_backfill(keys, limit, cursor, metadata, tenant_id, config)
        end

      {:error, _reason} = error ->
        error
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

  def backfill_session_order_indexes, do: backfill_session_order_indexes(config!())

  def backfill_session_order_indexes(config) when is_map(config) do
    case session_order_ready?(config) do
      {:ok, true} ->
        {:ok,
         %{
           legacy_sessions: 0,
           ready_already?: true,
           sessions_total: 0,
           tenant_sessions: 0,
           tenants: 0
         }}

      {:ok, false} ->
        do_backfill_session_order_indexes(config)

      {:error, _reason} = error ->
        error
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

  defp put_session(config, tenant_id, session_id, session) do
    key = Layout.session_key(config, tenant_id, session_id)
    body = Schema.encode_session(session)

    client(config).put_object(config, key, body,
      content_type: "application/json",
      if_none_match: "*"
    )
  end

  defp put_session_order_indexes(config, session_id, tenant_id)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    [
      Layout.session_order_tenant_key(config, tenant_id, session_id),
      Layout.session_order_global_key(config, tenant_id, session_id)
    ]
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case client(config).put_object(config, key, "",
             content_type: "application/octet-stream",
             if_none_match: "*"
           ) do
        :ok -> {:cont, :ok}
        {:error, :precondition_failed} -> {:cont, :ok}
        {:error, :unavailable} -> {:halt, {:error, :unavailable}}
      end
    end)
  end

  defp do_backfill_session_order_indexes(config) do
    with {:ok, keys} <- list_session_keys(config) do
      {tenant_keys, legacy_keys} = session_order_backfill_targets(keys, config)
      tenant_count = map_size(tenant_keys)

      tenant_sessions =
        Enum.reduce(tenant_keys, 0, fn {_tenant_id, grouped_keys}, acc ->
          acc + length(grouped_keys)
        end)

      legacy_sessions = length(legacy_keys)

      result =
        Enum.reduce_while(
          Enum.sort_by(tenant_keys, fn {tenant_id, _keys} -> tenant_id end),
          :ok,
          fn
            {_tenant_id, tenant_session_keys}, :ok ->
              case backfill_session_order_indexes_for_keys(tenant_session_keys, config) do
                :ok -> {:cont, :ok}
                {:error, _reason} = error -> {:halt, error}
              end
          end
        )

      result =
        case {result, legacy_keys} do
          {:ok, []} ->
            :ok

          {:ok, keys} ->
            backfill_session_order_indexes_for_keys(keys, config)

          {{:error, _reason} = error, _keys} ->
            error
        end

      case result do
        :ok ->
          case mark_session_order_ready(config) do
            :ok ->
              {:ok,
               %{
                 legacy_sessions: legacy_sessions,
                 ready_already?: false,
                 sessions_total: tenant_sessions + legacy_sessions,
                 tenant_sessions: tenant_sessions,
                 tenants: tenant_count
               }}

            {:error, :unavailable} ->
              {:error, :archive_write_unavailable}
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp session_order_ready?(config) do
    case client(config).get_object(config, Layout.session_order_ready_key(config)) do
      {:ok, :not_found} -> {:ok, false}
      {:ok, {_body, _etag}} -> {:ok, true}
      {:error, :unavailable} -> {:error, :archive_read_unavailable}
    end
  end

  defp mark_session_order_ready(config) do
    case client(config).put_object(
           config,
           Layout.session_order_ready_key(config),
           ~s({"ready":true}),
           content_type: "application/json"
         ) do
      :ok -> :ok
      {:error, :precondition_failed} -> :ok
      {:error, :unavailable} -> {:error, :unavailable}
    end
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

  defp list_sessions_from_ordered_index(limit, cursor, metadata, tenant_id, config)
       when is_integer(limit) and limit > 0 and is_map(metadata) do
    with {:ok, listing} <- ordered_index_listing(config, tenant_id, cursor) do
      load_session_page_from_ordered_index(listing, limit, metadata, tenant_id, config)
    end
  end

  defp ordered_index_listing(config, tenant_id, nil)
       when is_binary(tenant_id) and tenant_id != "" do
    {:ok,
     %{
       prefix: Layout.session_order_tenant_prefix(config, tenant_id),
       start_after: nil,
       tenant_id: tenant_id,
       scope: :tenant
     }}
  end

  defp ordered_index_listing(config, tenant_id, cursor)
       when is_binary(tenant_id) and tenant_id != "" and is_binary(cursor) and cursor != "" do
    {:ok,
     %{
       prefix: Layout.session_order_tenant_prefix(config, tenant_id),
       start_after: Layout.session_order_tenant_key(config, tenant_id, cursor),
       tenant_id: tenant_id,
       scope: :tenant
     }}
  end

  defp ordered_index_listing(config, nil, nil) do
    {:ok,
     %{
       prefix: Layout.session_order_global_prefix(config),
       start_after: nil,
       tenant_id: nil,
       scope: :global
     }}
  end

  defp ordered_index_listing(config, nil, cursor) when is_binary(cursor) and cursor != "" do
    with {:ok, tenant_id} when is_binary(tenant_id) and tenant_id != "" <-
           resolve_session_tenant(config, cursor) do
      {:ok,
       %{
         prefix: Layout.session_order_global_prefix(config),
         start_after: Layout.session_order_global_key(config, tenant_id, cursor),
         tenant_id: nil,
         scope: :global
       }}
    else
      {:ok, nil} -> {:error, :archive_read_unavailable}
      {:error, _reason} = error -> error
    end
  end

  defp load_session_page_from_ordered_index(
         listing,
         limit,
         metadata_filters,
         tenant_id_filter,
         config
       )
       when is_map(listing) and is_integer(limit) and limit > 0 and is_map(metadata_filters) do
    target_size = limit + 1
    batch_size = ordered_list_batch_size(limit)

    do_load_session_page_from_ordered_index(
      listing,
      nil,
      batch_size,
      target_size,
      metadata_filters,
      tenant_id_filter,
      config,
      [],
      0
    )
  end

  defp do_load_session_page_from_ordered_index(
         listing,
         continuation_token,
         batch_size,
         target_size,
         metadata_filters,
         tenant_id_filter,
         config,
         sessions,
         count
       ) do
    case list_ordered_session_candidates(listing, continuation_token, batch_size, config) do
      {:ok, %{candidates: [], next_continuation_token: nil}} when count == 0 ->
        {:ok, build_session_page(Enum.reverse(sessions), target_size - 1)}

      {:ok, %{candidates: candidates, next_continuation_token: next_token}} ->
        with {:ok, next_sessions, next_count} <-
               load_matching_candidates(
                 candidates,
                 metadata_filters,
                 tenant_id_filter,
                 config,
                 sessions,
                 count,
                 target_size
               ) do
          cond do
            next_count >= target_size ->
              {:ok, build_session_page(Enum.reverse(next_sessions), target_size - 1)}

            is_binary(next_token) and next_token != "" ->
              do_load_session_page_from_ordered_index(
                listing,
                next_token,
                batch_size,
                target_size,
                metadata_filters,
                tenant_id_filter,
                config,
                next_sessions,
                next_count
              )

            true ->
              {:ok, build_session_page(Enum.reverse(next_sessions), target_size - 1)}
          end
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp list_ordered_session_candidates(listing, continuation_token, batch_size, config)
       when is_map(listing) and is_integer(batch_size) and batch_size > 0 do
    opts =
      [max_keys: batch_size]
      |> maybe_put_page_opt(:start_after, if(is_nil(continuation_token), do: listing.start_after))
      |> maybe_put_page_opt(:continuation_token, continuation_token)

    case client(config).list_keys_page(config, listing.prefix, opts) do
      {:ok, %{keys: keys, next_continuation_token: next_token}} ->
        {:ok,
         %{
           candidates:
             keys
             |> Enum.reduce([], fn key, acc ->
               case ordered_session_candidate(key, listing, config) do
                 {:ok, candidate} -> [candidate | acc]
                 :error -> acc
               end
             end)
             |> Enum.reverse(),
           next_continuation_token: next_token
         }}

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp ordered_session_candidate(key, %{scope: :tenant, tenant_id: tenant_id} = listing, config)
       when is_binary(key) and is_binary(tenant_id) do
    prefix = listing.prefix

    if String.starts_with?(key, prefix) do
      key
      |> String.trim_leading(prefix)
      |> ordered_tenant_candidate_from_suffix(tenant_id, config)
    else
      :error
    end
  end

  defp ordered_session_candidate(key, %{scope: :global} = listing, config) when is_binary(key) do
    prefix = listing.prefix

    if String.starts_with?(key, prefix) do
      key
      |> String.trim_leading(prefix)
      |> ordered_global_candidate_from_suffix(config)
    else
      :error
    end
  end

  defp ordered_tenant_candidate_from_suffix("", _tenant_id, _config), do: :error

  defp ordered_tenant_candidate_from_suffix(suffix, tenant_id, config)
       when is_binary(suffix) and is_binary(tenant_id) do
    with {:ok, session_id} <-
           suffix
           |> String.trim_trailing(".json")
           |> decode_ordered_segment() do
      {:ok,
       %{
         id: session_id,
         tenant_id: tenant_id,
         keys: session_keys_for_id(config, tenant_id, session_id)
       }}
    end
  end

  defp ordered_global_candidate_from_suffix(suffix, config) when is_binary(suffix) do
    case String.split(suffix, "/", trim: true) do
      [session_segment, tenant_segment] ->
        with {:ok, session_id} <- decode_ordered_segment(session_segment),
             {:ok, tenant_id} <-
               tenant_segment
               |> String.trim_trailing(".json")
               |> decode_base64url() do
          {:ok,
           %{
             id: session_id,
             tenant_id: tenant_id,
             keys: session_keys_for_id(config, tenant_id, session_id)
           }}
        end

      _other ->
        :error
    end
  end

  defp list_sessions_for_keys_with_backfill(keys, limit, cursor, metadata, tenant_id, config) do
    with {:ok, sessions} <- load_sessions(Enum.uniq(keys), config) do
      _ = materialize_session_order_indexes(sessions, config)

      {:ok,
       sessions
       |> session_page(limit, cursor, metadata, tenant_id)}
    end
  end

  defp list_sessions_for_keys(keys, limit, cursor, metadata, tenant_id, config) do
    keys
    |> session_candidates(config)
    |> sort_candidates_after_cursor(cursor)
    |> load_session_page(limit, metadata, tenant_id, config)
    |> case do
      {:ok, page} -> {:ok, page}
      {:error, _reason} = error -> error
    end
  end

  defp load_session_page(candidates, limit, metadata_filters, tenant_id_filter, config)
       when is_list(candidates) and is_integer(limit) and limit > 0 and is_map(metadata_filters) do
    target_size = limit + 1

    load_matching_candidates(
      candidates,
      metadata_filters,
      tenant_id_filter,
      config,
      [],
      0,
      target_size
    )
    |> case do
      {:ok, sessions, _count} ->
        {:ok, build_session_page(Enum.reverse(sessions), limit)}

      {:error, _reason} = error ->
        error
    end
  end

  defp load_sessions(keys, config) when is_list(keys) do
    keys
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, sessions} ->
      case load_session(key, config) do
        {:ok, nil} -> {:cont, {:ok, sessions}}
        {:ok, session} -> {:cont, {:ok, [session | sessions]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp load_matching_candidates(
         candidates,
         metadata_filters,
         tenant_id_filter,
         config,
         sessions,
         count,
         target_size
       )
       when is_list(candidates) and is_map(metadata_filters) and is_integer(count) and
              is_integer(target_size) and target_size > 0 do
    Enum.reduce_while(candidates, {:ok, sessions, count}, fn candidate,
                                                             {:ok, acc, current_count} ->
      if current_count >= target_size do
        {:halt, {:ok, acc, current_count}}
      else
        case load_matching_session(candidate, metadata_filters, tenant_id_filter, config) do
          {:ok, nil} -> {:cont, {:ok, acc, current_count}}
          {:ok, session} -> {:cont, {:ok, [session | acc], current_count + 1}}
          {:error, _reason} = error -> {:halt, error}
        end
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

  defp load_session_for_candidate(%{keys: keys}, config) when is_list(keys) do
    load_session_for_keys(keys, config)
  end

  defp load_session_for_candidate(%{key: key}, config) when is_binary(key) do
    load_session(key, config)
  end

  defp load_session_for_keys([], _config), do: {:ok, nil}

  defp load_session_for_keys([key | rest], config) when is_binary(key) do
    case load_session(key, config) do
      {:ok, nil} -> load_session_for_keys(rest, config)
      {:ok, session} -> {:ok, session}
      {:error, _reason} = error -> error
    end
  end

  defp load_matching_session(candidate, metadata_filters, tenant_id_filter, config)
       when is_map(candidate) and is_map(metadata_filters) do
    case obvious_tenant_mismatch?(candidate, tenant_id_filter) do
      true ->
        {:ok, nil}

      false ->
        with {:ok, session} <- load_session_for_candidate(candidate, config) do
          if matching_session?(session, metadata_filters, tenant_id_filter) do
            {:ok, session}
          else
            {:ok, nil}
          end
        end
    end
  end

  defp obvious_tenant_mismatch?(%{tenant_id: nil}, _tenant_id_filter), do: false
  defp obvious_tenant_mismatch?(_candidate, nil), do: false

  defp obvious_tenant_mismatch?(%{tenant_id: tenant_id}, tenant_id_filter)
       when is_binary(tenant_id_filter) do
    tenant_id != tenant_id_filter
  end

  defp matching_session?(nil, _metadata_filters, _tenant_id_filter), do: false

  defp matching_session?(session, metadata_filters, tenant_id_filter) do
    tenant_match?(Map.get(session, :tenant_id), tenant_id_filter) and
      metadata_match?(Map.get(session, :metadata, %{}), metadata_filters)
  end

  defp build_session_page(sessions, limit)
       when is_list(sessions) and is_integer(limit) and limit > 0 do
    {page_sessions, extra} = Enum.split(sessions, limit)

    %{
      sessions: page_sessions,
      next_cursor:
        if(extra == [] or page_sessions == [], do: nil, else: List.last(page_sessions).id)
    }
  end

  defp session_page(sessions, limit, cursor, metadata_filters, tenant_id_filter) do
    filtered =
      sessions
      |> dedupe_sessions()
      |> Enum.sort_by(& &1.id)
      |> Enum.filter(fn session ->
        (is_nil(cursor) or session.id > cursor) and
          tenant_match?(Map.get(session, :tenant_id), tenant_id_filter) and
          metadata_match?(Map.get(session, :metadata, %{}), metadata_filters)
      end)

    build_session_page(filtered, limit)
  end

  defp dedupe_sessions(sessions) when is_list(sessions) do
    sessions
    |> Enum.reduce(%{}, fn %{id: id} = session, acc ->
      Map.put_new(acc, id, session)
    end)
    |> Map.values()
  end

  defp sort_candidates_after_cursor(candidates, nil) when is_list(candidates) do
    Enum.sort_by(candidates, & &1.id)
  end

  defp sort_candidates_after_cursor(candidates, cursor)
       when is_list(candidates) and is_binary(cursor) and cursor != "" do
    candidates
    |> Enum.filter(&(&1.id > cursor))
    |> Enum.sort_by(& &1.id)
  end

  defp session_candidates(keys, config) when is_list(keys) do
    keys
    |> Enum.reduce(%{}, fn key, acc ->
      case session_candidate(key, config) do
        {:ok, %{id: id} = candidate} -> Map.put_new(acc, id, candidate)
        :error -> acc
      end
    end)
    |> Map.values()
  end

  defp session_candidate(key, config) when is_binary(key) do
    prefix = Layout.session_prefix(config)

    if String.starts_with?(key, prefix) do
      key
      |> String.trim_leading(prefix)
      |> session_candidate_from_suffix(key)
    else
      :error
    end
  end

  defp session_candidate_from_suffix("", _key), do: :error

  defp session_candidate_from_suffix(suffix, key) when is_binary(suffix) and is_binary(key) do
    case String.split(suffix, "/", trim: true) do
      [session_segment] ->
        with {:ok, session_id} <- decode_session_segment(session_segment) do
          {:ok, %{id: session_id, tenant_id: nil, key: key}}
        end

      [tenant_segment, session_segment] ->
        with {:ok, tenant_id} <- decode_base64url(tenant_segment),
             {:ok, session_id} <- decode_session_segment(session_segment) do
          {:ok, %{id: session_id, tenant_id: tenant_id, key: key}}
        end

      _other ->
        :error
    end
  end

  defp decode_session_segment(segment) when is_binary(segment) and segment != "" do
    segment
    |> String.trim_trailing(".json")
    |> decode_base64url()
  end

  defp decode_session_segment(_segment), do: :error

  defp decode_base64url(segment) when is_binary(segment) and segment != "" do
    case Base.url_decode64(segment, padding: false) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> :error
    end
  end

  defp decode_base64url(_segment), do: :error

  defp decode_ordered_segment(segment) when is_binary(segment) and segment != "" do
    case Base.decode16(segment, case: :lower) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _ -> :error
    end
  end

  defp decode_ordered_segment(_segment), do: :error

  defp tenant_match?(_tenant_id, nil), do: true

  defp tenant_match?(tenant_id, tenant_id_filter)
       when is_binary(tenant_id) and is_binary(tenant_id_filter) do
    tenant_id == tenant_id_filter
  end

  defp tenant_match?(_tenant_id, _tenant_id_filter), do: false

  defp metadata_match?(_metadata, filters) when map_size(filters) == 0, do: true

  defp metadata_match?(metadata, filters) when is_map(metadata) and is_map(filters) do
    Enum.all?(filters, fn {key, expected} -> Map.get(metadata, key) == expected end)
  end

  defp metadata_match?(_metadata, _filters), do: false

  defp materialize_session_order_indexes(sessions, config, mark_ready? \\ true)

  defp materialize_session_order_indexes(sessions, config, mark_ready?)
       when is_list(sessions) and is_boolean(mark_ready?) do
    sessions
    |> Enum.reduce_while(:ok, fn
      %{id: id, tenant_id: tenant_id}, :ok ->
        with :ok <- put_session_tenant_index(config, id, tenant_id),
             :ok <- put_session_order_indexes(config, id, tenant_id) do
          {:cont, :ok}
        else
          {:error, :unavailable} -> {:halt, {:error, :archive_write_unavailable}}
          {:error, _reason} = error -> {:halt, error}
        end

      _session, :ok ->
        {:cont, :ok}
    end)
    |> case do
      :ok ->
        if mark_ready?, do: mark_session_order_ready(config), else: :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp backfill_session_order_indexes_for_keys([], _config), do: :ok

  defp backfill_session_order_indexes_for_keys(keys, config) when is_list(keys) do
    case load_sessions(Enum.uniq(keys), config) do
      {:ok, sessions} -> materialize_session_order_indexes(sessions, config, false)
      {:error, _reason} = error -> error
    end
  end

  defp session_order_backfill_targets(keys, config) when is_list(keys) do
    Enum.reduce(keys, {%{}, []}, fn key, {tenant_keys, legacy_keys} = acc ->
      case session_candidate(key, config) do
        {:ok, %{tenant_id: tenant_id}} when is_binary(tenant_id) and tenant_id != "" ->
          {Map.update(tenant_keys, tenant_id, [key], &[key | &1]), legacy_keys}

        {:ok, %{tenant_id: nil}} ->
          {tenant_keys, [key | legacy_keys]}

        :error ->
          acc
      end
    end)
    |> then(fn {tenant_keys, legacy_keys} ->
      {
        Map.new(tenant_keys, fn {tenant_id, grouped_keys} ->
          {tenant_id, Enum.uniq(grouped_keys)}
        end),
        Enum.uniq(legacy_keys)
      }
    end)
  end

  defp ordered_list_batch_size(limit) when is_integer(limit) and limit > 0 do
    min(max(limit * @ordered_list_batch_factor, limit + 1), 1_000)
  end

  defp maybe_put_page_opt(opts, _key, nil), do: opts
  defp maybe_put_page_opt(opts, _key, ""), do: opts
  defp maybe_put_page_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp normalize_session_put_result(:ok), do: :ok
  defp normalize_session_put_result({:error, :precondition_failed}), do: :ok
  defp normalize_session_put_result({:error, :unavailable}), do: {:error, :unavailable}

  defp event_put_opts(nil), do: [content_type: "application/x-ndjson", if_none_match: "*"]
  defp event_put_opts(etag), do: [content_type: "application/x-ndjson", if_match: etag]

  defp event_from_row(row), do: Map.delete(row, :session_id)

  defp config!, do: :persistent_term.get(@config_key)
  defp client(%{client_mod: client_mod}), do: client_mod
end
