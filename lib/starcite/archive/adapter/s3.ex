defmodule Starcite.Archive.Adapter.S3 do
  @moduledoc """
  S3-backed archive adapter.

  Events are stored in chunked JSON blobs and merged idempotently on
  `(session_id, seq)`. Session catalog entries are stored as per-session JSON
  documents under a dedicated prefix.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  alias ExAws.S3

  @config_key {__MODULE__, :config}
  @default_prefix "starcite"
  @default_chunk_size 256
  @default_max_write_retries 4
  @default_compressed true
  @default_path_style true
  @events_prefix "events/v1"
  @sessions_prefix "sessions/v1"

  @impl true
  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) when is_list(opts) do
    config = build_config!(opts)
    :persistent_term.put(@config_key, config)
    {:ok, config}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :persistent_term.erase(@config_key)
    :ok
  end

  @impl true
  def write_events(rows) when is_list(rows) do
    with {:ok, config} <- fetch_config(),
         {:ok, inserted} <- do_write_events(rows, config) do
      {:ok, inserted}
    else
      {:error, :missing_config} -> {:error, :archive_write_unavailable}
      {:error, _reason} -> {:error, :archive_write_unavailable}
    end
  end

  @impl true
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, config} <- fetch_config(),
         {:ok, events} <- do_read_events(session_id, from_seq, to_seq, config) do
      {:ok, events}
    else
      {:error, :missing_config} -> {:error, :archive_read_unavailable}
      {:error, _reason} -> {:error, :archive_read_unavailable}
    end
  end

  @impl true
  def upsert_session(%{
        id: id,
        title: title,
        metadata: metadata,
        created_at: %DateTime{} = created_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and is_map(metadata) do
    with {:ok, config} <- fetch_config(),
         {:ok, _} <- put_session_once(id, title, metadata, created_at, config) do
      :ok
    else
      {:error, :missing_config} -> {:error, :archive_write_unavailable}
      {:error, _reason} -> {:error, :archive_write_unavailable}
    end
  end

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata})
      when is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    with {:ok, config} <- fetch_config(),
         {:ok, sessions} <- load_all_sessions(config) do
      {:ok, session_page(sessions, limit, cursor, metadata)}
    else
      {:error, :missing_config} -> {:error, :archive_read_unavailable}
      {:error, _reason} -> {:error, :archive_read_unavailable}
    end
  end

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata})
      when is_list(ids) and is_integer(limit) and limit > 0 and
             (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_map(metadata) do
    with {:ok, config} <- fetch_config(),
         {:ok, sessions} <- load_sessions_by_ids(ids, config) do
      {:ok, session_page(sessions, limit, cursor, metadata)}
    else
      {:error, :missing_config} -> {:error, :archive_read_unavailable}
      {:error, _reason} -> {:error, :archive_read_unavailable}
    end
  end

  defp do_write_events([], _config), do: {:ok, 0}

  defp do_write_events(rows, %{chunk_size: chunk_size} = config) when is_list(rows) do
    rows_by_chunk =
      Enum.group_by(rows, fn %{session_id: session_id, seq: seq} ->
        {session_id, chunk_start_for(seq, chunk_size)}
      end)

    Enum.reduce_while(rows_by_chunk, {:ok, 0}, fn
      {{session_id, chunk_start}, chunk_rows}, {:ok, total_inserted} ->
        case write_chunk_with_retries(session_id, chunk_start, chunk_rows, config, 1) do
          {:ok, inserted} ->
            {:cont, {:ok, total_inserted + inserted}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
    end)
  end

  defp write_chunk_with_retries(session_id, chunk_start, rows, config, attempt)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 and
              is_list(rows) and is_integer(attempt) and attempt > 0 do
    with {:ok, existing_events, etag} <- fetch_event_chunk(session_id, chunk_start, config),
         {merged_events, inserted} <- merge_chunk(existing_events, rows),
         {:ok, _} <-
           put_event_chunk(session_id, chunk_start, merged_events, etag, inserted, config) do
      {:ok, inserted}
    else
      {:retry, :precondition_failed} when attempt <= config.max_write_retries ->
        write_chunk_with_retries(session_id, chunk_start, rows, config, attempt + 1)

      {:retry, _reason} ->
        {:error, :archive_write_unavailable}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp merge_chunk(existing_events, rows) when is_list(existing_events) and is_list(rows) do
    existing_by_seq = Map.new(existing_events, fn event -> {event.seq, event} end)

    incoming_by_seq =
      rows
      |> Enum.sort_by(& &1.seq)
      |> Enum.reduce(%{}, fn %{seq: seq} = row, acc ->
        Map.put_new(acc, seq, event_from_row(row))
      end)

    inserted =
      Enum.count(incoming_by_seq, fn {seq, _event} ->
        not Map.has_key?(existing_by_seq, seq)
      end)

    merged =
      incoming_by_seq
      |> Map.merge(existing_by_seq)
      |> Map.values()
      |> Enum.sort_by(& &1.seq)

    {merged, inserted}
  end

  defp put_event_chunk(_session_id, _chunk_start, _merged_events, _etag, 0, _config), do: {:ok, 0}

  defp put_event_chunk(session_id, chunk_start, merged_events, etag, inserted, config)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 and
              is_list(merged_events) and is_integer(inserted) and inserted > 0 do
    body = encode_chunk_blob(merged_events, config.compressed)

    put_opts =
      base_put_opts(config.compressed) ++
        etag_match_opts(etag)

    operation =
      S3.put_object(
        config.bucket,
        event_chunk_key(config, session_id, chunk_start),
        body,
        put_opts
      )

    case request(operation, config) do
      {:ok, %{status_code: status}} when status in 200..299 ->
        {:ok, inserted}

      {:error, {:http_error, 409, _error}} ->
        {:retry, :precondition_failed}

      {:error, {:http_error, 412, _error}} ->
        {:retry, :precondition_failed}

      {:error, _reason} ->
        {:error, :archive_write_unavailable}
    end
  end

  defp do_read_events(session_id, from_seq, to_seq, %{chunk_size: chunk_size} = config)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    chunk_starts = chunk_starts_for_range(from_seq, to_seq, chunk_size)

    with {:ok, chunk_events} <- read_event_chunks(session_id, chunk_starts, config) do
      events =
        chunk_events
        |> Enum.filter(fn %{seq: seq} -> seq >= from_seq and seq <= to_seq end)
        |> Enum.sort_by(& &1.seq)
        |> Enum.uniq_by(& &1.seq)

      {:ok, events}
    end
  end

  defp read_event_chunks(_session_id, [], _config), do: {:ok, []}

  defp read_event_chunks(session_id, [chunk_start | rest], config)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    with {:ok, events, _etag} <- fetch_event_chunk(session_id, chunk_start, config),
         {:ok, tail} <- read_event_chunks(session_id, rest, config) do
      {:ok, events ++ tail}
    end
  end

  defp fetch_event_chunk(session_id, chunk_start, config)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    key = event_chunk_key(config, session_id, chunk_start)
    operation = S3.get_object(config.bucket, key)

    case request(operation, config) do
      {:ok, %{status_code: status, body: body, headers: headers}}
      when status in 200..299 and is_binary(body) ->
        with {:ok, events} <- decode_chunk_blob(body) do
          {:ok, events, header_value(headers, "etag")}
        end

      {:error, {:http_error, 404, _error}} ->
        {:ok, [], nil}

      {:error, _reason} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp put_session_once(id, title, metadata, created_at, config)
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and
              is_struct(created_at, DateTime) do
    body =
      Jason.encode!(%{
        id: id,
        title: title,
        metadata: metadata,
        created_at: DateTime.to_iso8601(created_at)
      })

    operation =
      S3.put_object(
        config.bucket,
        session_key(config, id),
        body,
        content_type: "application/json",
        if_none_match: "*"
      )

    case request(operation, config) do
      {:ok, %{status_code: status}} when status in 200..299 ->
        {:ok, :created}

      {:error, {:http_error, 409, _error}} ->
        {:ok, :already_exists}

      {:error, {:http_error, 412, _error}} ->
        {:ok, :already_exists}

      {:error, _reason} ->
        {:error, :archive_write_unavailable}
    end
  end

  defp load_all_sessions(config) do
    with {:ok, keys} <- list_object_keys(session_prefix(config), config) do
      keys
      |> Enum.reduce_while({:ok, []}, fn key, {:ok, sessions} ->
        case load_session_by_key(key, config) do
          {:ok, nil} -> {:cont, {:ok, sessions}}
          {:ok, session} -> {:cont, {:ok, [session | sessions]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
      |> case do
        {:ok, sessions} -> {:ok, sessions}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp load_sessions_by_ids(ids, config) when is_list(ids) do
    ids
    |> Enum.uniq()
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce_while({:ok, []}, fn id, {:ok, acc} ->
      case load_session(id, config) do
        {:ok, nil} -> {:cont, {:ok, acc}}
        {:ok, session} -> {:cont, {:ok, [session | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp load_session(id, config) when is_binary(id) and id != "" do
    load_session_by_key(session_key(config, id), config)
  end

  defp load_session_by_key(key, config) when is_binary(key) and key != "" do
    operation = S3.get_object(config.bucket, key)

    case request(operation, config) do
      {:ok, %{status_code: status, body: body}} when status in 200..299 and is_binary(body) ->
        case Jason.decode(body) do
          {:ok, session} -> {:ok, normalize_session(session)}
          {:error, _reason} -> {:error, :archive_read_unavailable}
        end

      {:error, {:http_error, 404, _error}} ->
        {:ok, nil}

      {:error, _reason} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp list_object_keys(prefix, config) when is_binary(prefix) and prefix != "" do
    list_object_keys(prefix, nil, config, [])
  end

  defp list_object_keys(prefix, continuation_token, config, acc)
       when is_binary(prefix) and prefix != "" and is_list(acc) do
    operation =
      S3.list_objects_v2(
        config.bucket,
        list_objects_opts(prefix, continuation_token)
      )
      |> Map.put(:parser, &Function.identity/1)

    case request(operation, config) do
      {:ok, %{status_code: status, body: xml}} when status in 200..299 and is_binary(xml) ->
        with {:ok, page} <- parse_list_objects_xml(xml) do
          next = page.keys ++ acc

          if is_binary(page.next_continuation_token) and page.next_continuation_token != "" do
            list_object_keys(prefix, page.next_continuation_token, config, next)
          else
            {:ok, Enum.reverse(next)}
          end
        end

      {:error, _reason} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp list_objects_opts(prefix, nil), do: [prefix: prefix, max_keys: 1_000]

  defp list_objects_opts(prefix, continuation_token)
       when is_binary(prefix) and prefix != "" and is_binary(continuation_token) and
              continuation_token != "" do
    [prefix: prefix, continuation_token: continuation_token, max_keys: 1_000]
  end

  defp parse_list_objects_xml(xml) when is_binary(xml) and xml != "" do
    keys =
      Regex.scan(~r/<Key>([^<]*)<\/Key>/, xml, capture: :all_but_first)
      |> Enum.map(&xml_unescape(hd(&1)))

    is_truncated =
      Regex.run(~r/<IsTruncated>([^<]*)<\/IsTruncated>/, xml, capture: :all_but_first)
      |> case do
        [value] when value in ["true", "True", "1"] -> true
        _ -> false
      end

    next_continuation_token =
      Regex.run(~r/<NextContinuationToken>([^<]*)<\/NextContinuationToken>/, xml,
        capture: :all_but_first
      )
      |> case do
        [value] when is_truncated -> xml_unescape(value)
        _ -> nil
      end

    {:ok, %{keys: keys, next_continuation_token: next_continuation_token}}
  end

  defp parse_list_objects_xml(_xml), do: {:error, :archive_read_unavailable}

  defp session_page(sessions, limit, cursor, metadata_filters)
       when is_list(sessions) and is_integer(limit) and limit > 0 and
              (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and
              is_map(metadata_filters) do
    filtered =
      sessions
      |> Enum.sort_by(& &1.id)
      |> Enum.filter(fn session ->
        (is_nil(cursor) or session.id > cursor) and
          metadata_match?(session.metadata, metadata_filters)
      end)

    page_sessions = Enum.take(filtered, limit)

    %{
      sessions: page_sessions,
      next_cursor:
        if(length(filtered) > limit and page_sessions != [],
          do: List.last(page_sessions).id,
          else: nil
        )
    }
  end

  defp metadata_match?(_metadata, filters) when map_size(filters) == 0, do: true

  defp metadata_match?(metadata, filters) when is_map(metadata) and is_map(filters) do
    Enum.all?(filters, fn {key, expected} -> Map.get(metadata, key) == expected end)
  end

  defp metadata_match?(_metadata, _filters), do: false

  defp normalize_session(%{
         "id" => id,
         "title" => title,
         "metadata" => metadata,
         "created_at" => created_at
       })
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and
              is_binary(created_at) do
    %{
      id: id,
      title: title,
      metadata: metadata,
      created_at: created_at
    }
  end

  defp normalize_session(_session), do: raise(ArgumentError, "invalid persisted session shape")

  defp encode_chunk_blob(events, compressed?)
       when is_list(events) and is_boolean(compressed?) do
    body =
      Jason.encode!(%{
        version: 1,
        events: Enum.map(events, &event_to_storage/1)
      })

    if compressed?, do: :zlib.gzip(body), else: body
  end

  defp decode_chunk_blob(body) when is_binary(body) do
    json =
      if gzip_blob?(body) do
        :zlib.gunzip(body)
      else
        body
      end

    case Jason.decode(json) do
      {:ok, %{"events" => events}} when is_list(events) ->
        {:ok, Enum.map(events, &event_from_storage/1)}

      _ ->
        {:error, :archive_read_unavailable}
    end
  end

  defp event_to_storage(%{
         seq: seq,
         type: type,
         payload: payload,
         actor: actor,
         producer_id: producer_id,
         producer_seq: producer_seq,
         source: source,
         metadata: metadata,
         refs: refs,
         idempotency_key: idempotency_key,
         inserted_at: inserted_at
       })
       when is_integer(seq) and seq > 0 and is_binary(type) and type != "" and is_map(payload) and
              is_binary(actor) and actor != "" and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 and
              (is_binary(source) or is_nil(source)) and
              is_map(metadata) and is_map(refs) and
              (is_binary(idempotency_key) or is_nil(idempotency_key)) do
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
      "inserted_at" => format_inserted_at(inserted_at)
    }
  end

  defp event_from_storage(%{
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
       })
       when is_integer(seq) and seq > 0 and is_binary(type) and type != "" and is_map(payload) and
              is_binary(actor) and actor != "" and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 and
              (is_binary(source) or is_nil(source)) and is_map(metadata) and is_map(refs) and
              (is_binary(idempotency_key) or is_nil(idempotency_key)) and is_binary(inserted_at) do
    %{
      seq: seq,
      type: type,
      payload: payload,
      actor: actor,
      producer_id: producer_id,
      producer_seq: producer_seq,
      source: source,
      metadata: metadata,
      refs: refs,
      idempotency_key: idempotency_key,
      inserted_at: parse_inserted_at!(inserted_at)
    }
  end

  defp event_from_storage(_event), do: raise(ArgumentError, "invalid persisted event shape")

  defp event_from_row(row) when is_map(row) do
    row
    |> Map.delete(:session_id)
    |> Map.take([
      :seq,
      :type,
      :payload,
      :actor,
      :producer_id,
      :producer_seq,
      :source,
      :metadata,
      :refs,
      :idempotency_key,
      :inserted_at
    ])
  end

  defp base_put_opts(compressed?) when is_boolean(compressed?) do
    base = [content_type: "application/json"]
    if compressed?, do: [content_encoding: "gzip"] ++ base, else: base
  end

  defp etag_match_opts(nil), do: [if_none_match: "*"]
  defp etag_match_opts(etag) when is_binary(etag) and etag != "", do: [if_match: etag]

  defp header_value(headers, target_name) when is_list(headers) and is_binary(target_name) do
    Enum.find_value(headers, fn
      {name, value} when is_binary(name) and is_binary(value) ->
        if String.downcase(name) == target_name, do: value, else: nil

      _ ->
        nil
    end)
  end

  defp request(operation, %{request_opts: request_opts}) when is_list(request_opts) do
    ExAws.request(operation, request_opts)
  end

  defp fetch_config do
    case :persistent_term.get(@config_key, :undefined) do
      :undefined -> {:error, :missing_config}
      config when is_map(config) -> {:ok, config}
    end
  end

  defp build_config!(opts) when is_list(opts) do
    merged_opts =
      Application.get_env(:starcite, :archive_adapter_opts, [])
      |> Keyword.merge(opts)
      |> Keyword.new()

    bucket =
      keyword_or_env(merged_opts, :bucket, "STARCITE_S3_BUCKET")
      |> require_non_empty!("STARCITE_S3_BUCKET")

    prefix =
      keyword_or_env(merged_opts, :prefix, "STARCITE_S3_PREFIX")
      |> normalize_prefix()

    region =
      keyword_or_env(merged_opts, :region, "STARCITE_S3_REGION") ||
        System.get_env("AWS_REGION")

    access_key_id =
      keyword_or_env(merged_opts, :access_key_id, "STARCITE_S3_ACCESS_KEY_ID") ||
        System.get_env("AWS_ACCESS_KEY_ID")

    secret_access_key =
      keyword_or_env(merged_opts, :secret_access_key, "STARCITE_S3_SECRET_ACCESS_KEY") ||
        System.get_env("AWS_SECRET_ACCESS_KEY")

    security_token =
      keyword_or_env(merged_opts, :security_token, "STARCITE_S3_SESSION_TOKEN") ||
        System.get_env("AWS_SESSION_TOKEN")

    endpoint =
      keyword_or_env(merged_opts, :endpoint, "STARCITE_S3_ENDPOINT") ||
        System.get_env("AWS_ENDPOINT_URL_S3") ||
        System.get_env("AWS_ENDPOINT_URL")

    path_style =
      keyword_or_env(merged_opts, :path_style, "STARCITE_S3_PATH_STYLE")
      |> parse_bool_or_default!(@default_path_style, "STARCITE_S3_PATH_STYLE")

    cache_chunk_size =
      case Application.get_env(:starcite, :event_store_cache_chunk_size, @default_chunk_size) do
        value when is_integer(value) and value > 0 ->
          value

        other ->
          raise ArgumentError,
                "invalid event_store_cache_chunk_size: #{inspect(other)} (expected positive integer)"
      end

    chunk_size =
      keyword_or_env(merged_opts, :chunk_size, "STARCITE_S3_CHUNK_SIZE")
      |> parse_positive_integer_or_default!(cache_chunk_size, "STARCITE_S3_CHUNK_SIZE")
      |> ensure_chunk_aligned!(cache_chunk_size)

    compressed =
      keyword_or_env(merged_opts, :compressed, "STARCITE_S3_COMPRESSED")
      |> parse_bool_or_default!(@default_compressed, "STARCITE_S3_COMPRESSED")

    max_write_retries =
      keyword_or_env(merged_opts, :max_write_retries, "STARCITE_S3_MAX_WRITE_RETRIES")
      |> parse_positive_integer_or_default!(
        @default_max_write_retries,
        "STARCITE_S3_MAX_WRITE_RETRIES"
      )

    request_opts =
      [
        region: region,
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        security_token: security_token,
        virtual_host: not path_style
      ]
      |> maybe_put_endpoint(endpoint)
      |> Enum.reject(fn {_key, value} -> is_nil(value) end)

    %{
      bucket: bucket,
      prefix: prefix,
      chunk_size: chunk_size,
      compressed: compressed,
      max_write_retries: max_write_retries,
      request_opts: request_opts
    }
  end

  defp maybe_put_endpoint(opts, nil), do: opts

  defp maybe_put_endpoint(opts, endpoint) when is_binary(endpoint) and endpoint != "" do
    case URI.parse(endpoint) do
      %URI{scheme: scheme, host: host, port: port, path: path}
      when scheme in ["http", "https"] and is_binary(host) and host != "" and
             (is_nil(path) or path == "" or path == "/") ->
        opts ++
          [
            scheme: "#{scheme}://",
            host: host,
            port: port
          ]

      _ ->
        raise ArgumentError,
              "invalid STARCITE_S3_ENDPOINT: #{inspect(endpoint)} (expected http(s)://host[:port])"
    end
  end

  defp keyword_or_env(opts, key, env_name)
       when is_list(opts) and is_atom(key) and is_binary(env_name) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> value
      :error -> System.get_env(env_name)
    end
  end

  defp require_non_empty!(value, _env_name) when is_binary(value) and value != "", do: value

  defp require_non_empty!(value, env_name) do
    raise ArgumentError,
          "missing required #{env_name} for S3 archive adapter (got: #{inspect(value)})"
  end

  defp parse_positive_integer_or_default!(nil, default, _env_name)
       when is_integer(default) and default > 0,
       do: default

  defp parse_positive_integer_or_default!(value, _default, _env_name)
       when is_integer(value) and value > 0,
       do: value

  defp parse_positive_integer_or_default!(value, _default, env_name) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} when parsed > 0 ->
        parsed

      _ ->
        raise ArgumentError,
              "invalid integer for #{env_name}: #{inspect(value)} (expected positive integer)"
    end
  end

  defp parse_positive_integer_or_default!(value, _default, env_name) do
    raise ArgumentError,
          "invalid integer for #{env_name}: #{inspect(value)} (expected positive integer)"
  end

  defp parse_bool_or_default!(nil, default, _env_name) when is_boolean(default), do: default
  defp parse_bool_or_default!(value, _default, _env_name) when is_boolean(value), do: value

  defp parse_bool_or_default!(value, _default, env_name) when is_binary(value) do
    normalized = value |> String.trim() |> String.downcase()

    case normalized do
      "1" -> true
      "true" -> true
      "yes" -> true
      "on" -> true
      "0" -> false
      "false" -> false
      "no" -> false
      "off" -> false
      _ -> raise ArgumentError, "invalid boolean for #{env_name}: #{inspect(value)}"
    end
  end

  defp parse_bool_or_default!(value, _default, env_name) do
    raise ArgumentError,
          "invalid boolean for #{env_name}: #{inspect(value)} (expected true/false)"
  end

  defp ensure_chunk_aligned!(chunk_size, cache_chunk_size)
       when is_integer(chunk_size) and chunk_size > 0 and is_integer(cache_chunk_size) and
              cache_chunk_size > 0 do
    if rem(chunk_size, cache_chunk_size) == 0 do
      chunk_size
    else
      raise ArgumentError,
            "invalid STARCITE_S3_CHUNK_SIZE=#{chunk_size}: must be a multiple of event_store_cache_chunk_size=#{cache_chunk_size}"
    end
  end

  defp normalize_prefix(nil), do: @default_prefix

  defp normalize_prefix(prefix) when is_binary(prefix) do
    normalized = prefix |> String.trim() |> String.trim("/")
    if normalized == "", do: @default_prefix, else: normalized
  end

  defp normalize_prefix(prefix) do
    raise ArgumentError, "invalid STARCITE_S3_PREFIX: #{inspect(prefix)}"
  end

  defp event_chunk_key(config, session_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    extension = if config.compressed, do: "json.gz", else: "json"

    key_join([
      config.prefix,
      @events_prefix,
      encode_session_id(session_id),
      "#{chunk_start}.#{extension}"
    ])
  end

  defp session_prefix(config) do
    key_join([config.prefix, @sessions_prefix]) <> "/"
  end

  defp session_key(config, session_id) when is_binary(session_id) and session_id != "" do
    key_join([config.prefix, @sessions_prefix, "#{encode_session_id(session_id)}.json"])
  end

  defp key_join(parts) when is_list(parts) do
    parts
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&to_string/1)
    |> Enum.map(&String.trim(&1, "/"))
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("/")
  end

  defp encode_session_id(session_id) when is_binary(session_id) and session_id != "" do
    Base.url_encode64(session_id, padding: false)
  end

  defp chunk_starts_for_range(from_seq, to_seq, chunk_size)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_integer(chunk_size) and chunk_size > 0 do
    first = chunk_start_for(from_seq, chunk_size)
    last = chunk_start_for(to_seq, chunk_size)

    first
    |> Stream.iterate(&(&1 + chunk_size))
    |> Enum.take_while(&(&1 <= last))
  end

  defp chunk_start_for(seq, chunk_size)
       when is_integer(seq) and seq > 0 and is_integer(chunk_size) and chunk_size > 0 do
    div(seq - 1, chunk_size) * chunk_size + 1
  end

  defp format_inserted_at(%NaiveDateTime{} = value) do
    value
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp format_inserted_at(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp parse_inserted_at!(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        datetime

      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive_datetime} -> naive_datetime
          {:error, reason} -> raise ArgumentError, "invalid inserted_at value: #{inspect(reason)}"
        end
    end
  end

  defp gzip_blob?(<<31, 139, _rest::binary>>), do: true
  defp gzip_blob?(_body), do: false

  defp xml_unescape(value) when is_binary(value) do
    value
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&quot;", "\"")
    |> String.replace("&apos;", "'")
    |> String.replace("&amp;", "&")
  end
end
