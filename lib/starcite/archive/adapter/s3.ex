defmodule Starcite.Archive.Adapter.S3 do
  @moduledoc """
  S3-backed archive adapter.

  Storage layout:
  - Events: `<prefix>/events/v1/<base64url(session_id)>/<chunk_start>.ndjson`
  - Sessions: `<prefix>/sessions/v1/<base64url(session_id)>.json`

  Event objects are newline-delimited JSON (NDJSON), one event per line, with
  one object per cache-line chunk. Session objects are plain JSON maps.

  Writes are idempotent by `(session_id, seq)` via read/merge/conditional-write
  using ETag preconditions.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  alias Starcite.Auth.Principal
  alias __MODULE__.{Config, Layout}

  @config_key {__MODULE__, :config}

  @impl true
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    runtime_opts = Application.get_env(:starcite, :archive_adapter_opts, [])
    config = Config.build!(runtime_opts, opts)
    :persistent_term.put(@config_key, config)
    {:ok, config}
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
        creator_principal: creator_principal,
        metadata: metadata,
        created_at: created_at
      })
      when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
             (is_struct(creator_principal, Principal) or is_nil(creator_principal)) and
             is_map(metadata) do
    with {:ok, creator_principal_payload} <- principal_to_map(creator_principal) do
      config = config!()

      session = %{
        id: id,
        title: title,
        creator_principal: creator_principal_payload,
        metadata: metadata,
        created_at: created_at
      }

      case put_session(config, id, session) do
        :ok -> :ok
        {:error, :precondition_failed} -> :ok
        {:error, :unavailable} -> {:error, :archive_write_unavailable}
      end
    else
      _ -> {:error, :archive_write_unavailable}
    end
  end

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata}) do
    config = config!()

    with {:ok, keys} <- list_session_keys(config) do
      list_sessions_for_keys(keys, limit, cursor, metadata, config)
    end
  end

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata}) do
    config = config!()
    keys = ids |> Enum.uniq() |> Enum.map(&Layout.session_key(config, &1))

    list_sessions_for_keys(keys, limit, cursor, metadata, config)
  end

  defp write_events([], _config), do: {:ok, 0}

  defp write_events(rows, config) do
    rows
    |> Layout.group_event_rows(config.chunk_size)
    |> Enum.reduce_while({:ok, 0}, fn {{session_id, chunk_start}, chunk_rows}, {:ok, total} ->
      case write_chunk(session_id, chunk_start, chunk_rows, config, 1) do
        {:ok, inserted} -> {:cont, {:ok, total + inserted}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp write_chunk(session_id, chunk_start, rows, config, attempt) do
    with {:ok, existing_events, etag} <- fetch_chunk(session_id, chunk_start, config),
         {merged, inserted} <- merge_chunk(existing_events, rows),
         {:ok, inserted} <- put_chunk(session_id, chunk_start, merged, etag, inserted, config) do
      {:ok, inserted}
    else
      {:error, :precondition_failed} when attempt <= config.max_write_retries ->
        write_chunk(session_id, chunk_start, rows, config, attempt + 1)

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

  defp put_chunk(_session_id, _chunk_start, _events, _etag, 0, _config), do: {:ok, 0}

  defp put_chunk(session_id, chunk_start, events, etag, inserted, config) do
    key = Layout.event_chunk_key(config, session_id, chunk_start)
    body = encode_chunk(events)

    case client(config).put_object(config, key, body, event_put_opts(etag)) do
      :ok -> {:ok, inserted}
      {:error, :precondition_failed} -> {:error, :precondition_failed}
      {:error, :unavailable} -> {:error, :archive_write_unavailable}
    end
  end

  defp read_events(session_id, from_seq, to_seq, config) do
    chunk_starts = Layout.chunk_starts_for_range(from_seq, to_seq, config.chunk_size)

    with {:ok, chunks} <- read_chunks(session_id, chunk_starts, config) do
      events =
        chunks
        |> List.flatten()
        |> Enum.filter(fn event -> event.seq >= from_seq and event.seq <= to_seq end)

      {:ok, events}
    end
  end

  defp read_chunks(session_id, chunk_starts, config) do
    chunk_starts
    |> Enum.reduce_while({:ok, []}, fn chunk_start, {:ok, acc} ->
      case fetch_chunk(session_id, chunk_start, config) do
        {:ok, events, _etag} -> {:cont, {:ok, [events | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_chunk(session_id, chunk_start, config) do
    case client(config).get_object(
           config,
           Layout.event_chunk_key(config, session_id, chunk_start)
         ) do
      {:ok, :not_found} ->
        {:ok, [], nil}

      {:ok, {body, etag}} ->
        with {:ok, events} <- decode_chunk(body) do
          {:ok, events, etag}
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp put_session(config, session_id, session) do
    key = Layout.session_key(config, session_id)
    body = Jason.encode!(session)

    client(config).put_object(config, key, body,
      content_type: "application/json",
      if_none_match: "*"
    )
  end

  defp list_session_keys(config) do
    case client(config).list_keys(config, Layout.session_prefix(config)) do
      {:ok, keys} -> {:ok, keys}
      {:error, :unavailable} -> {:error, :archive_read_unavailable}
    end
  end

  defp list_sessions_for_keys(keys, limit, cursor, metadata, config) do
    with {:ok, sessions} <- load_sessions(keys, config) do
      {:ok, session_page(sessions, limit, cursor, metadata)}
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
        with {:ok, decoded} <- Jason.decode(body),
             {:ok, session} <- decode_session(decoded) do
          {:ok, session}
        else
          _ -> {:error, :archive_read_unavailable}
        end

      {:error, :unavailable} ->
        {:error, :archive_read_unavailable}
    end
  end

  defp session_page(sessions, limit, cursor, metadata_filters) do
    filtered =
      sessions
      |> Enum.sort_by(& &1.id)
      |> Enum.filter(fn session ->
        (is_nil(cursor) or session.id > cursor) and
          Enum.all?(metadata_filters, fn {key, expected} -> session.metadata[key] == expected end)
      end)

    {page_sessions, rest} = Enum.split(filtered, limit)

    %{
      sessions: page_sessions,
      next_cursor:
        if(rest == [] or page_sessions == [], do: nil, else: List.last(page_sessions).id)
    }
  end

  defp event_put_opts(nil), do: [content_type: "application/x-ndjson", if_none_match: "*"]
  defp event_put_opts(etag), do: [content_type: "application/x-ndjson", if_match: etag]

  defp event_from_row(row), do: Map.delete(row, :session_id)

  defp encode_chunk(events), do: Enum.map_join(events, "\n", &Jason.encode!/1)

  defp decode_chunk(body) when is_binary(body) do
    events =
      body
      |> String.split("\n", trim: true)
      |> Enum.map(&decode_chunk_event!/1)

    {:ok, events}
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp decode_chunk_event!(line) do
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
    } = Jason.decode!(line)

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
      inserted_at: inserted_at
    }
  end

  defp decode_session(%{
         "id" => id,
         "title" => title,
         "creator_principal" => creator_principal_payload,
         "metadata" => metadata,
         "created_at" => created_at
       })
       when is_binary(id) and id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and
              is_binary(created_at) do
    with {:ok, creator_principal} <- principal_from_map(creator_principal_payload),
         {:ok, created_at_datetime, _offset} <- DateTime.from_iso8601(created_at) do
      {:ok,
       %{
         id: id,
         title: title,
         creator_principal: creator_principal,
         metadata: metadata,
         created_at: created_at_datetime
       }}
    else
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp decode_session(_invalid), do: {:error, :archive_read_unavailable}

  defp principal_from_map(nil), do: {:ok, nil}

  defp principal_from_map(%{"tenant_id" => tenant_id, "id" => id, "type" => type})
       when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" do
    with {:ok, principal_type} <- principal_type(type),
         {:ok, principal} <- Principal.new(tenant_id, id, principal_type) do
      {:ok, principal}
    end
  end

  defp principal_from_map(_invalid), do: {:error, :archive_read_unavailable}

  defp principal_to_map(nil), do: {:ok, nil}

  defp principal_to_map(%Principal{} = principal) do
    {:ok,
     %{
       "tenant_id" => principal.tenant_id,
       "id" => principal.id,
       "type" => Atom.to_string(principal.type)
     }}
  end

  defp principal_type("user"), do: {:ok, :user}
  defp principal_type("agent"), do: {:ok, :agent}
  defp principal_type(_invalid), do: {:error, :archive_read_unavailable}

  defp config!, do: :persistent_term.get(@config_key)
  defp client(%{client_mod: client_mod}), do: client_mod
end
