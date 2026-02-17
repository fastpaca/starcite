defmodule Starcite.Archive.Adapter.S3 do
  @moduledoc """
  S3-backed archive adapter.

  Events are stored as chunked NDJSON blobs and merged idempotently on
  `(session_id, seq)`. Sessions are stored as per-session JSON documents.
  """

  @behaviour Starcite.Archive.Adapter

  use GenServer

  alias __MODULE__.{Client, Config, Layout}

  @config_key {__MODULE__, :config}
  @stored_event_fields Starcite.Archive.Event.__schema__(:fields) -- [:session_id]

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
  def write_events(rows), do: with_config(:write, fn config -> write_events(rows, config) end)

  @impl true
  def read_events(session_id, from_seq, to_seq) do
    with_config(:read, fn config -> read_events(session_id, from_seq, to_seq, config) end)
  end

  @impl true
  def upsert_session(%{id: id, title: title, metadata: metadata, created_at: created_at}) do
    with_config(:write, fn config ->
      session = %{id: id, title: title, metadata: metadata, created_at: created_at}

      case put_session(config, id, session) do
        :ok -> :ok
        {:error, :precondition_failed} -> :ok
        {:error, :unavailable} -> {:error, :archive_write_unavailable}
      end
    end)
  end

  @impl true
  def list_sessions(%{limit: limit, cursor: cursor, metadata: metadata}) do
    with_config(:read, fn config ->
      with {:ok, keys} <- list_session_keys(config),
           {:ok, sessions} <- load_sessions(keys, config) do
        {:ok, session_page(sessions, limit, cursor, metadata)}
      end
    end)
  end

  @impl true
  def list_sessions_by_ids(ids, %{limit: limit, cursor: cursor, metadata: metadata}) do
    with_config(:read, fn config ->
      keys = ids |> Enum.uniq() |> Enum.map(&Layout.session_key(config, &1))

      with {:ok, sessions} <- load_sessions(keys, config) do
        {:ok, session_page(sessions, limit, cursor, metadata)}
      end
    end)
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

      {:error, :precondition_failed} ->
        {:error, :archive_write_unavailable}

      {:error, :archive_read_unavailable} ->
        {:error, :archive_write_unavailable}

      {:error, :archive_write_unavailable} ->
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

    case Client.put_object(config, key, body, event_put_opts(etag)) do
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
        |> Enum.sort_by(& &1.seq)
        |> Enum.uniq_by(& &1.seq)

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
    case Client.get_object(config, Layout.event_chunk_key(config, session_id, chunk_start)) do
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

    Client.put_object(config, key, body,
      content_type: "application/json",
      if_none_match: "*"
    )
  end

  defp list_session_keys(config) do
    case Client.list_keys(config, Layout.session_prefix(config)) do
      {:ok, keys} -> {:ok, keys}
      {:error, :unavailable} -> {:error, :archive_read_unavailable}
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
    case Client.get_object(config, key) do
      {:ok, :not_found} ->
        {:ok, nil}

      {:ok, {body, _etag}} ->
        case Jason.decode(body) do
          {:ok,
           %{"id" => id, "title" => title, "metadata" => metadata, "created_at" => created_at}} ->
            {:ok, %{id: id, title: title, metadata: metadata, created_at: created_at}}

          _ ->
            {:error, :archive_read_unavailable}
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

  defp event_put_opts(nil), do: [content_type: "application/x-ndjson", if_none_match: "*"]
  defp event_put_opts(etag), do: [content_type: "application/x-ndjson", if_match: etag]

  defp event_from_row(row), do: Map.take(row, @stored_event_fields)

  defp encode_chunk(events) do
    events
    |> Enum.map_join("\n", fn event ->
      event
      |> Map.take(@stored_event_fields)
      |> Jason.encode!()
    end)
  end

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
    decoded = Jason.decode!(line)

    Enum.reduce(@stored_event_fields, %{}, fn field, acc ->
      Map.put(acc, field, decoded[Atom.to_string(field)])
    end)
  end

  defp with_config(kind, fun) when kind in [:read, :write] and is_function(fun, 1) do
    with {:ok, config} <- fetch_config() do
      case fun.(config) do
        {:error, _reason} -> unavailable_error(kind)
        result -> result
      end
    else
      {:error, :missing_config} -> unavailable_error(kind)
    end
  end

  defp unavailable_error(:read), do: {:error, :archive_read_unavailable}
  defp unavailable_error(:write), do: {:error, :archive_write_unavailable}

  defp fetch_config do
    case :persistent_term.get(@config_key, :undefined) do
      :undefined -> {:error, :missing_config}
      config when is_map(config) -> {:ok, config}
    end
  end
end
