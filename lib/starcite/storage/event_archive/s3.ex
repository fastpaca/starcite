defmodule Starcite.Storage.EventArchive.S3 do
  @moduledoc """
  S3-backed event archive.

  Storage layout:
  - Events: `<prefix>/events/v1/<base64url(tenant_id)>/<base64url(session_id)>/<chunk_start>.ndjson`

  Event objects are newline-delimited JSON (NDJSON), one event per line, with
  one object per cache-line chunk.

  Writes are idempotent by `(session_id, seq)` via read/merge/conditional-write
  using ETag preconditions.
  """

  use GenServer

  alias __MODULE__.{Config, Layout, Schema, SchemaControl}

  @config_key {__MODULE__, :config}

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    runtime_opts = Application.get_env(:starcite, :event_archive_opts, [])
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

  def write_events(rows), do: write_events(rows, config!())

  def read_events(session_id, tenant_id, from_seq, to_seq),
    do: read_events(session_id, tenant_id, from_seq, to_seq, config!())

  defp write_events([], _config), do: {:ok, 0}

  defp write_events(rows, config) do
    with :ok <- validate_session_tenants(rows) do
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
  end

  defp write_chunk(tenant_id, session_id, chunk_start, rows, config, attempt) do
    with {:ok, expected_tenant_id} <- chunk_tenant_id(rows),
         true <- expected_tenant_id == tenant_id,
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

  defp validate_session_tenants(rows) when is_list(rows) do
    rows
    |> Enum.reduce_while(%{}, fn
      %{session_id: session_id, tenant_id: tenant_id}, acc
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" ->
        case acc do
          %{^session_id => ^tenant_id} ->
            {:cont, acc}

          %{^session_id => _other_tenant_id} ->
            {:halt, {:error, :archive_write_unavailable}}

          _ ->
            {:cont, Map.put(acc, session_id, tenant_id)}
        end

      _invalid_row, _acc ->
        {:halt, {:error, :archive_write_unavailable}}
    end)
    |> case do
      %{} -> :ok
      {:error, _reason} = error -> error
    end
  end

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

  defp read_events(session_id, tenant_id, from_seq, to_seq, config)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" do
    chunk_starts = Layout.chunk_starts_for_range(from_seq, to_seq, config.chunk_size)

    with {:ok, chunks} <- read_chunks(session_id, tenant_id, chunk_starts, config) do
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

  defp event_put_opts(nil), do: [content_type: "application/x-ndjson", if_none_match: "*"]
  defp event_put_opts(etag), do: [content_type: "application/x-ndjson", if_match: etag]

  defp event_from_row(row), do: Map.delete(row, :session_id)

  defp config!, do: :persistent_term.get(@config_key)
  defp client(%{client_mod: client_mod}), do: client_mod
end
