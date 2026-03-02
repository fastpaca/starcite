defmodule Starcite.DataPlane.SessionStore do
  @moduledoc """
  Cachex-backed canonical session read store.

  `SessionStore` serves session reads as hot/cold tiers:

  - hot: Cachex in-memory session cache
  - cold: archive adapter lookup via `Starcite.Archive.Store`

  Cache misses hydrate from archive and populate cache for follow-up reads.
  """

  alias Starcite.Archive.Store
  alias Starcite.Session
  import Cachex.Spec, only: [expiration: 1]

  @cache :starcite_session_store
  @default_ttl_ms 21_600_000
  @default_purge_interval_ms 60_000
  @default_compressed true
  @default_touch_on_read true

  @doc """
  Start the session store cache process.
  """
  def start_link(opts \\ []) do
    Cachex.start_link(cache_options(opts))
  end

  @doc false
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  @doc """
  Insert or replace one session by id.
  """
  @spec put_session(String.t(), Session.t()) :: :ok
  def put_session(session_id, %Session{id: session_id} = session)
      when is_binary(session_id) and session_id != "" do
    _ = cache_put(session_id, session)
    :ok
  end

  @doc """
  Insert or replace one session using its embedded id.
  """
  @spec put_session(Session.t()) :: :ok
  def put_session(%Session{id: session_id} = session)
      when is_binary(session_id) and session_id != "" do
    put_session(session_id, session)
  end

  @doc """
  Read one session by id.

  Resolution order:

  - local Cachex (hot)
  - archive adapter (cold) with cache write-back on hit
  """
  @spec get_session(String.t()) :: {:ok, Session.t()} | {:error, term()}
  def get_session(session_id) when is_binary(session_id) and session_id != "" do
    case Cachex.get(@cache, session_id) do
      {:ok, %Session{} = session} ->
        maybe_refresh_ttl(session_id, session)
        {:ok, session}

      _ ->
        load_session_from_archive(session_id)
    end
  end

  def get_session(_session_id), do: {:error, :invalid_session_id}

  @doc """
  Delete one session by id.
  """
  @spec delete_session(String.t()) :: :ok
  def delete_session(session_id) when is_binary(session_id) and session_id != "" do
    _ = Cachex.del(@cache, session_id)
    :ok
  end

  @doc """
  Number of session entries currently in hot storage.
  """
  @spec size() :: non_neg_integer()
  def size do
    session_entries()
    |> length()
  end

  @doc """
  All session ids currently in hot storage.
  """
  @spec session_ids() :: [String.t()]
  def session_ids do
    session_entries()
    |> Enum.map(fn {session_id, _session} -> session_id end)
  end

  @doc """
  Approximate Cachex memory usage for the hot session cache.
  """
  @spec memory_bytes() :: non_neg_integer()
  def memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 -> bytes
      _ -> 0
    end
  end

  @doc false
  @spec clear() :: :ok
  def clear do
    _ = Cachex.clear(@cache)
    :ok
  end

  defp cache_options(opts) when is_list(opts) do
    [
      name: @cache,
      compressed: cache_compressed?(),
      expiration:
        expiration(
          default: cache_ttl_ms(),
          interval: cache_purge_interval_ms()
        )
    ] ++ opts
  end

  defp cache_ttl_ms do
    case Application.get_env(:starcite, :session_store_ttl_ms, @default_ttl_ms) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :session_store_ttl_ms: #{inspect(value)} (expected positive integer)"
    end
  end

  defp cache_compressed? do
    case Application.get_env(:starcite, :session_store_compressed, @default_compressed) do
      value when is_boolean(value) ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :session_store_compressed: #{inspect(value)} (expected true/false)"
    end
  end

  defp cache_purge_interval_ms do
    case Application.get_env(
           :starcite,
           :session_store_purge_interval_ms,
           @default_purge_interval_ms
         ) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :session_store_purge_interval_ms: #{inspect(value)} (expected positive integer)"
    end
  end

  defp touch_on_read? do
    case Application.get_env(:starcite, :session_store_touch_on_read, @default_touch_on_read) do
      value when is_boolean(value) ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :session_store_touch_on_read: #{inspect(value)} (expected true/false)"
    end
  end

  defp cache_put(session_id, %Session{} = session)
       when is_binary(session_id) and session_id != "" do
    case Cachex.put(@cache, session_id, session, ttl: cache_ttl_ms()) do
      {:ok, _result} -> :ok
      _ -> :ok
    end
  end

  defp maybe_refresh_ttl(session_id, %Session{} = _session)
       when is_binary(session_id) and session_id != "" do
    if touch_on_read?() do
      _ = Cachex.touch(@cache, session_id)
    end

    :ok
  end

  defp load_session_from_archive(session_id)
       when is_binary(session_id) and session_id != "" do
    with {:ok, %{sessions: sessions}} when is_list(sessions) <-
           Store.list_sessions_by_ids([session_id], %{limit: 1, cursor: nil, metadata: %{}}),
         {:ok, %Session{} = session} <- session_from_archive_rows(session_id, sessions) do
      :ok = cache_put(session_id, session)
      {:ok, session}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp session_from_archive_rows(_session_id, []), do: {:error, :session_not_found}

  defp session_from_archive_rows(session_id, [row | _rest]),
    do: session_from_archive_row(session_id, row)

  defp session_from_archive_row(
         session_id,
         %{
           id: session_id,
           title: title,
           tenant_id: tenant_id,
           creator_principal: creator_principal,
           metadata: metadata
         } = row
       )
       when is_binary(tenant_id) and tenant_id != "" and (is_binary(title) or is_nil(title)) and
              is_map(metadata) and
              (is_map(creator_principal) or is_struct(creator_principal) or
                 is_nil(creator_principal)) do
    with {:ok, inserted_at} <- archive_created_at(row),
         {:ok, last_seq} <- archive_non_neg_integer(row, :last_seq, 0),
         {:ok, archived_seq} <- archive_non_neg_integer(row, :archived_seq, 0),
         {:ok, last_progress_poll} <- archive_non_neg_integer(row, :last_progress_poll, 0),
         :ok <- validate_archive_cursors(last_seq, archived_seq),
         {:ok, retention} <- archive_retention(row),
         {:ok, producer_cursors} <- archive_producer_cursors(row) do
      session =
        Session.new(session_id,
          title: title,
          tenant_id: tenant_id,
          creator_principal: creator_principal,
          metadata: metadata,
          timestamp: inserted_at,
          tail_keep: retention.tail_keep,
          producer_max_entries: retention.producer_max_entries,
          last_progress_poll: last_progress_poll
        )

      {:ok,
       %Session{
         session
         | last_seq: last_seq,
           archived_seq: archived_seq,
           producer_cursors: producer_cursors
       }}
    end
  end

  defp session_from_archive_row(_session_id, _row), do: {:error, :archive_read_unavailable}

  defp archive_created_at(row) when is_map(row) do
    case Map.get(row, :created_at) || Map.get(row, "created_at") do
      %DateTime{} = datetime -> {:ok, DateTime.to_naive(datetime)}
      %NaiveDateTime{} = datetime -> {:ok, datetime}
      value when is_binary(value) -> parse_archive_naive_datetime(value)
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp parse_archive_naive_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_naive(datetime)}
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp archive_non_neg_integer(row, key, default)
       when is_map(row) and is_atom(key) and is_integer(default) and default >= 0 do
    case Map.get(row, key, Map.get(row, Atom.to_string(key), default)) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp validate_archive_cursors(last_seq, archived_seq)
       when is_integer(last_seq) and is_integer(archived_seq) and last_seq >= 0 and
              archived_seq >= 0 and
              archived_seq <= last_seq,
       do: :ok

  defp validate_archive_cursors(_last_seq, _archived_seq), do: {:error, :archive_read_unavailable}

  defp archive_retention(row) when is_map(row) do
    retention = Map.get(row, :retention, Map.get(row, "retention", %{}))

    if is_map(retention) do
      with {:ok, tail_keep} <- archive_retention_value(retention, :tail_keep, 1_000),
           {:ok, producer_max_entries} <-
             archive_retention_value(retention, :producer_max_entries, 10_000) do
        {:ok, %{tail_keep: tail_keep, producer_max_entries: producer_max_entries}}
      end
    else
      {:error, :archive_read_unavailable}
    end
  end

  defp archive_retention_value(retention, key, default)
       when is_map(retention) and is_atom(key) and is_integer(default) and default > 0 do
    case Map.get(retention, key, Map.get(retention, Atom.to_string(key), default)) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp archive_producer_cursors(row) when is_map(row) do
    case Map.get(row, :producer_cursors, Map.get(row, "producer_cursors", %{})) do
      value when is_map(value) -> decode_archive_producer_cursors(value)
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp decode_archive_producer_cursors(cursors) when map_size(cursors) == 0, do: {:ok, %{}}

  defp decode_archive_producer_cursors(cursors) when is_map(cursors) do
    Enum.reduce_while(cursors, {:ok, %{}}, fn
      {producer_id, cursor}, {:ok, acc}
      when is_binary(producer_id) and producer_id != "" and is_map(cursor) ->
        with {:ok, producer_seq} <- archive_cursor_integer(cursor, :producer_seq),
             {:ok, session_seq} <- archive_cursor_integer(cursor, :session_seq),
             {:ok, hash} <- archive_cursor_hash(cursor) do
          {:cont,
           {:ok,
            Map.put(acc, producer_id, %{
              producer_seq: producer_seq,
              session_seq: session_seq,
              hash: hash
            })}}
        end

      _, _acc ->
        {:halt, {:error, :archive_read_unavailable}}
    end)
  end

  defp archive_cursor_integer(cursor, key) when is_map(cursor) and is_atom(key) do
    case Map.get(cursor, key, Map.get(cursor, Atom.to_string(key))) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, :archive_read_unavailable}
    end
  end

  defp archive_cursor_hash(cursor) when is_map(cursor) do
    case Map.get(cursor, :hash, Map.get(cursor, "hash")) do
      value when is_binary(value) and value != "" ->
        {:ok, decode_archive_hash(value)}

      _ ->
        {:error, :archive_read_unavailable}
    end
  end

  defp decode_archive_hash(value) when is_binary(value) do
    case Base.url_decode64(value, padding: false) do
      {:ok, decoded} when decoded != "" -> decoded
      _ -> value
    end
  end

  defp session_entries do
    case Cachex.keys(@cache) do
      {:ok, keys} when is_list(keys) ->
        keys
        |> Enum.filter(&(is_binary(&1) and &1 != ""))
        |> Enum.reduce([], fn session_id, acc ->
          case Cachex.get(@cache, session_id) do
            {:ok, %Session{} = session} -> [{session_id, session} | acc]
            _ -> acc
          end
        end)
        |> Enum.reverse()

      _ ->
        []
    end
  end
end
