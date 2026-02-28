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
  Read one session by id from local cache only.

  This helper never falls back to archive reads and is safe for low-latency
  metadata extraction paths such as telemetry labeling.
  """
  @spec peek_session(String.t()) :: {:ok, Session.t()} | :error
  def peek_session(session_id) when is_binary(session_id) and session_id != "" do
    case Cachex.get(@cache, session_id) do
      {:ok, %Session{} = session} -> {:ok, session}
      _ -> :error
    end
  end

  def peek_session(_session_id), do: :error

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
         %{id: session_id, title: title, creator_principal: creator_principal, metadata: metadata}
       )
       when (is_binary(title) or is_nil(title)) and is_map(metadata) and
              (is_map(creator_principal) or is_nil(creator_principal)) do
    {:ok,
     Session.new(session_id,
       title: title,
       creator_principal: creator_principal,
       metadata: metadata
     )}
  end

  defp session_from_archive_row(_session_id, _row), do: {:error, :archive_read_unavailable}

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
