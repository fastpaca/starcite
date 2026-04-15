defmodule Starcite.Storage.EventArchive do
  @moduledoc """
  Durable archived event persistence backed by `Starcite.Repo`.

  Archived events are written to the Postgres `events` table. This module owns
  archived-read caching on top of that durable store.

  During a legacy-S3 cutover window, reads can fall back to legacy S3 and
  backfill Postgres, and archive writes can temporarily double-write to both
  stores.
  """

  use GenServer
  require Logger

  import Ecto.Query

  alias Starcite.Repo
  alias Starcite.Storage.ArchiveEventRecord
  alias Starcite.Storage.EventArchive.S3
  alias Starcite.Storage.SessionCatalog

  @cache :starcite_archive_read_cache
  @default_cache_chunk_size 256
  @default_write_batch_size 1_000
  @default_read_batch_size 1_000
  @default_legacy_s3_batch_size 1_000
  @boot_backfill_scan_batch_size 100
  @boot_backfill_lock_key 8_212_965_103_174_221
  @postgres_safe_parameter_limit 60_000
  @write_columns_per_row 13

  @type event_row :: %{
          required(:session_id) => String.t(),
          required(:seq) => pos_integer(),
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          required(:producer_id) => String.t(),
          required(:producer_seq) => pos_integer(),
          required(:tenant_id) => String.t(),
          optional(:source) => String.t() | nil,
          required(:metadata) => map(),
          required(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t() | DateTime.t()
        }

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  @doc """
  Start the archive storage owner.
  """
  def start_link(opts \\ []) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    if legacy_s3_boot_backfill_enabled?() do
      send(self(), :run_legacy_s3_backfill)
    end

    {:ok, %{}}
  end

  @impl true
  def handle_info(:run_legacy_s3_backfill, state) do
    :ok = run_legacy_s3_backfill()
    {:noreply, state}
  end

  @spec write_events([event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Persist archive event rows to Postgres. Duplicate `(session_id, seq)` rows are
  ignored so repeated flushes remain idempotent. When legacy S3 cutover is
  enabled, writes must also succeed against S3 before the batch is considered
  durable.
  """
  def write_events([]), do: {:ok, 0}

  def write_events(rows) when is_list(rows) do
    with {:ok, normalized_rows} <- normalize_write_rows(rows),
         :ok <- validate_session_tenants(normalized_rows) do
      with {:ok, inserted} <- write_events_postgres_rows(normalized_rows),
           :ok <- maybe_write_events_legacy_s3(normalized_rows) do
        {:ok, inserted}
      end
    else
      {:error, _reason} = error ->
        error
    end
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @spec write_events_postgres([event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc """
  Persist archive event rows to Postgres only.

  This exists for one-off migration tooling and S3 backfill paths that should
  not write back into the legacy store.
  """
  def write_events_postgres([]), do: {:ok, 0}

  def write_events_postgres(rows) when is_list(rows) do
    with {:ok, normalized_rows} <- normalize_write_rows(rows),
         :ok <- validate_session_tenants(normalized_rows) do
      write_events_postgres_rows(normalized_rows)
    else
      {:error, _reason} = error ->
        error
    end
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  @spec read_events(String.t(), pos_integer(), pos_integer()) :: {:ok, [map()]} | {:error, term()}
  @doc """
  Read archived events from cache first, then fill cache misses from Postgres.
  """
  def read_events(session_id, from_seq, to_seq)
      when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
             is_integer(to_seq) and to_seq >= from_seq do
    session_id
    |> read_postgres_events(from_seq, to_seq)
    |> maybe_backfill_from_legacy_s3(session_id, from_seq, to_seq)
  end

  @doc false
  @spec run_legacy_s3_backfill() :: :ok
  def run_legacy_s3_backfill do
    if legacy_s3_boot_backfill_enabled?() do
      maybe_run_legacy_s3_backfill()
    else
      :ok
    end
  end

  @spec get_cached_event(String.t(), pos_integer()) :: {:ok, map()} | :error
  @doc """
  Return one archived event from local cache by session and sequence.
  """
  def get_cached_event(session_id, seq)
      when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    chunk_start = chunk_start_for(seq, cache_chunk_size())

    case Map.fetch(get_cached_chunk(session_id, chunk_start), seq) do
      {:ok, event} -> {:ok, event}
      :error -> :error
    end
  end

  @spec cache_events(String.t(), [map()]) :: :ok
  @doc """
  Insert archived events into local archived-read cache.
  """
  def cache_events(session_id, events)
      when is_binary(session_id) and session_id != "" and is_list(events) do
    chunk_size = cache_chunk_size()

    events_by_chunk =
      Enum.reduce(events, %{}, fn event, acc ->
        put_event_in_chunk_map(acc, event, chunk_size)
      end)

    Enum.each(events_by_chunk, fn {chunk_start, additions} ->
      existing = get_cached_chunk(session_id, chunk_start)
      merged = Map.merge(existing, additions)
      :ok = put_cached_chunk(session_id, chunk_start, merged)
    end)

    :ok
  end

  @spec clear_cache() :: :ok | :error
  @doc """
  Clear all archived-read cache entries.
  """
  def clear_cache do
    case Cachex.clear(@cache) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec cache_memory_bytes() :: {:ok, non_neg_integer()} | :error
  @doc """
  Return archived-read cache memory usage in bytes.
  """
  def cache_memory_bytes do
    case Cachex.inspect(@cache, {:memory, :bytes}) do
      {:ok, bytes} when is_integer(bytes) and bytes >= 0 -> {:ok, bytes}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @spec cache_memory_bytes_or_zero() :: non_neg_integer()
  @doc """
  Return archived-read cache memory usage, falling back to `0` on inspection
  failures.
  """
  def cache_memory_bytes_or_zero do
    case cache_memory_bytes() do
      {:ok, bytes} -> bytes
      :error -> 0
    end
  end

  @spec evict_cache_to_target_memory(non_neg_integer()) :: :ok
  @doc """
  Evict archived-read cache entries until usage is at or below
  `target_cache_bytes`.
  """
  def evict_cache_to_target_memory(target_cache_bytes)
      when is_integer(target_cache_bytes) and target_cache_bytes >= 0 do
    cache_entries =
      cache_stream()
      |> Enum.filter(fn
        {:entry, {:event_chunk, _session_id, _chunk_start}, _touched, _ttl, _value} -> true
        _other -> false
      end)
      |> Enum.sort_by(fn {:entry, _key, touched, _ttl, _value} -> touched end)

    _bytes_after =
      Enum.reduce_while(cache_entries, cache_memory_bytes_or_zero(), fn
        {:entry, key, _touched, _ttl, _value}, current_bytes ->
          if current_bytes <= target_cache_bytes do
            {:halt, current_bytes}
          else
            _ = cache_del(key)
            {:cont, cache_memory_bytes_or_zero()}
          end
      end)

    :ok
  end

  defp read_postgres_events(session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, cached_by_seq, missing_ranges} <-
           cached_events_and_missing_ranges(session_id, from_seq, to_seq),
         {:ok, fetched_events} <- fetch_missing_ranges(session_id, missing_ranges) do
      :ok = cache_events(session_id, fetched_events)

      merged =
        cached_by_seq
        |> Map.merge(Map.new(fetched_events, fn %{seq: seq} = event -> {seq, event} end))
        |> Map.values()
        |> Enum.sort_by(& &1.seq)

      {:ok, merged}
    end
  end

  defp maybe_backfill_from_legacy_s3({:ok, events}, session_id, from_seq, to_seq)
       when is_list(events) and is_binary(session_id) and session_id != "" and
              is_integer(from_seq) and is_integer(to_seq) do
    if legacy_s3_enabled?() do
      case SessionCatalog.get_archive_context(session_id) do
        {:ok, %{tenant_id: tenant_id, archived_seq: archived_seq}} ->
          expected_to_seq = min(to_seq, archived_seq)

          if archived_range_complete?(events, from_seq, expected_to_seq) do
            {:ok, events}
          else
            backfill_requested_range_from_legacy_s3(
              session_id,
              tenant_id,
              archived_seq,
              from_seq,
              to_seq
            )
          end

        {:error, _reason} ->
          {:ok, events}
      end
    else
      {:ok, events}
    end
  end

  defp maybe_backfill_from_legacy_s3({:error, _reason}, session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and
              is_integer(to_seq) do
    if legacy_s3_enabled?() do
      case SessionCatalog.get_archive_context(session_id) do
        {:ok, %{archived_seq: archived_seq}} when archived_seq < from_seq ->
          {:ok, []}

        {:ok, %{tenant_id: tenant_id, archived_seq: archived_seq}} ->
          backfill_requested_range_from_legacy_s3(
            session_id,
            tenant_id,
            archived_seq,
            from_seq,
            to_seq
          )

        {:error, _reason} ->
          {:error, :archive_read_unavailable}
      end
    else
      {:error, :archive_read_unavailable}
    end
  end

  defp fetch_missing_ranges(_session_id, []), do: {:ok, []}

  defp fetch_missing_ranges(session_id, [{from_seq, to_seq} | rest])
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    with {:ok, events} <- read_persisted_events(session_id, from_seq, to_seq),
         {:ok, tail} <- fetch_missing_ranges(session_id, rest) do
      {:ok, events ++ tail}
    end
  end

  defp read_persisted_events(session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    from_seq
    |> bounded_ranges(to_seq, read_batch_size())
    |> Enum.reduce_while({:ok, []}, fn {batch_from, batch_to}, {:ok, acc} ->
      case read_persisted_events_batch(session_id, batch_from, batch_to) do
        {:ok, events} -> {:cont, {:ok, [events | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, batches} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp read_persisted_events_batch(session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    query =
      from(event in ArchiveEventRecord,
        where:
          event.session_id == ^session_id and event.seq >= ^from_seq and event.seq <= ^to_seq,
        order_by: [asc: event.seq],
        select: %{
          seq: event.seq,
          type: event.type,
          payload: event.payload,
          actor: event.actor,
          producer_id: event.producer_id,
          producer_seq: event.producer_seq,
          tenant_id: event.tenant_id,
          source: event.source,
          metadata: event.metadata,
          refs: event.refs,
          idempotency_key: event.idempotency_key,
          inserted_at: event.inserted_at
        }
      )

    {:ok, Enum.map(Repo.all(query), &normalize_persisted_event/1)}
  rescue
    _ -> {:error, :archive_read_unavailable}
  end

  defp normalize_persisted_event(%{inserted_at: %DateTime{} = inserted_at} = event) do
    %{event | inserted_at: DateTime.to_naive(inserted_at)}
  end

  defp normalize_persisted_event(event) when is_map(event), do: event

  defp insert_rows_in_batches(rows) when is_list(rows) do
    rows
    |> Enum.chunk_every(write_batch_size())
    |> Enum.reduce_while({:ok, 0}, fn batch, {:ok, total_inserted} ->
      case insert_rows_batch(batch) do
        {:ok, inserted} -> {:cont, {:ok, total_inserted + inserted}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp write_events_postgres_rows(rows) when is_list(rows) do
    insert_rows_in_batches(rows)
  end

  defp insert_rows_batch(rows) when is_list(rows) and rows != [] do
    {inserted, _records} =
      Repo.insert_all(
        ArchiveEventRecord,
        rows,
        on_conflict: :nothing,
        conflict_target: [:session_id, :seq]
      )

    {:ok, inserted}
  rescue
    _ -> {:error, :archive_write_unavailable}
  end

  defp bounded_ranges(from_seq, to_seq, batch_size)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_integer(batch_size) and batch_size > 0 do
    Stream.unfold(from_seq, fn
      current when current > to_seq ->
        nil

      current ->
        batch_to = min(current + batch_size - 1, to_seq)
        {{current, batch_to}, batch_to + 1}
    end)
    |> Enum.to_list()
  end

  defp write_batch_size do
    configured_batch_size =
      case Application.get_env(:starcite, :archive_db_write_batch_size, @default_write_batch_size) do
        value when is_integer(value) and value > 0 ->
          value

        value ->
          raise ArgumentError,
                "invalid value for :archive_db_write_batch_size: #{inspect(value)} (expected positive integer)"
      end

    min(configured_batch_size, max_rows_per_insert())
  end

  defp read_batch_size do
    case Application.get_env(:starcite, :archive_db_read_batch_size, @default_read_batch_size) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :archive_db_read_batch_size: #{inspect(value)} (expected positive integer)"
    end
  end

  defp max_rows_per_insert do
    div(@postgres_safe_parameter_limit, @write_columns_per_row)
  end

  defp normalize_write_rows(rows) when is_list(rows) do
    rows
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case normalize_write_row(row) do
        {:ok, normalized_row} -> {:cont, {:ok, [normalized_row | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, normalized_rows} -> {:ok, Enum.reverse(normalized_rows)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_write_row(%{
         session_id: session_id,
         seq: seq,
         type: type,
         payload: payload,
         actor: actor,
         producer_id: producer_id,
         producer_seq: producer_seq,
         tenant_id: tenant_id,
         source: source,
         metadata: metadata,
         refs: refs,
         idempotency_key: idempotency_key,
         inserted_at: inserted_at
       })
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 and
              is_binary(type) and type != "" and is_map(payload) and is_binary(actor) and
              actor != "" and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 and is_binary(tenant_id) and
              tenant_id != "" and (is_binary(source) or is_nil(source)) and is_map(metadata) and
              is_map(refs) and (is_binary(idempotency_key) or is_nil(idempotency_key)) do
    with {:ok, inserted_at} <- normalize_inserted_at(inserted_at) do
      {:ok,
       %{
         session_id: session_id,
         seq: seq,
         type: type,
         payload: payload,
         actor: actor,
         producer_id: producer_id,
         producer_seq: producer_seq,
         tenant_id: tenant_id,
         source: source,
         metadata: metadata,
         refs: refs,
         idempotency_key: idempotency_key,
         inserted_at: inserted_at
       }}
    end
  end

  defp normalize_write_row(_row), do: {:error, :archive_write_unavailable}

  defp normalize_inserted_at(%NaiveDateTime{} = inserted_at) do
    {:ok, inserted_at |> DateTime.from_naive!("Etc/UTC") |> DateTime.truncate(:second)}
  end

  defp normalize_inserted_at(%DateTime{} = inserted_at) do
    {:ok, inserted_at |> DateTime.shift_zone!("Etc/UTC") |> DateTime.truncate(:second)}
  end

  defp normalize_inserted_at(_inserted_at), do: {:error, :archive_write_unavailable}

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

  defp maybe_write_events_legacy_s3(rows) when is_list(rows) do
    if legacy_s3_enabled?() do
      case S3.write_events(rows, legacy_s3_storage_opts()) do
        {:ok, _inserted} -> :ok
        {:error, _reason} -> {:error, :archive_write_unavailable}
      end
    else
      :ok
    end
  end

  defp archived_range_complete?(_events, from_seq, expected_to_seq)
       when is_integer(from_seq) and is_integer(expected_to_seq) and expected_to_seq < from_seq,
       do: true

  defp archived_range_complete?(events, from_seq, expected_to_seq)
       when is_list(events) and is_integer(from_seq) and is_integer(expected_to_seq) and
              expected_to_seq >= from_seq do
    expected_count = expected_to_seq - from_seq + 1

    length(events) == expected_count and
      Enum.map(events, & &1.seq) == Enum.to_list(from_seq..expected_to_seq)
  end

  defp backfill_requested_range_from_legacy_s3(
         _session_id,
         _tenant_id,
         archived_seq,
         from_seq,
         _to_seq
       )
       when is_integer(archived_seq) and archived_seq >= 0 and is_integer(from_seq) and
              from_seq > archived_seq do
    {:ok, []}
  end

  defp backfill_requested_range_from_legacy_s3(
         session_id,
         tenant_id,
         archived_seq,
         from_seq,
         to_seq
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(archived_seq) and archived_seq >= 0 and is_integer(from_seq) and
              from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    requested_to_seq = min(to_seq, archived_seq)

    case backfill_session_from_legacy_s3(
           session_id,
           tenant_id,
           archived_seq,
           from_seq,
           requested_to_seq
         ) do
      {:ok, requested_events, _inserted} -> {:ok, requested_events}
      {:error, _reason} -> {:error, :archive_read_unavailable}
    end
  end

  defp backfill_session_from_legacy_s3(
         _session_id,
         _tenant_id,
         0,
         _requested_from_seq,
         _requested_to_seq
       ),
       do: {:ok, [], 0}

  defp backfill_session_from_legacy_s3(
         session_id,
         tenant_id,
         archived_seq,
         requested_from_seq,
         requested_to_seq
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(archived_seq) and archived_seq > 0 and is_integer(requested_from_seq) and
              requested_from_seq > 0 and is_integer(requested_to_seq) and requested_to_seq >= 0 do
    1
    |> bounded_ranges(archived_seq, legacy_s3_batch_size())
    |> Enum.reduce_while({:ok, 1, 0, []}, fn {batch_from, batch_to},
                                             {:ok, expected_seq, inserted_total, requested_acc} ->
      with {:ok, events} <-
             S3.read_events(
               session_id,
               tenant_id,
               batch_from,
               batch_to,
               legacy_s3_storage_opts()
             ),
           {:ok, next_expected_seq} <- validate_migrated_batch(events, expected_seq),
           {:ok, inserted} <- write_events_postgres(add_session_id(session_id, events)) do
        requested_acc =
          collect_requested_events(
            requested_acc,
            events,
            requested_from_seq,
            requested_to_seq
          )

        {:cont, {:ok, next_expected_seq, inserted_total + inserted, requested_acc}}
      else
        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, expected_seq, inserted_total, requested_events}
      when expected_seq == archived_seq + 1 ->
        requested_events = Enum.reverse(requested_events)

        if requested_events != [] do
          :ok = cache_events(session_id, requested_events)
        end

        {:ok, requested_events, inserted_total}

      {:ok, _expected_seq, _inserted_total, _requested_events} ->
        {:error, :archive_read_unavailable}

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_migrated_batch(events, expected_seq)
       when is_list(events) and is_integer(expected_seq) and expected_seq > 0 do
    events
    |> Enum.reduce_while(expected_seq, fn
      %{seq: seq}, next_expected when is_integer(seq) and seq == next_expected ->
        {:cont, next_expected + 1}

      %{seq: _seq}, _next_expected ->
        {:halt, :gap}

      _event, _next_expected ->
        {:halt, :gap}
    end)
    |> case do
      :gap ->
        {:error, :archive_read_unavailable}

      next_expected when is_integer(next_expected) and next_expected >= expected_seq ->
        {:ok, next_expected}
    end
  end

  defp collect_requested_events(acc, events, requested_from_seq, requested_to_seq)
       when is_list(acc) and is_list(events) and is_integer(requested_from_seq) and
              is_integer(requested_to_seq) do
    Enum.reduce(events, acc, fn
      %{seq: seq} = event, inner
      when is_integer(seq) and seq >= requested_from_seq and seq <= requested_to_seq ->
        [event | inner]

      _event, inner ->
        inner
    end)
  end

  defp add_session_id(session_id, events)
       when is_binary(session_id) and session_id != "" and is_list(events) do
    Enum.map(events, &Map.put(&1, :session_id, session_id))
  end

  defp maybe_run_legacy_s3_backfill do
    if Repo.checked_out?() do
      do_run_legacy_s3_backfill()
    else
      Repo.checkout(fn ->
        do_run_legacy_s3_backfill()
      end)
    end

    :ok
  rescue
    error ->
      Logger.warning("legacy S3 boot backfill crashed: #{Exception.message(error)}")
      :ok
  end

  defp do_run_legacy_s3_backfill do
    case acquire_backfill_lock() do
      :ok ->
        Logger.info("legacy S3 boot backfill starting")

        try do
          {sessions, inserted_rows} = run_legacy_s3_backfill_batches(nil, 0, 0)

          Logger.info(
            "legacy S3 boot backfill finished sessions=#{sessions} inserted_rows=#{inserted_rows}"
          )
        after
          :ok = release_backfill_lock()
        end

      :busy ->
        Logger.info("legacy S3 boot backfill skipped because another node is running it")

      :error ->
        Logger.warning("legacy S3 boot backfill unavailable")
    end
  end

  defp run_legacy_s3_backfill_batches(cursor, migrated_sessions, inserted_rows) do
    case load_backfill_sessions(cursor, @boot_backfill_scan_batch_size) do
      [] ->
        {migrated_sessions, inserted_rows}

      sessions ->
        {migrated_sessions, inserted_rows} =
          Enum.reduce(sessions, {migrated_sessions, inserted_rows}, fn
            %{id: session_id, tenant_id: tenant_id, archived_seq: archived_seq},
            {sessions_acc, rows_acc} ->
              case maybe_backfill_session_from_legacy_s3(session_id, tenant_id, archived_seq) do
                {:ok, inserted} ->
                  {sessions_acc + 1, rows_acc + inserted}

                {:error, reason} ->
                  Logger.warning(
                    "legacy S3 backfill failed session_id=#{session_id} reason=#{inspect(reason)}"
                  )

                  {sessions_acc, rows_acc}
              end
          end)

        %{id: next_cursor} = List.last(sessions)
        run_legacy_s3_backfill_batches(next_cursor, migrated_sessions, inserted_rows)
    end
  end

  defp maybe_backfill_session_from_legacy_s3(_session_id, _tenant_id, archived_seq)
       when is_integer(archived_seq) and archived_seq <= 0,
       do: {:ok, 0}

  defp maybe_backfill_session_from_legacy_s3(session_id, tenant_id, archived_seq)
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and
              is_integer(archived_seq) and archived_seq > 0 do
    if archive_session_backfilled?(session_id, archived_seq) do
      {:ok, 0}
    else
      case backfill_session_from_legacy_s3(
             session_id,
             tenant_id,
             archived_seq,
             archived_seq + 1,
             archived_seq
           ) do
        {:ok, _requested_events, inserted} -> {:ok, inserted}
        {:error, _reason} = error -> error
      end
    end
  end

  defp archive_session_backfilled?(session_id, archived_seq)
       when is_binary(session_id) and session_id != "" and is_integer(archived_seq) and
              archived_seq >= 0 do
    query =
      from(event in ArchiveEventRecord,
        where: event.session_id == ^session_id,
        select: {count(event.seq), max(event.seq)}
      )

    case Repo.one(query) do
      {count, max_seq} when is_integer(count) and count >= 0 ->
        count == archived_seq and normalize_max_seq(max_seq) == archived_seq

      _ ->
        false
    end
  rescue
    _ -> false
  end

  defp normalize_max_seq(nil), do: 0
  defp normalize_max_seq(max_seq) when is_integer(max_seq) and max_seq >= 0, do: max_seq
  defp normalize_max_seq(_max_seq), do: -1

  defp load_backfill_sessions(cursor, limit)
       when (is_nil(cursor) or (is_binary(cursor) and cursor != "")) and is_integer(limit) and
              limit > 0 do
    "sessions"
    |> then(fn query ->
      if is_nil(cursor), do: query, else: where(query, [s], field(s, :id) > ^cursor)
    end)
    |> where([s], field(s, :archived_seq) > 0)
    |> order_by([s], asc: field(s, :id))
    |> limit(^limit)
    |> select([s], %{
      id: field(s, :id),
      tenant_id: field(s, :tenant_id),
      archived_seq: field(s, :archived_seq)
    })
    |> Repo.all()
  rescue
    _ -> []
  end

  defp acquire_backfill_lock do
    case Repo.query("SELECT pg_try_advisory_lock($1)", [@boot_backfill_lock_key]) do
      {:ok, %{rows: [[true]]}} -> :ok
      {:ok, %{rows: [[false]]}} -> :busy
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp release_backfill_lock do
    case Repo.query("SELECT pg_advisory_unlock($1)", [@boot_backfill_lock_key]) do
      {:ok, _result} -> :ok
      _ -> :ok
    end
  rescue
    _ -> :ok
  end

  defp legacy_s3_enabled? do
    case Keyword.get(legacy_s3_opts(), :enabled, false) do
      true ->
        true

      false ->
        false

      value ->
        raise ArgumentError,
              "invalid value for :enabled in :archive_legacy_s3_opts: #{inspect(value)}"
    end
  end

  defp legacy_s3_boot_backfill_enabled? do
    legacy_s3_enabled?() and
      case Keyword.get(legacy_s3_opts(), :boot_backfill, false) do
        true ->
          true

        false ->
          false

        value ->
          raise ArgumentError,
                "invalid value for :boot_backfill in :archive_legacy_s3_opts: #{inspect(value)}"
      end
  end

  defp legacy_s3_opts do
    Application.get_env(:starcite, :archive_legacy_s3_opts, [])
    |> Keyword.new()
  end

  defp legacy_s3_storage_opts do
    legacy_s3_opts()
    |> Keyword.drop([:enabled, :boot_backfill, :migrate_batch_size])
  end

  defp legacy_s3_batch_size do
    case Keyword.get(legacy_s3_opts(), :migrate_batch_size, @default_legacy_s3_batch_size) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        raise ArgumentError,
              "invalid value for :migrate_batch_size in :archive_legacy_s3_opts: #{inspect(value)} (expected positive integer)"
    end
  end

  defp cached_events_and_missing_ranges(session_id, from_seq, to_seq)
       when is_binary(session_id) and session_id != "" and is_integer(from_seq) and from_seq > 0 and
              is_integer(to_seq) and to_seq >= from_seq do
    chunk_size = cache_chunk_size()

    cached_by_seq =
      chunk_starts_for_range(from_seq, to_seq, chunk_size)
      |> Enum.reduce(%{}, fn chunk_start, acc ->
        chunk = get_cached_chunk(session_id, chunk_start)

        Enum.reduce(chunk, acc, fn
          {seq, event}, inner when is_integer(seq) and seq >= from_seq and seq <= to_seq ->
            Map.put(inner, seq, event)

          _, inner ->
            inner
        end)
      end)

    present_seqs =
      cached_by_seq
      |> Map.keys()
      |> Enum.sort()

    {:ok, cached_by_seq, missing_ranges(from_seq, to_seq, present_seqs)}
  end

  defp cache_chunk_size do
    case Application.get_env(:starcite, :event_store_cache_chunk_size, @default_cache_chunk_size) do
      size when is_integer(size) and size > 0 -> size
      _ -> @default_cache_chunk_size
    end
  end

  defp put_event_in_chunk_map(acc, %{seq: seq} = event, chunk_size)
       when is_map(acc) and is_integer(seq) and seq > 0 and is_integer(chunk_size) and
              chunk_size > 0 do
    chunk_start = chunk_start_for(seq, chunk_size)
    normalized_event = normalize_cached_event(event)

    Map.update(
      acc,
      chunk_start,
      %{seq => normalized_event},
      &Map.put(&1, seq, normalized_event)
    )
  end

  defp put_event_in_chunk_map(_acc, event, _chunk_size) do
    raise ArgumentError,
          "invalid archived cache event shape: #{inspect(event)} (expected map with positive integer :seq)"
  end

  defp normalize_cached_event(%{} = event) do
    Map.delete(event, :session_id)
  end

  defp missing_ranges(from_seq, to_seq, present_seqs)
       when is_integer(from_seq) and from_seq > 0 and is_integer(to_seq) and to_seq >= from_seq and
              is_list(present_seqs) do
    {ranges, next_expected} =
      Enum.reduce(present_seqs, {[], from_seq}, fn
        seq, {acc, expected} when seq < expected ->
          {acc, expected}

        seq, {acc, expected} when seq == expected ->
          {acc, expected + 1}

        seq, {acc, expected} ->
          {[{expected, seq - 1} | acc], seq + 1}
      end)

    ranges =
      if next_expected <= to_seq do
        [{next_expected, to_seq} | ranges]
      else
        ranges
      end

    Enum.reverse(ranges)
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

  defp chunk_key(session_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    {:event_chunk, session_id, chunk_start}
  end

  defp get_cached_chunk(session_id, chunk_start)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 do
    key = chunk_key(session_id, chunk_start)

    case cache_get(key) do
      {:ok, chunk} when is_map(chunk) ->
        chunk

      _ ->
        %{}
    end
  end

  defp put_cached_chunk(session_id, chunk_start, chunk)
       when is_binary(session_id) and session_id != "" and is_integer(chunk_start) and
              chunk_start > 0 and
              is_map(chunk) do
    key = chunk_key(session_id, chunk_start)
    _ = cache_put(key, chunk)
    :ok
  end

  defp cache_get(key) do
    case Cachex.get(@cache, key) do
      {:ok, value} -> {:ok, value}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_put(key, value) do
    case Cachex.put(@cache, key, value) do
      {:ok, true} -> :ok
      {:ok, _other} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_del(key) do
    case Cachex.del(@cache, key) do
      {:ok, _} -> :ok
      _ -> :error
    end
  rescue
    _ -> :error
  end

  defp cache_stream do
    Cachex.stream!(@cache)
  rescue
    _ -> []
  end
end
