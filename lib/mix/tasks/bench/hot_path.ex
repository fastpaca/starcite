defmodule Mix.Tasks.Bench.HotPath do
  require Logger

  alias Starcite.Auth.Principal
  alias Starcite.Config.Size
  alias Starcite.WritePath

  def run do
    started_at_ms = System.monotonic_time(:millisecond)
    IO.puts("Hot-path Benchee phase: stopping applications...")
    ensure_apps_stopped()
    config = benchmark_config()
    configure_runtime(config)

    IO.puts("Hot-path Benchee phase: starting application tree...")
    app_start_started_at_ms = System.monotonic_time(:millisecond)
    Mix.Task.run("app.start")

    IO.puts(
      "Hot-path Benchee phase: app.start completed in #{elapsed_ms(app_start_started_at_ms)}ms"
    )

    Logger.configure(level: config.log_level)
    print_config(config)

    session_seed_started_at_ms = System.monotonic_time(:millisecond)
    IO.puts("Hot-path Benchee phase: preparing #{config.session_count} sessions...")
    sessions = prepare_sessions(config.session_count)

    IO.puts(
      "Hot-path Benchee phase: prepared sessions in #{elapsed_ms(session_seed_started_at_ms)}ms"
    )

    session_count = tuple_size(sessions)
    producer_state = prepare_producer_state(config, sessions)
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)

    event = %{
      type: "content",
      payload: %{text: payload_text(config.payload_bytes)},
      actor: "agent:benchee",
      source: "benchmark",
      metadata: %{bench: true, scenario: "hot_path_benchee"},
      refs: %{},
      idempotency_key: nil
    }

    append_event =
      case config.batch_size do
        1 ->
          fn ->
            index = :atomics.add_get(counter, 1, 1)
            session_index = rem(index - 1, session_count) + 1
            session_id = elem(sessions, session_index - 1)

            event_with_producer =
              with_bench_producer(event, session_index, index, config, producer_state)

            append_one(session_id, event_with_producer)
          end

        batch_size ->
          fn ->
            last_index = :atomics.add_get(counter, 1, batch_size)
            first_index = last_index - batch_size + 1
            session_index = rem(first_index - 1, session_count) + 1
            session_id = elem(sessions, session_index - 1)

            events =
              for offset <- 0..(batch_size - 1) do
                index = first_index + offset
                with_bench_producer(event, session_index, index, config, producer_state)
              end

            append_batch(session_id, events)
          end
      end

    benchee_started_at_ms = System.monotonic_time(:millisecond)
    IO.puts("Hot-path Benchee phase: starting Benchee run...")

    run_benchee(
      %{scenario_name(config.batch_size) => append_event},
      parallel: config.parallel,
      warmup: config.warmup_seconds,
      time: config.time_seconds,
      memory_time: 0,
      print: [fast_warning: false]
    )

    IO.puts(
      "Hot-path Benchee phase: Benchee run completed in #{elapsed_ms(benchee_started_at_ms)}ms"
    )

    IO.puts("Hot-path Benchee total runtime: #{elapsed_ms(started_at_ms)}ms")
  end

  defp run_benchee(scenarios, options) when is_map(scenarios) and is_list(options) do
    benchee = :"Elixir.Benchee"

    if Code.ensure_loaded?(benchee) do
      apply(benchee, :run, [scenarios, options])
    else
      Mix.raise("Benchee is not available. Run benchmarks with MIX_ENV=dev.")
    end
  end

  defp ensure_apps_stopped do
    _ = Application.stop(:starcite)
    _ = Application.stop(:ra)
    :ok
  end

  defp configure_runtime(config) do
    Application.put_env(:logger, :level, config.log_level)
    Logger.configure(level: config.log_level)
    Application.put_env(:starcite, :num_groups, config.num_groups)
    Application.put_env(:starcite, :raft_data_dir, config.raft_data_dir)
    Application.put_env(:ra, :wal_write_strategy, config.ra_wal_write_strategy)
    Application.put_env(:ra, :wal_sync_method, config.ra_wal_sync_method)

    if config.clean_raft_data_dir do
      File.rm_rf!(config.raft_data_dir)
    end

    File.mkdir_p!(config.raft_data_dir)
    ra_system_dir = Path.join(config.raft_data_dir, "ra_system")
    File.mkdir_p!(ra_system_dir)
    Application.put_env(:ra, :data_dir, to_charlist(ra_system_dir))
    Application.delete_env(:ra, :wal_data_dir)

    Application.put_env(:starcite, :archive_flush_interval_ms, config.archive_flush_interval_ms)

    if max_size = System.get_env("BENCH_EVENT_STORE_MAX_SIZE") do
      Application.put_env(
        :starcite,
        :event_store_max_bytes,
        Size.parse_bytes!(max_size, "BENCH_EVENT_STORE_MAX_SIZE", examples: "256MB, 4G, 1024M")
      )
    end
  end

  defp benchmark_config do
    parallel = env_integer("BENCH_PARALLEL", 3)
    warmup_seconds = env_integer("BENCH_WARMUP_SECONDS", 5)
    time_seconds = env_integer("BENCH_TIME_SECONDS", 30)
    ra_wal_write_strategy_default = Application.get_env(:ra, :wal_write_strategy, :default)
    ra_wal_sync_method_default = Application.get_env(:ra, :wal_sync_method, :datasync)

    archive_flush_interval_ms_default =
      Application.get_env(:starcite, :archive_flush_interval_ms, 5_000)

    %{
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      num_groups: env_integer("BENCH_NUM_GROUPS", 256),
      session_count: env_integer("BENCH_SESSION_COUNT", 256),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      batch_size: env_integer("BENCH_BATCH_SIZE", 1),
      producer_mode: env_producer_mode("BENCH_PRODUCER_MODE", :stable),
      producer_pool_size: env_integer("BENCH_PRODUCER_POOL_SIZE", parallel),
      parallel: parallel,
      warmup_seconds: warmup_seconds,
      time_seconds: time_seconds,
      ra_wal_write_strategy:
        env_wal_write_strategy("BENCH_RA_WAL_WRITE_STRATEGY", ra_wal_write_strategy_default),
      ra_wal_sync_method:
        env_wal_sync_method("BENCH_RA_WAL_SYNC_METHOD", ra_wal_sync_method_default),
      archive_flush_interval_ms:
        env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", archive_flush_interval_ms_default)
    }
  end

  defp print_config(config) do
    archive_adapter =
      Application.get_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.S3)

    archive_adapter_opts = Application.get_env(:starcite, :archive_adapter_opts, [])

    IO.puts("Hot-path Benchee config:")
    IO.puts("  raft_data_dir: #{config.raft_data_dir}")
    IO.puts("  clean_raft_data_dir: #{config.clean_raft_data_dir}")
    IO.puts("  log_level: #{config.log_level}")
    IO.puts("  num_groups: #{config.num_groups}")
    IO.puts("  sessions: #{config.session_count}")
    IO.puts("  payload_bytes: #{config.payload_bytes}")
    IO.puts("  batch_size: #{config.batch_size}")
    IO.puts("  producer_mode: #{config.producer_mode}")
    IO.puts("  producer_pool_size: #{config.producer_pool_size}")
    IO.puts("  parallel: #{config.parallel}")
    IO.puts("  warmup_seconds: #{config.warmup_seconds}")
    IO.puts("  time_seconds: #{config.time_seconds}")
    IO.puts("  ra_wal_write_strategy: #{config.ra_wal_write_strategy}")
    IO.puts("  ra_wal_sync_method: #{config.ra_wal_sync_method}")
    IO.puts("  archive_adapter: #{inspect(archive_adapter)}")
    IO.puts("  archive_flush_interval_ms: #{config.archive_flush_interval_ms}")

    if archive_adapter == Starcite.Archive.Adapter.S3 do
      IO.puts("  archive_s3_endpoint: #{inspect(Keyword.get(archive_adapter_opts, :endpoint))}")
      IO.puts("  archive_s3_bucket: #{inspect(Keyword.get(archive_adapter_opts, :bucket))}")
    end

    if max_bytes = Application.get_env(:starcite, :event_store_max_bytes) do
      IO.puts("  event_store_max_bytes: #{inspect(max_bytes)}")
    end
  end

  defp prepare_sessions(session_count) when is_integer(session_count) and session_count > 0 do
    run_id = System.system_time(:millisecond)
    {:ok, principal} = Principal.new("service", "bench", :service)

    1..session_count
    |> Enum.map(fn index ->
      id = "hot-benchee-#{run_id}-#{index}"

      case WritePath.create_session_local(
             id,
             nil,
             principal,
             "service",
             %{bench: true, scenario: "hot_path_benchee"}
           ) do
        {:ok, _session} -> id
        {:error, :session_exists} -> id
        {:error, reason} -> raise "create_session failed for #{id}: #{inspect(reason)}"
      end
    end)
    |> List.to_tuple()
  end

  defp elapsed_ms(started_at_ms) when is_integer(started_at_ms) do
    System.monotonic_time(:millisecond) - started_at_ms
  end

  defp prepare_producer_state(
         %{producer_mode: :stable, producer_pool_size: producer_pool_size},
         sessions
       ) do
    session_count = tuple_size(sessions)
    producer_count = session_count * producer_pool_size
    producer_seqs = :atomics.new(producer_count, [])
    worker_counter = :atomics.new(1, [])
    :atomics.put(worker_counter, 1, 0)

    worker_slots =
      :ets.new(:bench_hot_path_worker_slots, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    producer_ids =
      sessions
      |> Tuple.to_list()
      |> Enum.map(fn session_id ->
        1..producer_pool_size
        |> Enum.map(fn producer_slot -> "#{session_id}:producer:#{producer_slot}" end)
        |> List.to_tuple()
      end)
      |> List.to_tuple()

    %{
      ids: producer_ids,
      seqs: producer_seqs,
      worker_slots: worker_slots,
      worker_counter: worker_counter
    }
  end

  defp prepare_producer_state(%{producer_mode: :unique}, _sessions), do: nil

  defp with_bench_producer(
         event,
         _session_index,
         index,
         %{producer_mode: :unique},
         _producer_state
       )
       when is_map(event) and is_integer(index) and index > 0 do
    event
    |> Map.put(:producer_id, "bench-producer-#{index}")
    |> Map.put(:producer_seq, 1)
  end

  defp with_bench_producer(
         event,
         session_index,
         _index,
         %{producer_mode: :stable, producer_pool_size: producer_pool_size},
         %{
           ids: producer_ids,
           seqs: producer_seqs,
           worker_slots: worker_slots,
           worker_counter: worker_counter
         }
       )
       when is_map(event) and is_integer(session_index) and session_index > 0 and
              is_integer(producer_pool_size) and producer_pool_size > 0 and
              is_tuple(producer_ids) do
    producer_slot = worker_slot(worker_slots, worker_counter, producer_pool_size)
    producer_counter_index = (session_index - 1) * producer_pool_size + producer_slot
    producer_id = producer_ids |> elem(session_index - 1) |> elem(producer_slot - 1)
    producer_seq = :atomics.add_get(producer_seqs, producer_counter_index, 1)

    event
    |> Map.put(:producer_id, producer_id)
    |> Map.put(:producer_seq, producer_seq)
  end

  defp worker_slot(worker_slots, worker_counter, producer_pool_size)
       when is_integer(producer_pool_size) and producer_pool_size > 0 do
    worker_pid = self()

    case :ets.lookup(worker_slots, worker_pid) do
      [{^worker_pid, slot}] when is_integer(slot) and slot > 0 ->
        slot

      [] ->
        slot = rem(:atomics.add_get(worker_counter, 1, 1) - 1, producer_pool_size) + 1
        true = :ets.insert(worker_slots, {worker_pid, slot})
        slot
    end
  end

  defp append_one(session_id, event) when is_binary(session_id) and is_map(event) do
    case WritePath.append_event(session_id, event) do
      {:ok, _reply} -> :ok
      {:error, reason} -> raise "append failed: #{inspect(reason)}"
      {:timeout, leader} -> raise "append timeout: #{inspect(leader)}"
    end
  end

  defp append_batch(session_id, [event]) when is_binary(session_id) and is_map(event) do
    append_one(session_id, event)
  end

  defp append_batch(session_id, events) when is_binary(session_id) and is_list(events) do
    case WritePath.append_events(session_id, events) do
      {:ok, _reply} -> :ok
      {:error, reason} -> raise "append batch failed: #{inspect(reason)}"
      {:timeout, leader} -> raise "append batch timeout: #{inspect(leader)}"
    end
  end

  defp scenario_name(1), do: "runtime.append_event"

  defp scenario_name(batch_size) when is_integer(batch_size) and batch_size > 1 do
    "runtime.append_events(batch_size=#{batch_size})"
  end

  defp payload_text(payload_bytes) when is_integer(payload_bytes) and payload_bytes > 0 do
    String.duplicate("x", payload_bytes)
  end

  defp env_producer_mode(name, default) when is_binary(name) and default in [:stable, :unique] do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case value |> String.trim() |> String.downcase() do
          "stable" -> :stable
          "unique" -> :unique
          other -> raise ArgumentError, "invalid producer mode for #{name}: #{inspect(other)}"
        end
    end
  end

  defp env_integer(name, default) when is_binary(name) and is_integer(default) and default > 0 do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _ -> raise ArgumentError, "invalid integer for #{name}: #{inspect(value)}"
        end
    end
  end

  defp env_boolean(name, default) when is_binary(name) and is_boolean(default) do
    case System.get_env(name) do
      nil -> default
      value -> env_boolean_value!(name, value)
    end
  end

  defp env_boolean_value!(name, value) when is_binary(name) and is_binary(value) do
    case String.downcase(String.trim(value)) do
      "1" -> true
      "true" -> true
      "yes" -> true
      "on" -> true
      "0" -> false
      "false" -> false
      "no" -> false
      "off" -> false
      _ -> raise ArgumentError, "invalid boolean for #{name}: #{inspect(value)}"
    end
  end

  defp env_log_level(name, default) when is_binary(name) and is_atom(default) do
    case System.get_env(name) do
      nil -> default
      "debug" -> :debug
      "info" -> :info
      "warning" -> :warning
      "error" -> :error
      value -> raise ArgumentError, "invalid log level for #{name}: #{inspect(value)}"
    end
  end

  defp env_wal_write_strategy(name, default)
       when is_binary(name) and default in [:default, :o_sync, :sync_after_notify] do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case value |> String.trim() |> String.downcase() do
          "default" ->
            :default

          "o_sync" ->
            :o_sync

          "sync_after_notify" ->
            :sync_after_notify

          other ->
            raise ArgumentError, "invalid wal_write_strategy for #{name}: #{inspect(other)}"
        end
    end
  end

  defp env_wal_sync_method(name, default)
       when is_binary(name) and default in [:datasync, :sync, :none] do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case value |> String.trim() |> String.downcase() do
          "datasync" -> :datasync
          "sync" -> :sync
          "none" -> :none
          other -> raise ArgumentError, "invalid wal_sync_method for #{name}: #{inspect(other)}"
        end
    end
  end
end
