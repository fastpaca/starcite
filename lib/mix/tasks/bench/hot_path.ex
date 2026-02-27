defmodule Mix.Tasks.Bench.HotPath do
  import Bitwise
  require Logger

  alias Mix.Tasks.Bench.SuccessMetrics
  alias Starcite.Config.Size
  alias Starcite.ReadPath
  alias Starcite.WritePath

  @slo_read_p99_ms 50.0
  @slo_read_p999_ms 100.0
  @slo_write_p99_ms 150.0
  @slo_write_p999_ms 500.0

  @default_workload_profile :production_like

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
    wait_for_endpoint_http!(config.api_port, 5_000)

    session_seed_started_at_ms = System.monotonic_time(:millisecond)
    IO.puts("Hot-path Benchee phase: preparing #{config.session_count} sessions...")
    sessions = prepare_sessions(config.session_count)

    IO.puts(
      "Hot-path Benchee phase: prepared sessions in #{elapsed_ms(session_seed_started_at_ms)}ms"
    )

    session_count = tuple_size(sessions)
    producer_state = prepare_producer_state(config, sessions)
    write_counter = new_counter()
    read_counter = new_counter()
    rtt_counter = new_counter()
    producer_counter = new_counter()
    mixed_counter = new_counter()
    read_cursors = new_sequence_counters(session_count)
    operation_schedule = build_operation_schedule(config.operation_mix)
    latency_table = new_latency_table()
    write_timestamp_table = new_write_timestamps_table()
    read_catchup_table = new_read_catchup_table()
    read_observation_table = new_read_observations_table()

    event = %{
      type: "content",
      payload: %{text: payload_text(config.payload_bytes)},
      actor: "agent:benchee",
      source: "benchmark",
      metadata: %{bench: true, scenario: "hot_path_benchee"},
      refs: %{},
      idempotency_key: nil
    }

    seed_reads_started_at_ms = System.monotonic_time(:millisecond)
    IO.puts("Hot-path Benchee phase: seeding sessions for read benchmarks...")

    seed_read_events(
      sessions,
      event,
      config,
      producer_state,
      producer_counter,
      write_timestamp_table,
      read_observation_table,
      read_catchup_table
    )

    IO.puts("Hot-path Benchee phase: seeded reads in #{elapsed_ms(seed_reads_started_at_ms)}ms")

    write_benchmark =
      build_write_benchmark(
        config,
        sessions,
        session_count,
        event,
        producer_state,
        write_counter,
        producer_counter,
        write_timestamp_table,
        read_observation_table,
        read_catchup_table
      )

    read_benchmark =
      build_read_benchmark(
        config,
        sessions,
        session_count,
        read_counter,
        read_cursors,
        write_timestamp_table,
        read_catchup_table
      )

    rtt_benchmark =
      build_rtt_benchmark(
        config,
        sessions,
        session_count,
        event,
        producer_state,
        rtt_counter,
        producer_counter,
        write_timestamp_table,
        read_observation_table,
        read_catchup_table
      )

    try do
      run_mpmc_warmup(
        config,
        fn ->
          run_mixed_operation(
            write_benchmark,
            read_benchmark,
            rtt_benchmark,
            mixed_counter,
            operation_schedule,
            latency_table,
            false
          )
        end
      )

      :ets.delete_all_objects(write_timestamp_table)
      :ets.delete_all_objects(read_catchup_table)
      :ets.delete_all_objects(read_observation_table)

      tail_consumers =
        start_tail_consumers(
          config,
          sessions,
          write_timestamp_table,
          read_catchup_table,
          read_observation_table
        )

      try do
        verify_tail_streaming!(
          sessions,
          event,
          config,
          producer_state,
          producer_counter,
          write_timestamp_table,
          read_observation_table,
          read_catchup_table
        )

        benchee_started_at_ms = System.monotonic_time(:millisecond)
        IO.puts("Hot-path Benchee phase: starting Benchee run...")

        suite =
          run_benchee(
            %{
              "mpmc_mixed" => fn ->
                run_mixed_operation(
                  write_benchmark,
                  read_benchmark,
                  rtt_benchmark,
                  mixed_counter,
                  operation_schedule,
                  latency_table,
                  true
                )
              end
            },
            parallel: config.parallel,
            warmup: 0,
            time: config.time_seconds,
            memory_time: 0,
            percentiles: [50, 95, 99, 99.9],
            print: [fast_warning: false]
          )

        measured_seconds = measured_seconds!(suite, "mpmc_mixed")
        operation_latencies = collect_operation_latencies(latency_table)

        report =
          SuccessMetrics.build_from_samples!(
            operation_latencies,
            measured_seconds,
            %{
              read: %{p99_ms: config.slo_read_p99_ms, p999_ms: config.slo_read_p999_ms},
              write: %{p99_ms: config.slo_write_p99_ms, p999_ms: config.slo_write_p999_ms}
            }
          )

        :ok = SuccessMetrics.print(report)
        :ok = print_read_catchup_metrics(read_catchup_table)
        alive_tail_consumers = Enum.count(tail_consumers, &Process.alive?/1)

        IO.puts(
          "Hot-path tail consumers alive before shutdown: #{alive_tail_consumers}/#{length(tail_consumers)}"
        )

        :ok = SuccessMetrics.raise_on_failures!(report)

        IO.puts(
          "Hot-path Benchee phase: Benchee run completed in #{elapsed_ms(benchee_started_at_ms)}ms"
        )
      after
        stop_tail_consumers(tail_consumers)
      end
    after
      :ets.delete(latency_table)
      :ets.delete(write_timestamp_table)
      :ets.delete(read_catchup_table)
      :ets.delete(read_observation_table)
    end

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

    endpoint_config =
      :starcite
      |> Application.get_env(StarciteWeb.Endpoint, [])
      |> Keyword.put(:server, true)
      |> Keyword.put(:http, ip: {127, 0, 0, 1}, port: config.api_port)

    Application.put_env(:starcite, StarciteWeb.Endpoint, endpoint_config)

    Application.put_env(:starcite, StarciteWeb.Auth,
      mode: :none,
      jwt_leeway_seconds: 1,
      jwks_refresh_ms: :timer.seconds(60)
    )

    if max_size = System.get_env("BENCH_EVENT_STORE_MAX_SIZE") do
      Application.put_env(
        :starcite,
        :event_store_max_bytes,
        Size.parse_bytes!(max_size, "BENCH_EVENT_STORE_MAX_SIZE", examples: "256MB, 4G, 1024M")
      )
    end
  end

  defp benchmark_config do
    workload_profile = env_workload_profile("BENCH_WORKLOAD_PROFILE", @default_workload_profile)
    profile_defaults = workload_profile_defaults(workload_profile)
    parallel = env_integer("BENCH_PARALLEL", profile_defaults.parallel)
    warmup_seconds = env_integer("BENCH_WARMUP_SECONDS", 5)
    time_seconds = env_integer("BENCH_TIME_SECONDS", 30)
    ra_wal_write_strategy_default = Application.get_env(:ra, :wal_write_strategy, :default)
    ra_wal_sync_method_default = Application.get_env(:ra, :wal_sync_method, :datasync)

    archive_flush_interval_ms_default =
      Application.get_env(:starcite, :archive_flush_interval_ms, 5_000)

    config = %{
      workload_profile: workload_profile,
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      api_port: env_integer("BENCH_API_PORT", 4000),
      num_groups: env_integer("BENCH_NUM_GROUPS", 256),
      session_count: env_integer("BENCH_SESSION_COUNT", profile_defaults.session_count),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", profile_defaults.payload_bytes),
      batch_size: env_integer("BENCH_BATCH_SIZE", 1),
      producer_mode: env_producer_mode("BENCH_PRODUCER_MODE", :stable),
      producer_pool_size:
        env_integer(
          "BENCH_PRODUCER_POOL_SIZE",
          max(parallel, profile_defaults.producer_pool_size)
        ),
      operation_mix: profile_defaults.operation_mix,
      read_mode: env_read_mode("BENCH_READ_MODE", profile_defaults.read_mode),
      read_window: env_integer("BENCH_READ_WINDOW", profile_defaults.read_window),
      tail_frame_batch_size:
        env_integer("BENCH_TAIL_FRAME_BATCH_SIZE", profile_defaults.tail_frame_batch_size),
      tail_consumer_count:
        env_integer("BENCH_TAIL_CONSUMER_COUNT", profile_defaults.tail_consumer_count),
      tail_connect_timeout_ms: env_integer("BENCH_TAIL_CONNECT_TIMEOUT_MS", 5_000),
      tail_read_timeout_ms: env_integer("BENCH_TAIL_READ_TIMEOUT_MS", 250),
      parallel: parallel,
      warmup_seconds: warmup_seconds,
      time_seconds: time_seconds,
      slo_read_p99_ms: @slo_read_p99_ms,
      slo_read_p999_ms: @slo_read_p999_ms,
      slo_write_p99_ms: @slo_write_p99_ms,
      slo_write_p999_ms: @slo_write_p999_ms,
      ra_wal_write_strategy:
        env_wal_write_strategy("BENCH_RA_WAL_WRITE_STRATEGY", ra_wal_write_strategy_default),
      ra_wal_sync_method:
        env_wal_sync_method("BENCH_RA_WAL_SYNC_METHOD", ra_wal_sync_method_default),
      archive_flush_interval_ms:
        env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", archive_flush_interval_ms_default)
    }

    validate_benchmark_config!(config)
  end

  defp workload_profile_defaults(:balanced) do
    %{
      parallel: 3,
      session_count: 256,
      producer_pool_size: 3,
      payload_bytes: 256,
      operation_mix: %{write: 1, read: 1, rtt: 1},
      read_mode: :point,
      read_window: 1,
      tail_frame_batch_size: 1,
      tail_consumer_count: 8
    }
  end

  defp workload_profile_defaults(:production_like) do
    %{
      parallel: 64,
      session_count: 2048,
      producer_pool_size: 128,
      payload_bytes: 1024,
      operation_mix: %{write: 96, read: 2, rtt: 2},
      read_mode: :point,
      read_window: 64,
      tail_frame_batch_size: 128,
      tail_consumer_count: 64
    }
  end

  defp validate_benchmark_config!(
         %{
           producer_mode: :stable,
           producer_pool_size: producer_pool_size,
           parallel: parallel,
           operation_mix: operation_mix
         } = config
       )
       when is_integer(producer_pool_size) and producer_pool_size > 0 and is_integer(parallel) and
              parallel > 0 and is_map(operation_mix) do
    if producer_pool_size < parallel do
      raise ArgumentError,
            "BENCH_PRODUCER_POOL_SIZE (#{producer_pool_size}) must be >= BENCH_PARALLEL (#{parallel}) " <>
              "for stable producers; otherwise producer_seq_conflict dominates benchmark results"
    end

    validate_operation_mix!(operation_mix)
    config
  end

  defp validate_benchmark_config!(
         %{producer_mode: :unique, operation_mix: operation_mix} = config
       )
       when is_map(operation_mix) do
    validate_operation_mix!(operation_mix)
    config
  end

  defp validate_operation_mix!(%{write: write_weight, read: read_weight, rtt: rtt_weight})
       when is_integer(write_weight) and write_weight > 0 and is_integer(read_weight) and
              read_weight > 0 and is_integer(rtt_weight) and rtt_weight > 0 do
    :ok
  end

  defp validate_operation_mix!(operation_mix) do
    raise ArgumentError,
          "invalid operation_mix: #{inspect(operation_mix)} (expected positive integer write/read/rtt weights)"
  end

  defp format_operation_mix(%{write: write_weight, read: read_weight, rtt: rtt_weight})
       when is_integer(write_weight) and is_integer(read_weight) and is_integer(rtt_weight) do
    "write/read/rtt = #{write_weight}:#{read_weight}:#{rtt_weight}"
  end

  defp print_config(config) do
    archive_adapter =
      Application.get_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.S3)

    archive_adapter_opts = Application.get_env(:starcite, :archive_adapter_opts, [])

    IO.puts("Hot-path Benchee config:")
    IO.puts("  raft_data_dir: #{config.raft_data_dir}")
    IO.puts("  clean_raft_data_dir: #{config.clean_raft_data_dir}")
    IO.puts("  log_level: #{config.log_level}")
    IO.puts("  api_port: #{config.api_port}")
    IO.puts("  num_groups: #{config.num_groups}")
    IO.puts("  sessions: #{config.session_count}")
    IO.puts("  payload_bytes: #{config.payload_bytes}")
    IO.puts("  batch_size: #{config.batch_size}")
    IO.puts("  producer_mode: #{config.producer_mode}")
    IO.puts("  producer_pool_size: #{config.producer_pool_size}")
    IO.puts("  parallel: #{config.parallel}")
    IO.puts("  warmup_seconds: #{config.warmup_seconds}")
    IO.puts("  time_seconds: #{config.time_seconds}")
    IO.puts("  workload_mode: mpmc_mixed")
    IO.puts("  workload_profile: #{config.workload_profile}")
    IO.puts("  operation_mix: #{format_operation_mix(config.operation_mix)}")
    IO.puts("  read_mode: #{config.read_mode}")
    IO.puts("  read_window: #{config.read_window}")
    IO.puts("  tail_frame_batch_size: #{config.tail_frame_batch_size}")
    IO.puts("  tail_consumer_count: #{config.tail_consumer_count}")
    IO.puts("  slo_read_p99_ms: #{config.slo_read_p99_ms}")
    IO.puts("  slo_read_p999_ms: #{config.slo_read_p999_ms}")
    IO.puts("  slo_write_p99_ms: #{config.slo_write_p99_ms}")
    IO.puts("  slo_write_p999_ms: #{config.slo_write_p999_ms}")
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

    1..session_count
    |> Enum.map(fn index ->
      id = "hot-benchee-#{run_id}-#{index}"

      case WritePath.create_session(
             id: id,
             metadata: %{bench: true, scenario: "hot_path_benchee"}
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

  defp wait_for_endpoint_http!(port, timeout_ms)
       when is_integer(port) and port > 0 and is_integer(timeout_ms) and timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_endpoint_http(port, deadline_ms)
  end

  defp do_wait_for_endpoint_http(port, deadline_ms)
       when is_integer(port) and port > 0 and is_integer(deadline_ms) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 250) do
      {:ok, socket} ->
        request = [
          "GET /health/live HTTP/1.1\r\n",
          "Host: 127.0.0.1:#{port}\r\n",
          "Connection: close\r\n",
          "\r\n"
        ]

        :ok = :gen_tcp.send(socket, request)

        case recv_until_headers(socket, <<>>, 500) do
          {:ok, response_headers, _rest} ->
            :ok = :gen_tcp.close(socket)

            if String.starts_with?(response_headers, "HTTP/1.1") do
              :ok
            else
              retry_wait_for_endpoint_http(port, deadline_ms)
            end

          {:error, _reason} ->
            :ok = :gen_tcp.close(socket)
            retry_wait_for_endpoint_http(port, deadline_ms)
        end

      {:error, _reason} ->
        retry_wait_for_endpoint_http(port, deadline_ms)
    end
  end

  defp retry_wait_for_endpoint_http(port, deadline_ms)
       when is_integer(port) and is_integer(deadline_ms) do
    if System.monotonic_time(:millisecond) >= deadline_ms do
      Mix.raise("endpoint did not respond on http://127.0.0.1:#{port} within timeout")
    else
      Process.sleep(50)
      do_wait_for_endpoint_http(port, deadline_ms)
    end
  end

  defp new_counter do
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)
    counter
  end

  defp new_latency_table do
    :ets.new(:bench_hot_path_latencies, [
      :duplicate_bag,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp new_write_timestamps_table do
    :ets.new(:bench_hot_path_write_timestamps, [
      :set,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp new_read_catchup_table do
    :ets.new(:bench_hot_path_read_catchup_latencies, [
      :duplicate_bag,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp new_read_observations_table do
    :ets.new(:bench_hot_path_read_observations, [
      :set,
      :public,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])
  end

  defp new_sequence_counters(size) when is_integer(size) and size > 0 do
    counters = :atomics.new(size, [])

    1..size
    |> Enum.each(fn index -> :atomics.put(counters, index, 0) end)

    counters
  end

  defp seed_read_events(
         sessions,
         event,
         config,
         producer_state,
         producer_counter,
         write_timestamp_table,
         read_observation_table,
         read_catchup_table
       )
       when is_tuple(sessions) and is_map(event) and is_map(config) do
    sessions
    |> Tuple.to_list()
    |> Enum.with_index(1)
    |> Enum.each(fn {session_id, session_index} ->
      producer_index = :atomics.add_get(producer_counter, 1, 1)

      event_with_producer =
        with_bench_producer(event, session_index, producer_index, config, producer_state)

      %{seq: seq} = append_one(session_id, event_with_producer)

      record_write_accepted(
        write_timestamp_table,
        read_observation_table,
        read_catchup_table,
        session_index,
        seq
      )

      :ok
    end)
  end

  defp build_write_benchmark(
         config,
         sessions,
         session_count,
         event,
         producer_state,
         write_counter,
         producer_counter,
         write_timestamp_table,
         read_observation_table,
         read_catchup_table
       )
       when is_map(config) and is_tuple(sessions) and is_integer(session_count) and
              session_count > 0 and
              is_map(event) do
    case config.batch_size do
      1 ->
        fn ->
          index = :atomics.add_get(write_counter, 1, 1)
          session_index = rem(index - 1, session_count) + 1
          session_id = elem(sessions, session_index - 1)
          producer_index = :atomics.add_get(producer_counter, 1, 1)

          event_with_producer =
            with_bench_producer(event, session_index, producer_index, config, producer_state)

          %{seq: seq} = append_one(session_id, event_with_producer)

          record_write_accepted(
            write_timestamp_table,
            read_observation_table,
            read_catchup_table,
            session_index,
            seq
          )

          :ok
        end

      batch_size when is_integer(batch_size) and batch_size > 1 ->
        fn ->
          first_index = :atomics.add_get(write_counter, 1, 1)
          session_index = rem(first_index - 1, session_count) + 1
          session_id = elem(sessions, session_index - 1)

          events =
            for _offset <- 1..batch_size do
              producer_index = :atomics.add_get(producer_counter, 1, 1)
              with_bench_producer(event, session_index, producer_index, config, producer_state)
            end

          reply = append_batch(session_id, events)

          record_batch_write_accepted(
            write_timestamp_table,
            read_observation_table,
            read_catchup_table,
            session_index,
            reply
          )

          :ok
        end
    end
  end

  defp build_read_benchmark(
         %{read_mode: :point},
         sessions,
         session_count,
         read_counter,
         _read_cursors,
         _write_timestamp_table,
         _read_catchup_table
       )
       when is_tuple(sessions) and is_integer(session_count) and session_count > 0 do
    fn ->
      index = :atomics.add_get(read_counter, 1, 1)
      session_index = rem(index - 1, session_count) + 1
      session_id = elem(sessions, session_index - 1)
      read_from_start(session_id)
    end
  end

  defp build_read_benchmark(
         %{read_mode: :catchup, read_window: read_window},
         sessions,
         session_count,
         read_counter,
         read_cursors,
         write_timestamp_table,
         read_catchup_table
       )
       when is_tuple(sessions) and is_integer(session_count) and session_count > 0 and
              is_integer(read_window) and read_window > 0 do
    fn ->
      index = :atomics.add_get(read_counter, 1, 1)
      session_index = rem(index - 1, session_count) + 1
      session_id = elem(sessions, session_index - 1)

      read_catchup(
        session_id,
        session_index,
        read_cursors,
        read_window,
        write_timestamp_table,
        read_catchup_table
      )
    end
  end

  defp build_rtt_benchmark(
         config,
         sessions,
         session_count,
         event,
         producer_state,
         rtt_counter,
         producer_counter,
         write_timestamp_table,
         read_observation_table,
         read_catchup_table
       )
       when is_map(config) and is_tuple(sessions) and is_integer(session_count) and
              session_count > 0 and
              is_map(event) do
    fn ->
      index = :atomics.add_get(rtt_counter, 1, 1)
      session_index = rem(index - 1, session_count) + 1
      session_id = elem(sessions, session_index - 1)
      producer_index = :atomics.add_get(producer_counter, 1, 1)

      event_with_producer =
        with_bench_producer(event, session_index, producer_index, config, producer_state)

      %{seq: seq} = append_one(session_id, event_with_producer)

      record_write_accepted(
        write_timestamp_table,
        read_observation_table,
        read_catchup_table,
        session_index,
        seq
      )

      verify_read_from_cursor(session_id, seq - 1, seq)
    end
  end

  defp run_mpmc_warmup(%{warmup_seconds: warmup_seconds}, _operation)
       when warmup_seconds <= 0 do
    :ok
  end

  defp run_mpmc_warmup(
         %{parallel: parallel, warmup_seconds: warmup_seconds},
         operation
       )
       when is_integer(parallel) and parallel > 0 and
              is_integer(warmup_seconds) and warmup_seconds > 0 and is_function(operation, 0) do
    IO.puts("Hot-path Benchee phase: warming mixed workload for #{warmup_seconds}s...")
    deadline_ms = System.monotonic_time(:millisecond) + warmup_seconds * 1000

    1..parallel
    |> Enum.map(fn _ -> Task.async(fn -> warmup_until(deadline_ms, operation) end) end)
    |> Enum.each(&Task.await(&1, :infinity))

    :ok
  end

  defp warmup_until(deadline_ms, operation)
       when is_integer(deadline_ms) and is_function(operation, 0) do
    if System.monotonic_time(:millisecond) < deadline_ms do
      operation.()
      warmup_until(deadline_ms, operation)
    else
      :ok
    end
  end

  defp run_mixed_operation(
         write_benchmark,
         read_benchmark,
         rtt_benchmark,
         mixed_counter,
         operation_schedule,
         latency_table,
         record_latency?
       )
       when is_function(write_benchmark, 0) and is_function(read_benchmark, 0) and
              is_function(rtt_benchmark, 0) and is_tuple(operation_schedule) and
              is_boolean(record_latency?) do
    operation = next_mixed_operation(mixed_counter, operation_schedule)
    started_at_ns = System.monotonic_time(:nanosecond)

    case operation do
      :write -> write_benchmark.()
      :read -> read_benchmark.()
      :rtt -> rtt_benchmark.()
    end

    if record_latency? do
      latency_ns = System.monotonic_time(:nanosecond) - started_at_ns
      true = :ets.insert(latency_table, {operation, latency_ns})
    end

    :ok
  end

  defp next_mixed_operation(mixed_counter, operation_schedule)
       when is_tuple(operation_schedule) do
    schedule_size = tuple_size(operation_schedule)
    schedule_index = rem(:atomics.add_get(mixed_counter, 1, 1) - 1, schedule_size) + 1
    elem(operation_schedule, schedule_index - 1)
  end

  defp build_operation_schedule(%{write: write_weight, read: read_weight, rtt: rtt_weight})
       when is_integer(write_weight) and write_weight > 0 and
              is_integer(read_weight) and read_weight > 0 and
              is_integer(rtt_weight) and rtt_weight > 0 do
    (List.duplicate(:write, write_weight) ++
       List.duplicate(:read, read_weight) ++
       List.duplicate(:rtt, rtt_weight))
    |> List.to_tuple()
  end

  defp collect_operation_latencies(latency_table) do
    %{
      write: lookup_operation_latencies(latency_table, :write),
      read: lookup_operation_latencies(latency_table, :read),
      rtt: lookup_operation_latencies(latency_table, :rtt)
    }
  end

  defp verify_tail_streaming!(
         sessions,
         event,
         config,
         producer_state,
         producer_counter,
         write_timestamp_table,
         read_observation_table,
         read_catchup_table
       )
       when is_tuple(sessions) and tuple_size(sessions) > 0 and is_map(event) and is_map(config) do
    session_id = elem(sessions, 0)
    session_index = 1
    producer_index = :atomics.add_get(producer_counter, 1, 1)

    event_with_producer =
      with_bench_producer(event, session_index, producer_index, config, producer_state)

    %{seq: seq} = append_one(session_id, event_with_producer)

    record_write_accepted(
      write_timestamp_table,
      read_observation_table,
      read_catchup_table,
      session_index,
      seq
    )

    :ok = wait_for_tail_sample!(read_catchup_table, 2_500)
    :ets.delete_all_objects(write_timestamp_table)
    :ets.delete_all_objects(read_observation_table)
    :ets.delete_all_objects(read_catchup_table)
    :ok
  end

  defp wait_for_tail_sample!(read_catchup_table, timeout_ms)
       when is_integer(timeout_ms) and timeout_ms > 0 do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_tail_sample(read_catchup_table, deadline)
  end

  defp do_wait_for_tail_sample(read_catchup_table, deadline_ms) when is_integer(deadline_ms) do
    case :ets.lookup(read_catchup_table, :read_catchup) do
      [] ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          Mix.raise(
            "tail catch-up preflight did not observe any websocket catch-up samples within timeout"
          )
        else
          Process.sleep(25)
          do_wait_for_tail_sample(read_catchup_table, deadline_ms)
        end

      _samples ->
        :ok
    end
  end

  defp start_tail_consumers(
         %{
           tail_consumer_count: requested_count,
           tail_frame_batch_size: tail_frame_batch_size,
           tail_connect_timeout_ms: tail_connect_timeout_ms,
           tail_read_timeout_ms: tail_read_timeout_ms,
           api_port: api_port
         },
         sessions,
         write_timestamp_table,
         read_catchup_table,
         read_observation_table
       )
       when is_tuple(sessions) and is_integer(requested_count) and requested_count > 0 and
              is_integer(tail_frame_batch_size) and tail_frame_batch_size > 0 and
              is_integer(tail_connect_timeout_ms) and tail_connect_timeout_ms > 0 and
              is_integer(tail_read_timeout_ms) and tail_read_timeout_ms > 0 and
              is_integer(api_port) and api_port > 0 do
    session_count = tuple_size(sessions)
    consumer_count = min(requested_count, session_count)

    tail_socket_opts = %{
      api_port: api_port,
      tail_frame_batch_size: tail_frame_batch_size,
      tail_connect_timeout_ms: tail_connect_timeout_ms,
      tail_read_timeout_ms: tail_read_timeout_ms
    }

    session_entries =
      sessions
      |> Tuple.to_list()
      |> Enum.with_index(1)
      |> Enum.take(consumer_count)

    IO.puts("Hot-path Benchee phase: starting #{consumer_count} tail catch-up consumers...")
    {first_session_id, _first_session_index} = hd(session_entries)

    wait_for_tail_upgrade!(
      first_session_id,
      tail_socket_opts,
      tail_connect_timeout_ms * 4
    )

    parent_pid = self()

    consumer_pids =
      Enum.map(session_entries, fn {session_id, session_index} ->
        {:ok, pid} =
          Task.start(fn ->
            tail_consumer_loop(
              session_id,
              session_index,
              write_timestamp_table,
              read_catchup_table,
              read_observation_table,
              tail_socket_opts,
              parent_pid
            )
          end)

        pid
      end)

    await_tail_consumer_connections(consumer_count, tail_connect_timeout_ms * 2)
    consumer_pids
  end

  defp wait_for_tail_upgrade!(session_id, tail_socket_opts, timeout_ms)
       when is_binary(session_id) and is_map(tail_socket_opts) and is_integer(timeout_ms) and
              timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_tail_upgrade(session_id, tail_socket_opts, deadline_ms, nil)
  end

  defp do_wait_for_tail_upgrade(session_id, tail_socket_opts, deadline_ms, last_reason)
       when is_binary(session_id) and is_integer(deadline_ms) do
    case connect_tail_socket(session_id, tail_socket_opts) do
      {:ok, socket, _buffer} ->
        :ok = :gen_tcp.close(socket)
        :ok

      {:error, reason} ->
        if System.monotonic_time(:millisecond) >= deadline_ms do
          Mix.raise(
            "tail websocket upgrade did not become ready in time for session=#{session_id}: #{inspect(reason || last_reason)}"
          )
        else
          Process.sleep(100)
          do_wait_for_tail_upgrade(session_id, tail_socket_opts, deadline_ms, reason)
        end
    end
  end

  defp stop_tail_consumers(consumer_pids) when is_list(consumer_pids) do
    Enum.each(consumer_pids, &send(&1, :stop))
    Process.sleep(300)

    Enum.each(consumer_pids, fn pid ->
      if Process.alive?(pid) do
        Process.exit(pid, :kill)
      end
    end)

    :ok
  end

  defp tail_consumer_loop(
         session_id,
         session_index,
         write_timestamp_table,
         read_catchup_table,
         read_observation_table,
         tail_socket_opts,
         parent_pid
       )
       when is_binary(session_id) and is_integer(session_index) and session_index > 0 do
    tail_consumer_loop_connected(
      session_id,
      session_index,
      write_timestamp_table,
      read_catchup_table,
      read_observation_table,
      tail_socket_opts,
      parent_pid,
      false
    )
  end

  defp tail_consumer_loop_connected(
         session_id,
         session_index,
         write_timestamp_table,
         read_catchup_table,
         read_observation_table,
         tail_socket_opts,
         parent_pid,
         connected_once?
       ) do
    if receive_stop_signal?() do
      :ok
    else
      case connect_tail_socket(session_id, tail_socket_opts) do
        {:ok, socket, buffer} ->
          if not connected_once? do
            send(parent_pid, {:bench_tail_consumer_connected, self(), session_index})
          end

          case consume_tail_frames(
                 socket,
                 buffer,
                 session_index,
                 write_timestamp_table,
                 read_catchup_table,
                 read_observation_table,
                 tail_socket_opts.tail_read_timeout_ms
               ) do
            :stop ->
              :ok

            :disconnected ->
              Process.sleep(25)

              tail_consumer_loop_connected(
                session_id,
                session_index,
                write_timestamp_table,
                read_catchup_table,
                read_observation_table,
                tail_socket_opts,
                parent_pid,
                true
              )
          end

        {:error, reason} ->
          Logger.warning(
            "tail consumer failed to connect for session=#{session_id}: #{inspect(reason)}"
          )

          Process.sleep(100)

          tail_consumer_loop_connected(
            session_id,
            session_index,
            write_timestamp_table,
            read_catchup_table,
            read_observation_table,
            tail_socket_opts,
            parent_pid,
            connected_once?
          )
      end
    end
  end

  defp await_tail_consumer_connections(expected_count, timeout_ms)
       when is_integer(expected_count) and expected_count >= 0 and is_integer(timeout_ms) and
              timeout_ms > 0 do
    deadline_ms = System.monotonic_time(:millisecond) + timeout_ms
    do_await_tail_consumer_connections(expected_count, %{}, deadline_ms)
  end

  defp do_await_tail_consumer_connections(expected_count, connected_pids, deadline_ms)
       when is_integer(expected_count) and is_map(connected_pids) and
              is_integer(deadline_ms) do
    connected_count = map_size(connected_pids)

    if connected_count >= expected_count do
      :ok
    else
      remaining_ms = max(deadline_ms - System.monotonic_time(:millisecond), 0)

      if remaining_ms == 0 do
        Mix.raise(
          "tail consumers did not connect in time: connected=#{connected_count}/#{expected_count}"
        )
      else
        receive do
          {:bench_tail_consumer_connected, pid, _session_index} when is_pid(pid) ->
            do_await_tail_consumer_connections(
              expected_count,
              Map.put(connected_pids, pid, true),
              deadline_ms
            )
        after
          remaining_ms ->
            Mix.raise(
              "tail consumers did not connect in time: connected=#{connected_count}/#{expected_count}"
            )
        end
      end
    end
  end

  defp connect_tail_socket(
         session_id,
         %{
           api_port: port,
           tail_frame_batch_size: tail_frame_batch_size,
           tail_connect_timeout_ms: connect_timeout_ms
         }
       )
       when is_binary(session_id) and is_integer(port) and port > 0 and
              is_integer(tail_frame_batch_size) and tail_frame_batch_size > 0 and
              is_integer(connect_timeout_ms) and connect_timeout_ms > 0 do
    last_seq = current_last_seq(session_id)

    case :gen_tcp.connect(
           ~c"127.0.0.1",
           port,
           [:binary, active: false, packet: :raw],
           connect_timeout_ms
         ) do
      {:ok, socket} ->
        query =
          URI.encode_query(%{
            "cursor" => Integer.to_string(last_seq),
            "batch_size" => Integer.to_string(tail_frame_batch_size)
          })

        request = [
          "GET /v1/sessions/#{session_id}/tail?#{query} HTTP/1.1\r\n",
          "Host: 127.0.0.1:#{port}\r\n",
          "Connection: Upgrade\r\n",
          "Upgrade: websocket\r\n",
          "Sec-WebSocket-Version: 13\r\n",
          "Sec-WebSocket-Key: #{Base.encode64(:crypto.strong_rand_bytes(16))}\r\n",
          "\r\n"
        ]

        :ok = :gen_tcp.send(socket, request)

        case recv_until_headers(socket, <<>>, connect_timeout_ms) do
          {:ok, response_headers, buffer} ->
            if String.starts_with?(response_headers, "HTTP/1.1 101") do
              {:ok, socket, buffer}
            else
              :ok = :gen_tcp.close(socket)
              {:error, {:websocket_upgrade_failed, response_headers, buffer}}
            end

          {:error, reason} ->
            :ok = :gen_tcp.close(socket)
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recv_until_headers(socket, buffer, timeout_ms)
       when is_binary(buffer) and is_integer(timeout_ms) and timeout_ms > 0 do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, _len} ->
        header_bytes = binary_part(buffer, 0, index + 4)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, header_bytes, rest}

      :nomatch ->
        case :gen_tcp.recv(socket, 0, timeout_ms) do
          {:ok, bytes} -> recv_until_headers(socket, buffer <> bytes, timeout_ms)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp consume_tail_frames(
         socket,
         buffer,
         session_index,
         write_timestamp_table,
         read_catchup_table,
         read_observation_table,
         read_timeout_ms
       )
       when is_integer(session_index) and session_index > 0 and is_integer(read_timeout_ms) and
              read_timeout_ms > 0 do
    case receive_stop_signal?() do
      true ->
        :ok = :gen_tcp.close(socket)
        :stop

      false ->
        case parse_ws_frame(buffer) do
          {:ok, %{opcode: 0x1, payload: payload}, rest} ->
            seqs = decode_tail_payload_seqs(payload)

            record_tail_frame_lags(
              write_timestamp_table,
              read_catchup_table,
              read_observation_table,
              session_index,
              seqs
            )

            consume_tail_frames(
              socket,
              rest,
              session_index,
              write_timestamp_table,
              read_catchup_table,
              read_observation_table,
              read_timeout_ms
            )

          {:ok, %{opcode: 0x9, payload: payload}, rest} ->
            :ok = send_ws_pong(socket, payload)

            consume_tail_frames(
              socket,
              rest,
              session_index,
              write_timestamp_table,
              read_catchup_table,
              read_observation_table,
              read_timeout_ms
            )

          {:ok, %{opcode: 0x8}, _rest} ->
            :ok = :gen_tcp.close(socket)
            :disconnected

          {:ok, _frame, rest} ->
            consume_tail_frames(
              socket,
              rest,
              session_index,
              write_timestamp_table,
              read_catchup_table,
              read_observation_table,
              read_timeout_ms
            )

          :more ->
            case :gen_tcp.recv(socket, 0, read_timeout_ms) do
              {:ok, bytes} ->
                consume_tail_frames(
                  socket,
                  buffer <> bytes,
                  session_index,
                  write_timestamp_table,
                  read_catchup_table,
                  read_observation_table,
                  read_timeout_ms
                )

              {:error, :timeout} ->
                consume_tail_frames(
                  socket,
                  buffer,
                  session_index,
                  write_timestamp_table,
                  read_catchup_table,
                  read_observation_table,
                  read_timeout_ms
                )

              {:error, _reason} ->
                :ok = :gen_tcp.close(socket)
                :disconnected
            end
        end
    end
  end

  defp receive_stop_signal? do
    receive do
      :stop -> true
    after
      0 -> false
    end
  end

  defp parse_ws_frame(<<b1, b2, rest::binary>>) do
    opcode = b1 &&& 0x0F
    masked? = (b2 &&& 0x80) == 0x80
    payload_len_code = b2 &&& 0x7F

    with {:ok, payload_length, rest_after_length} <-
           parse_ws_payload_length(payload_len_code, rest),
         {:ok, masking_key, payload_and_rest} <- parse_ws_masking_key(masked?, rest_after_length),
         true <- byte_size(payload_and_rest) >= payload_length do
      <<payload::binary-size(payload_length), tail::binary>> = payload_and_rest
      decoded_payload = decode_ws_payload(payload, masking_key, masked?)
      {:ok, %{opcode: opcode, payload: decoded_payload}, tail}
    else
      :more -> :more
      false -> :more
    end
  end

  defp parse_ws_frame(_buffer), do: :more

  defp parse_ws_payload_length(payload_len_code, rest)
       when is_integer(payload_len_code) and payload_len_code >= 0 and payload_len_code < 126 and
              is_binary(rest) do
    {:ok, payload_len_code, rest}
  end

  defp parse_ws_payload_length(126, <<payload_length::16, rest::binary>>) do
    {:ok, payload_length, rest}
  end

  defp parse_ws_payload_length(126, _rest), do: :more

  defp parse_ws_payload_length(127, <<payload_length::64, rest::binary>>) do
    {:ok, payload_length, rest}
  end

  defp parse_ws_payload_length(127, _rest), do: :more

  defp parse_ws_masking_key(true, <<masking_key::binary-size(4), rest::binary>>) do
    {:ok, masking_key, rest}
  end

  defp parse_ws_masking_key(true, _rest), do: :more
  defp parse_ws_masking_key(false, rest) when is_binary(rest), do: {:ok, <<>>, rest}

  defp decode_ws_payload(payload, _masking_key, false) when is_binary(payload), do: payload

  defp decode_ws_payload(payload, masking_key, true)
       when is_binary(payload) and byte_size(masking_key) == 4 do
    <<k1, k2, k3, k4>> = masking_key
    keys = {k1, k2, k3, k4}

    {decoded_payload, _index} =
      for <<byte <- payload>>, reduce: {<<>>, 0} do
        {acc, index} ->
          key = elem(keys, rem(index, 4))
          {<<acc::binary, bxor(byte, key)>>, index + 1}
      end

    decoded_payload
  end

  defp send_ws_pong(socket, payload) when is_binary(payload) do
    :ok = :gen_tcp.send(socket, encode_client_ws_frame(0xA, payload))
  end

  defp encode_client_ws_frame(opcode, payload)
       when is_integer(opcode) and opcode >= 0 and opcode <= 0xF and is_binary(payload) do
    fin_opcode = 0x80 ||| opcode
    length = byte_size(payload)
    mask = :crypto.strong_rand_bytes(4)
    masked_payload = apply_ws_mask(payload, mask)

    case length do
      len when len < 126 ->
        <<fin_opcode, 0x80 ||| len, mask::binary-size(4), masked_payload::binary>>

      len when len <= 65_535 ->
        <<fin_opcode, 0x80 ||| 126, len::16, mask::binary-size(4), masked_payload::binary>>

      len ->
        <<fin_opcode, 0x80 ||| 127, len::64, mask::binary-size(4), masked_payload::binary>>
    end
  end

  defp apply_ws_mask(payload, masking_key)
       when is_binary(payload) and byte_size(masking_key) == 4 do
    <<k1, k2, k3, k4>> = masking_key
    keys = {k1, k2, k3, k4}

    {masked_payload, _index} =
      for <<byte <- payload>>, reduce: {<<>>, 0} do
        {acc, index} ->
          key = elem(keys, rem(index, 4))
          {<<acc::binary, bxor(byte, key)>>, index + 1}
      end

    masked_payload
  end

  defp decode_tail_payload_seqs(payload) when is_binary(payload) do
    case Jason.decode(payload) do
      {:ok, [%{"seq" => _seq} | _rest] = events} ->
        events
        |> Enum.map(&Map.get(&1, "seq"))
        |> Enum.filter(&(is_integer(&1) and &1 > 0))

      {:ok, %{"seq" => seq}} when is_integer(seq) and seq > 0 ->
        [seq]

      _ ->
        []
    end
  end

  defp record_tail_frame_lags(
         write_timestamp_table,
         read_catchup_table,
         read_observation_table,
         session_index,
         seqs
       )
       when is_integer(session_index) and session_index > 0 and is_list(seqs) do
    Enum.each(seqs, fn seq ->
      record_tail_observed_lag(
        write_timestamp_table,
        read_catchup_table,
        read_observation_table,
        session_index,
        seq
      )
    end)

    case seqs do
      [] -> :ok
      _ -> prune_write_timestamps(write_timestamp_table, session_index, List.last(seqs))
    end
  end

  defp record_tail_observed_lag(
         write_timestamp_table,
         read_catchup_table,
         read_observation_table,
         session_index,
         seq
       )
       when is_integer(session_index) and session_index > 0 and is_integer(seq) and seq > 0 do
    observed_at_ns = System.monotonic_time(:nanosecond)

    case :ets.take(write_timestamp_table, {session_index, seq}) do
      [{{^session_index, ^seq}, accepted_at_ns}] ->
        lag_ns = max(observed_at_ns - accepted_at_ns, 0)
        true = :ets.insert(read_catchup_table, {:read_catchup, lag_ns})
        :ok

      [] ->
        _ = :ets.insert_new(read_observation_table, {{session_index, seq}, observed_at_ns})
        :ok
    end
  end

  defp current_last_seq(session_id) when is_binary(session_id) do
    case ReadPath.get_session(session_id) do
      {:ok, %{last_seq: last_seq}} when is_integer(last_seq) and last_seq >= 0 -> last_seq
      _ -> 0
    end
  end

  defp print_read_catchup_metrics(read_catchup_table) do
    samples =
      read_catchup_table
      |> :ets.lookup(:read_catchup)
      |> Enum.map(fn {:read_catchup, latency_ns} -> latency_ns end)
      |> Enum.sort()

    case samples do
      [] ->
        IO.puts("Hot-path read catch-up lag (write-ack -> observed): no samples")
        :ok

      _ ->
        avg_ms = samples |> Enum.sum() |> Kernel./(length(samples)) |> nanos_to_ms()
        min_ms = samples |> hd() |> nanos_to_ms()
        p50_ms = percentile_ms(samples, 50.0)
        p95_ms = percentile_ms(samples, 95.0)
        p99_ms = percentile_ms(samples, 99.0)
        p999_ms = percentile_ms(samples, 99.9)
        max_ms = samples |> List.last() |> nanos_to_ms()

        IO.puts(
          "Hot-path read catch-up lag (write-ack -> observed, ms): " <>
            "avg=#{format_ms(avg_ms)} " <>
            "min=#{format_ms(min_ms)} " <>
            "p50=#{format_ms(p50_ms)} " <>
            "p95=#{format_ms(p95_ms)} " <>
            "p99=#{format_ms(p99_ms)} " <>
            "p999=#{format_ms(p999_ms)} " <>
            "max=#{format_ms(max_ms)}"
        )

        :ok
    end
  end

  defp lookup_operation_latencies(latency_table, operation)
       when operation in [:write, :read, :rtt] do
    latency_table
    |> :ets.lookup(operation)
    |> Enum.map(fn {^operation, latency_ns} -> latency_ns end)
  end

  defp measured_seconds!(suite, scenario_name)
       when is_map(suite) and is_binary(scenario_name) do
    scenario = suite_scenario!(suite, scenario_name)
    run_time_stats = run_time_statistics!(scenario, scenario_name)
    sample_size = read_positive_number!(run_time_stats, :sample_size, "run_time_statistics")
    ips = read_positive_number!(run_time_stats, :ips, "run_time_statistics")
    sample_size / ips
  end

  defp suite_scenario!(suite, scenario_name)
       when is_map(suite) and is_binary(scenario_name) do
    case Map.fetch(suite, :scenarios) do
      {:ok, scenarios} when is_map(scenarios) ->
        fetch_map!(scenarios, scenario_name, "suite.scenarios")

      {:ok, scenarios} when is_list(scenarios) ->
        case Enum.find(scenarios, &scenario_name_match?(&1, scenario_name)) do
          nil ->
            raise ArgumentError, "missing suite.scenarios entry for #{inspect(scenario_name)}"

          scenario ->
            scenario
        end

      {:ok, other} ->
        raise ArgumentError,
              "invalid suite.scenarios: #{inspect(other)} (expected map/list)"

      :error ->
        raise ArgumentError, "missing suite.scenarios"
    end
  end

  defp run_time_statistics!(scenario, scenario_name)
       when is_map(scenario) and is_binary(scenario_name) do
    case Map.fetch(scenario, :run_time_statistics) do
      {:ok, stats} when is_map(stats) ->
        stats

      {:ok, stats} when is_struct(stats) ->
        Map.from_struct(stats)

      {:ok, stats} ->
        raise ArgumentError,
              "invalid scenario #{inspect(scenario_name)}.run_time_statistics: #{inspect(stats)} " <>
                "(expected map/struct)"

      :error ->
        run_time_data = fetch_map!(scenario, :run_time_data, "scenario #{inspect(scenario_name)}")
        statistics = fetch_map!(run_time_data, :statistics, "scenario #{inspect(scenario_name)}")
        if is_struct(statistics), do: Map.from_struct(statistics), else: statistics
    end
  end

  defp scenario_name_match?(scenario, scenario_name)
       when is_map(scenario) and is_binary(scenario_name) do
    name = Map.get(scenario, :name)
    job_name = Map.get(scenario, :job_name)
    display_name = Map.get(scenario, :display_name)
    name == scenario_name or job_name == scenario_name or display_name == scenario_name
  end

  defp read_positive_number!(map, key, context)
       when is_map(map) and is_atom(key) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_number(value) and value > 0 ->
        value * 1.0

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected positive number)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp fetch_map!(map, key, context) when is_map(map) and is_binary(context) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) ->
        value

      {:ok, value} ->
        raise ArgumentError,
              "invalid #{context}.#{inspect(key)}: #{inspect(value)} (expected map)"

      :error ->
        raise ArgumentError, "missing #{context}.#{inspect(key)}"
    end
  end

  defp read_from_start(session_id) when is_binary(session_id) do
    case read_from_cursor(session_id, 0, 1) do
      {:ok, [%{seq: seq}]} when is_integer(seq) and seq > 0 ->
        :ok

      {:ok, []} ->
        raise "read benchmark empty result for session=#{session_id} cursor=0 limit=1"

      {:ok, events} ->
        raise "read benchmark unexpected result for session=#{session_id}: #{inspect(events)}"

      {:error, reason} ->
        raise "read benchmark failed for session=#{session_id}: #{inspect(reason)}"
    end
  end

  defp read_catchup(
         session_id,
         session_index,
         read_cursors,
         read_window,
         write_timestamp_table,
         read_catchup_table
       )
       when is_binary(session_id) and is_integer(session_index) and session_index > 0 and
              is_integer(read_window) and read_window > 0 do
    cursor = :atomics.get(read_cursors, session_index)

    case read_from_cursor(session_id, cursor, read_window) do
      {:ok, []} ->
        :ok

      {:ok, events} ->
        last_seq = extract_last_seq!(events, session_id)
        :atomics.put(read_cursors, session_index, last_seq)

        record_read_catchup_lag(
          write_timestamp_table,
          read_catchup_table,
          session_index,
          last_seq
        )

        prune_write_timestamps(write_timestamp_table, session_index, last_seq)
        :ok

      {:error, reason} ->
        raise "read catchup failed for session=#{session_id} cursor=#{cursor}: #{inspect(reason)}"
    end
  end

  defp read_from_cursor(session_id, cursor, limit)
       when is_binary(session_id) and is_integer(cursor) and cursor >= 0 and is_integer(limit) and
              limit > 0 do
    ReadPath.get_events_from_cursor(session_id, cursor, limit)
  end

  defp extract_last_seq!(events, session_id)
       when is_list(events) and is_binary(session_id) do
    case List.last(events) do
      %{seq: seq} when is_integer(seq) and seq > 0 ->
        seq

      event ->
        raise "read catchup invalid event shape for session=#{session_id}: #{inspect(event)}"
    end
  end

  defp record_write_accepted(
         write_timestamp_table,
         read_observation_table,
         read_catchup_table,
         session_index,
         seq
       )
       when is_integer(session_index) and session_index > 0 and is_integer(seq) and seq > 0 do
    accepted_at_ns = System.monotonic_time(:nanosecond)
    true = :ets.insert(write_timestamp_table, {{session_index, seq}, accepted_at_ns})

    record_pending_tail_observation(
      write_timestamp_table,
      read_observation_table,
      read_catchup_table,
      session_index,
      seq,
      accepted_at_ns
    )

    :ok
  end

  defp record_batch_write_accepted(
         write_timestamp_table,
         read_observation_table,
         read_catchup_table,
         session_index,
         %{results: results}
       )
       when is_integer(session_index) and session_index > 0 and is_list(results) do
    accepted_at_ns = System.monotonic_time(:nanosecond)

    results
    |> Enum.map(fn
      %{seq: seq} when is_integer(seq) and seq > 0 -> {{session_index, seq}, accepted_at_ns}
      _ -> nil
    end)
    |> Enum.reject(&is_nil/1)
    |> then(fn entries ->
      if entries == [] do
        :ok
      else
        true = :ets.insert(write_timestamp_table, entries)

        Enum.each(entries, fn {{^session_index, seq}, ^accepted_at_ns} ->
          record_pending_tail_observation(
            write_timestamp_table,
            read_observation_table,
            read_catchup_table,
            session_index,
            seq,
            accepted_at_ns
          )
        end)

        :ok
      end
    end)
  end

  defp record_batch_write_accepted(
         write_timestamp_table,
         read_observation_table,
         read_catchup_table,
         session_index,
         %{last_seq: seq}
       )
       when is_integer(session_index) and session_index > 0 and is_integer(seq) and seq > 0 do
    record_write_accepted(
      write_timestamp_table,
      read_observation_table,
      read_catchup_table,
      session_index,
      seq
    )
  end

  defp record_batch_write_accepted(
         _write_timestamp_table,
         _read_observation_table,
         _read_catchup_table,
         _session_index,
         _reply
       ),
       do: :ok

  defp record_pending_tail_observation(
         write_timestamp_table,
         read_observation_table,
         read_catchup_table,
         session_index,
         seq,
         accepted_at_ns
       )
       when is_integer(session_index) and session_index > 0 and is_integer(seq) and seq > 0 and
              is_integer(accepted_at_ns) do
    case :ets.take(read_observation_table, {session_index, seq}) do
      [{{^session_index, ^seq}, observed_at_ns}] ->
        lag_ns = max(observed_at_ns - accepted_at_ns, 0)
        true = :ets.delete(write_timestamp_table, {session_index, seq})
        true = :ets.insert(read_catchup_table, {:read_catchup, lag_ns})
        :ok

      [] ->
        :ok
    end
  end

  defp record_read_catchup_lag(write_timestamp_table, read_catchup_table, session_index, seq)
       when is_integer(session_index) and session_index > 0 and is_integer(seq) and seq > 0 do
    case :ets.lookup(write_timestamp_table, {session_index, seq}) do
      [{{^session_index, ^seq}, accepted_at_ns}] ->
        lag_ns = max(System.monotonic_time(:nanosecond) - accepted_at_ns, 0)
        true = :ets.insert(read_catchup_table, {:read_catchup, lag_ns})
        :ok

      [] ->
        :ok
    end
  end

  defp prune_write_timestamps(write_timestamp_table, session_index, upto_seq)
       when is_integer(session_index) and session_index > 0 and is_integer(upto_seq) and
              upto_seq > 0 do
    :ets.select_delete(write_timestamp_table, [
      {{{session_index, :"$1"}, :_}, [{:"=<", :"$1", upto_seq}], [true]}
    ])

    :ok
  end

  defp verify_read_from_cursor(session_id, cursor, expected_seq)
       when is_binary(session_id) and is_integer(cursor) and is_integer(expected_seq) do
    case ReadPath.get_events_from_cursor(session_id, cursor, 1) do
      {:ok, [%{seq: ^expected_seq}]} ->
        :ok

      {:ok, [%{seq: read_seq}]} ->
        raise "rtt benchmark seq mismatch for session=#{session_id}: wrote=#{expected_seq} read=#{read_seq}"

      {:ok, []} ->
        raise "rtt benchmark read miss for session=#{session_id} cursor=#{cursor}"

      {:ok, events} ->
        raise "rtt benchmark unexpected read result for session=#{session_id}: #{inspect(events)}"

      {:error, reason} ->
        raise "rtt benchmark read failed for session=#{session_id}: #{inspect(reason)}"
    end
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
      {:ok, reply} when is_map(reply) -> reply
      {:error, reason} -> raise "append failed: #{inspect(reason)}"
      {:timeout, leader} -> raise "append timeout: #{inspect(leader)}"
    end
  end

  defp append_batch(session_id, [event]) when is_binary(session_id) and is_map(event) do
    append_one(session_id, event)
  end

  defp append_batch(session_id, events) when is_binary(session_id) and is_list(events) do
    case WritePath.append_events(session_id, events) do
      {:ok, reply} when is_map(reply) -> reply
      {:error, reason} -> raise "append batch failed: #{inspect(reason)}"
      {:timeout, leader} -> raise "append batch timeout: #{inspect(leader)}"
    end
  end

  defp payload_text(payload_bytes) when is_integer(payload_bytes) and payload_bytes > 0 do
    String.duplicate("x", payload_bytes)
  end

  defp percentile_ms(sorted_ns_samples, percentile)
       when is_list(sorted_ns_samples) and is_number(percentile) do
    sample_size = length(sorted_ns_samples)

    index =
      percentile
      |> Kernel./(100.0)
      |> Kernel.*(sample_size)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> min(sample_size)
      |> Kernel.-(1)

    sorted_ns_samples
    |> Enum.at(index)
    |> nanos_to_ms()
  end

  defp nanos_to_ms(value) when is_number(value), do: value / 1_000_000.0

  defp format_ms(value) when is_number(value) do
    value
    |> Kernel.*(1.0)
    |> :erlang.float_to_binary(decimals: 3)
    |> then(&"#{&1}ms")
  end

  defp env_workload_profile(name, default)
       when is_binary(name) and default in [:balanced, :production_like] do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case value |> String.trim() |> String.downcase() do
          "balanced" -> :balanced
          "production_like" -> :production_like
          "production-like" -> :production_like
          other -> raise ArgumentError, "invalid workload profile for #{name}: #{inspect(other)}"
        end
    end
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

  defp env_read_mode(name, default) when is_binary(name) and default in [:point, :catchup] do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case value |> String.trim() |> String.downcase() do
          "point" -> :point
          "catchup" -> :catchup
          other -> raise ArgumentError, "invalid read mode for #{name}: #{inspect(other)}"
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
