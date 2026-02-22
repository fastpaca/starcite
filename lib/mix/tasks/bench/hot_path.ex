defmodule Mix.Tasks.Bench.HotPath do
  require Logger

  alias Starcite.Config.Size
  alias Starcite.WritePath

  def run do
    ensure_apps_stopped()
    config = benchmark_config()
    configure_runtime(config)
    Mix.Task.run("app.start")
    Logger.configure(level: config.log_level)
    print_config(config)

    sessions = prepare_sessions(config.session_count)
    session_count = tuple_size(sessions)
    producer_state = prepare_producer_state(config, sessions)
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)

    event = %{
      type: "content",
      payload: %{text: payload_text(config.payload_bytes)},
      actor: "agent:benchee",
      source: "benchmark",
      metadata: %{bench: true, scenario: "hot_path_benchee"}
    }

    append_event = fn ->
      last_index = :atomics.add_get(counter, 1, config.batch_size)
      first_index = last_index - config.batch_size + 1
      session_index = rem(first_index - 1, session_count) + 1
      session_id = elem(sessions, session_index - 1)

      events =
        for offset <- 0..(config.batch_size - 1) do
          index = first_index + offset
          with_bench_producer(event, session_index, index, config, producer_state)
        end

      append_batch(session_id, events)
    end

    run_benchee(
      %{scenario_name(config.batch_size) => append_event},
      parallel: config.parallel,
      warmup: config.warmup_seconds,
      time: config.time_seconds,
      memory_time: 0,
      print: [fast_warning: false]
    )
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
    Application.put_env(:starcite, :raft_data_dir, config.raft_data_dir)

    if config.clean_raft_data_dir do
      File.rm_rf!(config.raft_data_dir)
    end

    File.mkdir_p!(config.raft_data_dir)
    ra_system_dir = Path.join(config.raft_data_dir, "ra_system")
    File.mkdir_p!(ra_system_dir)
    Application.put_env(:ra, :data_dir, to_charlist(ra_system_dir))
    Application.delete_env(:ra, :wal_data_dir)

    archive_flush_interval_ms = env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", 5_000)
    Application.put_env(:starcite, :archive_flush_interval_ms, archive_flush_interval_ms)

    if max_size = System.get_env("BENCH_EVENT_STORE_MAX_SIZE") do
      Application.put_env(
        :starcite,
        :event_store_max_bytes,
        Size.parse_bytes!(max_size, "BENCH_EVENT_STORE_MAX_SIZE", examples: "256MB, 4G, 1024M")
      )
    end
  end

  defp benchmark_config do
    parallel = env_integer("BENCH_PARALLEL", 4)

    %{
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      session_count: env_integer("BENCH_SESSION_COUNT", 256),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      batch_size: env_integer("BENCH_BATCH_SIZE", 1),
      producer_mode: env_producer_mode("BENCH_PRODUCER_MODE", :stable),
      producer_pool_size: env_integer("BENCH_PRODUCER_POOL_SIZE", parallel),
      parallel: parallel,
      warmup_seconds: env_integer("BENCH_WARMUP_SECONDS", 5),
      time_seconds: env_integer("BENCH_TIME_SECONDS", 30)
    }
  end

  defp print_config(config) do
    IO.puts("Hot-path Benchee config:")
    IO.puts("  raft_data_dir: #{config.raft_data_dir}")
    IO.puts("  clean_raft_data_dir: #{config.clean_raft_data_dir}")
    IO.puts("  log_level: #{config.log_level}")
    IO.puts("  sessions: #{config.session_count}")
    IO.puts("  payload_bytes: #{config.payload_bytes}")
    IO.puts("  batch_size: #{config.batch_size}")
    IO.puts("  producer_mode: #{config.producer_mode}")
    IO.puts("  producer_pool_size: #{config.producer_pool_size}")
    IO.puts("  parallel: #{config.parallel}")
    IO.puts("  warmup_seconds: #{config.warmup_seconds}")
    IO.puts("  time_seconds: #{config.time_seconds}")

    IO.puts(
      "  archive_flush_interval_ms: #{Application.get_env(:starcite, :archive_flush_interval_ms)}"
    )

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

  defp append_batch(session_id, [event]) when is_binary(session_id) and is_map(event) do
    case WritePath.append_event(session_id, event) do
      {:ok, _reply} -> :ok
      {:error, reason} -> raise "append failed: #{inspect(reason)}"
      {:timeout, leader} -> raise "append timeout: #{inspect(leader)}"
    end
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
end
