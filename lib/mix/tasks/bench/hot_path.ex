defmodule Mix.Tasks.Bench.HotPath do
  require Logger

  alias Starcite.Config.Size
  alias Starcite.Runtime

  def run do
    ensure_apps_stopped()
    config = benchmark_config()
    configure_runtime(config)
    Mix.Task.run("app.start")
    Logger.configure(level: config.log_level)
    print_config(config)

    sessions = prepare_sessions(config.session_count)
    session_count = tuple_size(sessions)
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
      index = :atomics.add_get(counter, 1, 1)
      session_id = elem(sessions, rem(index - 1, session_count))
      event_with_producer = with_bench_producer(event, index)

      case Runtime.append_event(session_id, event_with_producer) do
        {:ok, _reply} -> :ok
        {:error, reason} -> raise "append failed: #{inspect(reason)}"
        {:timeout, leader} -> raise "append timeout: #{inspect(leader)}"
      end
    end

    run_benchee(
      %{"runtime.append_event" => append_event},
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
    %{
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      session_count: env_integer("BENCH_SESSION_COUNT", 256),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      parallel: 4,
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

      case Runtime.create_session(id: id, metadata: %{bench: true, scenario: "hot_path_benchee"}) do
        {:ok, _session} -> id
        {:error, :session_exists} -> id
        {:error, reason} -> raise "create_session failed for #{id}: #{inspect(reason)}"
      end
    end)
    |> List.to_tuple()
  end

  defp with_bench_producer(event, index) when is_map(event) and is_integer(index) and index > 0 do
    event
    |> Map.put(:producer_id, "bench-producer-#{index}")
    |> Map.put(:producer_seq, 1)
  end

  defp payload_text(payload_bytes) when is_integer(payload_bytes) and payload_bytes > 0 do
    String.duplicate("x", payload_bytes)
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
