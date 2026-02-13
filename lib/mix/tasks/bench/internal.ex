defmodule Mix.Tasks.Bench.Internal do
  require Logger

  alias Starcite.Config.Size
  alias Starcite.Runtime
  alias Starcite.Runtime.{EventStore, RaftFSM}
  alias Starcite.Session

  def run do
    ensure_apps_stopped()
    config = benchmark_config()
    configure_runtime(config)
    Mix.Task.run("app.start")
    Logger.configure(level: config.log_level)
    print_config(config)
    EventStore.clear()

    sessions = build_session_ids(config.session_count)
    session_count = tuple_size(sessions)
    input_event_template = input_event_template(config.payload_bytes)
    stored_event_template = stored_event_template(input_event_template)

    runtime_sessions = prepare_runtime_sessions(sessions)
    fsm_initial_state = build_fsm_state(runtime_sessions)

    put_counter = :atomics.new(1, [])
    :atomics.put(put_counter, 1, 0)

    raw_insert_counter = :atomics.new(1, [])
    :atomics.put(raw_insert_counter, 1, 0)

    runtime_counter = :atomics.new(1, [])
    :atomics.put(runtime_counter, 1, 0)

    fsm_counter = :atomics.new(1, [])
    :atomics.put(fsm_counter, 1, 0)

    session_counter = :atomics.new(1, [])
    :atomics.put(session_counter, 1, 0)

    base_session =
      Session.new("bench-session", metadata: %{bench: true, scenario: "internal_attribution"})

    session_append = fn ->
      seq = :atomics.add_get(session_counter, 1, 1)
      session = %Session{base_session | last_seq: seq - 1}
      event_with_producer = with_bench_producer(input_event_template, "bench-session", 1)

      case Session.append_event(session, event_with_producer) do
        {:appended, _updated, _event} ->
          :ok

        {:deduped, _session, _seq} ->
          :ok

        {:error, reason} ->
          raise "session_append failed: #{inspect(reason)}"
      end
    end

    put_event = fn ->
      seq = :atomics.add_get(put_counter, 1, 1)
      session_id = elem(sessions, rem(seq - 1, session_count))
      event = Map.put(stored_event_template, :seq, seq)

      case EventStore.put_event(session_id, event) do
        :ok -> :ok
        {:error, reason} -> raise "event_store.put_event failed: #{inspect(reason)}"
      end
    end

    raw_ets_insert = fn ->
      seq = :atomics.add_get(raw_insert_counter, 1, 1)
      session_id = elem(sessions, rem(seq - 1, session_count))
      event = Map.put(stored_event_template, :seq, seq)

      event_table = :ets.whereis(:starcite_event_store_events)
      index_table = :ets.whereis(:starcite_event_store_session_max_seq)

      true = :ets.insert(event_table, {{session_id, seq}, event})
      true = :ets.insert(index_table, {session_id, seq})
      :ok
    end

    fsm_apply_append = fn ->
      seq = :atomics.add_get(fsm_counter, 1, 1)
      session_id = elem(runtime_sessions, rem(seq - 1, session_count))
      producer_seq = div(seq - 1, session_count) + 1
      event_with_producer = with_bench_producer(input_event_template, session_id, producer_seq)
      fsm_state = Process.get(:bench_fsm_state) || fsm_initial_state
      meta = %{index: seq}

      case RaftFSM.apply(meta, {:append_event, session_id, event_with_producer, []}, fsm_state) do
        {updated_state, {:reply, {:ok, _reply}}, _effects} ->
          Process.put(:bench_fsm_state, updated_state)
          :ok

        {updated_state, {:reply, {:ok, _reply}}} ->
          Process.put(:bench_fsm_state, updated_state)
          :ok

        {_updated_state, {:reply, {:error, reason}}} ->
          raise "raft_fsm.apply append failed: #{inspect(reason)}"
      end
    end

    runtime_append = fn ->
      seq = :atomics.add_get(runtime_counter, 1, 1)
      session_id = elem(runtime_sessions, rem(seq - 1, session_count))
      producer_seq = div(seq - 1, session_count) + 1
      event_with_producer = with_bench_producer(input_event_template, session_id, producer_seq)

      case Runtime.append_event(session_id, event_with_producer) do
        {:ok, _reply} -> :ok
        {:error, reason} -> raise "runtime.append_event failed: #{inspect(reason)}"
        {:timeout, leader} -> raise "runtime.append_event timeout: #{inspect(leader)}"
      end
    end

    run_benchee(
      %{
        "session.append_event" => session_append,
        "event_store.memory_bytes" => fn -> EventStore.memory_bytes() end,
        "event_store.put_event" => put_event,
        "event_store.raw_ets_insert" => raw_ets_insert,
        "raft_fsm.apply_append" => fsm_apply_append,
        "runtime.append_event" => runtime_append
      },
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

    archive_flush_interval_ms = env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", 60_000)
    Application.put_env(:starcite, :archive_flush_interval_ms, archive_flush_interval_ms)

    Application.put_env(
      :starcite,
      :event_store_max_bytes,
      Size.parse_bytes!(
        System.get_env("BENCH_EVENT_STORE_MAX_SIZE", "64GB"),
        "BENCH_EVENT_STORE_MAX_SIZE",
        examples: "256MB, 4G, 1024M"
      )
    )
  end

  defp benchmark_config do
    %{
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft_internal"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      session_count: env_integer("BENCH_SESSION_COUNT", 256),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      parallel: env_integer("BENCH_PARALLEL", 8),
      warmup_seconds: env_integer("BENCH_WARMUP_SECONDS", 3),
      time_seconds: env_integer("BENCH_TIME_SECONDS", 10)
    }
  end

  defp print_config(config) do
    IO.puts("Internal attribution config:")
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

    IO.puts(
      "  event_store_max_bytes: #{inspect(Application.get_env(:starcite, :event_store_max_bytes))}"
    )
  end

  defp build_session_ids(session_count) when is_integer(session_count) and session_count > 0 do
    run_id = System.system_time(:millisecond)

    1..session_count
    |> Enum.map(fn index ->
      "internal-benchee-#{run_id}-#{index}"
    end)
    |> List.to_tuple()
  end

  defp prepare_runtime_sessions(session_ids) when is_tuple(session_ids) do
    Tuple.to_list(session_ids)
    |> Enum.map(fn id ->
      case Runtime.create_session(
             id: id,
             metadata: %{bench: true, scenario: "internal_attribution_runtime"}
           ) do
        {:ok, _session} -> id
        {:error, :session_exists} -> id
        {:error, reason} -> raise "create_session failed for #{id}: #{inspect(reason)}"
      end
    end)
    |> List.to_tuple()
  end

  defp build_fsm_state(session_ids) when is_tuple(session_ids) do
    sessions =
      session_ids
      |> Tuple.to_list()
      |> Enum.reduce(%{}, fn id, acc ->
        Map.put(
          acc,
          id,
          Session.new(id, metadata: %{bench: true, scenario: "internal_attribution_fsm"})
        )
      end)

    %RaftFSM{group_id: 0, sessions: sessions}
  end

  defp input_event_template(payload_bytes) do
    %{
      type: "content",
      payload: %{text: payload_text(payload_bytes)},
      actor: "agent:benchee",
      source: "benchmark",
      metadata: %{bench: true, scenario: "internal_attribution"},
      producer_id: "bench-producer",
      producer_seq: 1,
      idempotency_key: nil,
      refs: %{}
    }
  end

  defp stored_event_template(input_event_template) when is_map(input_event_template) do
    Map.put(input_event_template, :inserted_at, NaiveDateTime.utc_now())
  end

  defp payload_text(payload_bytes) when is_integer(payload_bytes) and payload_bytes > 0 do
    String.duplicate("x", payload_bytes)
  end

  defp with_bench_producer(event, producer_id, producer_seq)
       when is_map(event) and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 do
    event
    |> Map.put(:producer_id, producer_id)
    |> Map.put(:producer_seq, producer_seq)
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
