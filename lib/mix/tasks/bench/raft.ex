defmodule Mix.Tasks.Bench.Raft do
  require Logger

  alias Starcite.Auth.Principal
  alias Starcite.Config.Size
  alias Starcite.DataPlane.{EventStore, RaftManager}

  @percentiles [50, 90, 95, 99, 99.9]

  @scenario_keys [
    :create_session,
    :append_event,
    :append_events_batch,
    :ack_archived,
    :hydrate_session
  ]

  @default_timeout_ms 2_000
  @default_archive_flush_interval_ms 60_000

  @default_tail_keep 1_000
  @default_producer_max_entries 10_000
  @default_hydrate_archived_seq 1

  @tenant_id "bench"

  def run do
    ensure_apps_stopped()
    config = benchmark_config()
    ensure_group_capacity!(config.num_groups)
    configure_runtime(config)

    Mix.Task.run("app.start")
    Logger.configure(level: config.log_level)

    print_config(config)
    ensure_benchee_available!()

    principal = bench_principal!()
    run_id = System.system_time(:millisecond)

    contexts =
      @scenario_keys
      |> Enum.with_index()
      |> Enum.map(fn {scenario, group_id} ->
        server_id = start_group!(group_id)

        {scenario,
         %{
           scenario: scenario,
           group_id: group_id,
           server_id: server_id,
           run_id: run_id,
           principal: principal,
           config: config
         }}
      end)
      |> Map.new()

    clear_event_store()

    summaries =
      [
        bench_create_session(contexts.create_session),
        bench_append_event(contexts.append_event),
        bench_append_events_batch(contexts.append_events_batch),
        bench_ack_archived(contexts.ack_archived),
        bench_hydrate_session(contexts.hydrate_session)
      ]

    print_summary(summaries)
    :ok
  end

  defp bench_create_session(context) when is_map(context) do
    clear_event_store()

    timeout_ms = context.config.raft_timeout_ms
    metadata = %{bench: true, scenario: "raft_create_session"}
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)

    before_each = fn _input ->
      index = :atomics.add_get(counter, 1, 1)
      "raft-create-#{context.run_id}-#{index}"
    end

    job = fn session_id ->
      process_command!(
        context.server_id,
        {
          :create_session,
          session_id,
          nil,
          context.principal,
          @tenant_id,
          metadata
        },
        timeout_ms
      )
    end

    run_case("raft.create_session", job, context.config, before_each: before_each)
  end

  defp bench_append_event(context) when is_map(context) do
    clear_event_store()

    timeout_ms = context.config.raft_timeout_ms
    payload_text = payload_text(context.config.payload_bytes)
    producer_key = :append_event

    before_each = fn _input ->
      {session_id, producer_seq} = worker_session_and_next_seq(context, producer_key, 1)

      event =
        event_payload(
          payload_text,
          "writer:append_event:#{session_id}",
          producer_seq,
          "raft_append_event"
        )

      {session_id, event}
    end

    job = fn {session_id, event} ->
      process_command!(
        context.server_id,
        {:append_event, session_id, event, nil},
        timeout_ms
      )
    end

    run_case("raft.append_event", job, context.config, before_each: before_each)
  end

  defp bench_append_events_batch(context) when is_map(context) do
    clear_event_store()

    timeout_ms = context.config.raft_timeout_ms
    payload_text = payload_text(context.config.payload_bytes)
    batch_size = context.config.batch_size
    producer_key = :append_events_batch

    before_each = fn _input ->
      {session_id, first_seq} = worker_session_and_next_seq(context, producer_key, batch_size)

      events =
        for offset <- 0..(batch_size - 1) do
          producer_seq = first_seq + offset

          event_payload(
            payload_text,
            "writer:append_events_batch:#{session_id}",
            producer_seq,
            "raft_append_events_batch"
          )
        end

      {session_id, events}
    end

    job = fn {session_id, events} ->
      process_command!(
        context.server_id,
        {:append_events, session_id, events, []},
        timeout_ms
      )
    end

    run_case("raft.append_events_batch", job, context.config, before_each: before_each)
  end

  defp bench_ack_archived(context) when is_map(context) do
    clear_event_store()

    timeout_ms = context.config.raft_timeout_ms
    payload_text = payload_text(context.config.payload_bytes)
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)

    before_each = fn _input ->
      index = :atomics.add_get(counter, 1, 1)
      session_id = "raft-ack-#{context.run_id}-#{index}"

      _create_reply =
        process_command!(
          context.server_id,
          {
            :create_session,
            session_id,
            nil,
            context.principal,
            @tenant_id,
            %{bench: true, scenario: "raft_ack_archived"}
          },
          timeout_ms
        )

      append_event =
        event_payload(
          payload_text,
          "writer:ack_archived:#{session_id}",
          1,
          "raft_ack_archived"
        )

      %{seq: upto_seq} =
        process_command!(
          context.server_id,
          {:append_event, session_id, append_event, nil},
          timeout_ms
        )

      {session_id, upto_seq}
    end

    job = fn {session_id, upto_seq} ->
      process_command!(
        context.server_id,
        {:ack_archived, session_id, upto_seq},
        timeout_ms
      )
    end

    run_case("raft.ack_archived", job, context.config, before_each: before_each)
  end

  defp bench_hydrate_session(context) when is_map(context) do
    clear_event_store()

    timeout_ms = context.config.raft_timeout_ms
    inserted_at = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
    counter = :atomics.new(1, [])
    :atomics.put(counter, 1, 0)

    before_each = fn _input ->
      index = :atomics.add_get(counter, 1, 1)
      "raft-hydrate-#{context.run_id}-#{index}"
    end

    job = fn session_id ->
      hydrate_result =
        process_command!(
          context.server_id,
          {
            :hydrate_session,
            session_id,
            "Hydrated",
            context.principal,
            @tenant_id,
            %{bench: true, scenario: "raft_hydrate_session"},
            inserted_at,
            context.config.hydrate_archived_seq,
            context.config.hydrate_archived_seq,
            @default_tail_keep,
            @default_producer_max_entries
          },
          timeout_ms
        )

      {session_id, hydrate_result}
    end

    after_each = fn {session_id, :hydrated} ->
      cleanup_event =
        event_payload(
          "x",
          "writer:hydrate_cleanup:#{session_id}",
          1,
          "raft_hydrate_cleanup"
        )

      %{seq: cleanup_seq} =
        process_command!(
          context.server_id,
          {:append_event, session_id, cleanup_event, nil},
          timeout_ms
        )

      _ack_reply =
        process_command!(
          context.server_id,
          {:ack_archived, session_id, cleanup_seq},
          timeout_ms
        )

      :ok
    end

    run_case("raft.hydrate_session", job, context.config,
      before_each: before_each,
      after_each: after_each
    )
  end

  defp worker_session_and_next_seq(context, scenario_key, increment)
       when is_map(context) and is_atom(scenario_key) and is_integer(increment) and increment > 0 do
    session_key = {:raft_bench_worker_session_id, scenario_key}
    seq_key = {:raft_bench_worker_producer_seq, scenario_key}

    session_id =
      case Process.get(session_key) do
        nil ->
          id =
            "raft-worker-#{scenario_key}-#{context.run_id}-#{System.unique_integer([:positive, :monotonic])}"

          _create_reply =
            process_command!(
              context.server_id,
              {
                :create_session,
                id,
                nil,
                context.principal,
                @tenant_id,
                %{bench: true, scenario: Atom.to_string(scenario_key)}
              },
              context.config.raft_timeout_ms
            )

          Process.put(session_key, id)
          Process.put(seq_key, 0)
          id

        id when is_binary(id) ->
          id
      end

    current_seq = Process.get(seq_key, 0)
    first_seq = current_seq + 1
    Process.put(seq_key, current_seq + increment)

    {session_id, first_seq}
  end

  defp run_case(name, job, config, opts)
       when is_binary(name) and is_function(job) and is_map(config) and is_list(opts) do
    IO.puts("Raft Benchee scenario: #{name}")

    benchee_options =
      [
        parallel: config.parallel,
        warmup: config.warmup_seconds,
        time: config.time_seconds,
        memory_time: 0,
        percentiles: @percentiles,
        print: [fast_warning: false]
      ]
      |> maybe_put_hook(:before_each, opts[:before_each])
      |> maybe_put_hook(:after_each, opts[:after_each])

    suite = run_benchee(%{name => job}, benchee_options)
    summary = summarize_case!(suite, name)

    IO.puts(
      "  summary: ips=#{format_float(summary.ips, 2)} avg=#{format_ms(summary.avg_ms)} " <>
        "p99=#{format_ms(summary.p99_ms)} p99.9=#{format_ms(summary.p999_ms)} " <>
        "max=#{format_ms(summary.max_ms)}"
    )

    summary
  end

  defp run_benchee(scenarios, options) when is_map(scenarios) and is_list(options) do
    benchee = :"Elixir.Benchee"

    if Code.ensure_loaded?(benchee) do
      apply(benchee, :run, [scenarios, options])
    else
      Mix.raise("Benchee is not available. Run benchmarks with MIX_ENV=dev.")
    end
  end

  defp summarize_case!(suite, scenario_name) when is_map(suite) and is_binary(scenario_name) do
    scenario = scenario_from_suite!(suite, scenario_name)
    stats = scenario_stats!(scenario, scenario_name)
    percentiles = fetch_map!(stats, :percentiles, "#{scenario_name} stats")

    %{
      scenario: scenario_name,
      ips: fetch_number!(stats, :ips, "#{scenario_name} stats"),
      avg_ms: nanos_to_ms(fetch_number!(stats, :average, "#{scenario_name} stats")),
      p99_ms: nanos_to_ms(percentile_value!(percentiles, 99.0)),
      p999_ms: nanos_to_ms(percentile_value!(percentiles, 99.9)),
      max_ms: nanos_to_ms(fetch_number!(stats, :maximum, "#{scenario_name} stats"))
    }
  end

  defp scenario_from_suite!(suite, scenario_name)
       when is_map(suite) and is_binary(scenario_name) do
    scenarios = fetch_any!(suite, [:scenarios], "Benchee suite")

    cond do
      is_map(scenarios) ->
        fetch_map!(scenarios, scenario_name, "Benchee suite scenarios")

      is_list(scenarios) ->
        Enum.find(scenarios, fn
          %{name: ^scenario_name} -> true
          %{job_name: ^scenario_name} -> true
          %{display_name: ^scenario_name} -> true
          _ -> false
        end) ||
          Mix.raise("missing Benchee scenario #{inspect(scenario_name)}")

      true ->
        Mix.raise("invalid Benchee suite.scenarios: #{inspect(scenarios)}")
    end
  end

  defp scenario_stats!(scenario, scenario_name)
       when is_map(scenario) and is_binary(scenario_name) do
    case Map.fetch(scenario, :run_time_statistics) do
      {:ok, stats} when is_map(stats) ->
        stats

      {:ok, stats} when is_struct(stats) ->
        Map.from_struct(stats)

      _ ->
        run_time_data = fetch_map!(scenario, :run_time_data, "scenario #{scenario_name}")

        case Map.fetch(run_time_data, :statistics) do
          {:ok, stats} when is_map(stats) -> stats
          {:ok, stats} when is_struct(stats) -> Map.from_struct(stats)
          _ -> Mix.raise("missing Benchee runtime stats for #{inspect(scenario_name)}")
        end
    end
  end

  defp percentile_value!(percentiles, percentile)
       when is_map(percentiles) and is_float(percentile) do
    candidates = [
      percentile,
      trunc(percentile),
      Float.round(percentile, 1),
      to_string(percentile)
    ]

    Enum.find_value(candidates, fn key ->
      case Map.fetch(percentiles, key) do
        {:ok, value} ->
          if is_number(value), do: value, else: nil

        :error ->
          nil
      end
    end) ||
      Mix.raise("missing Benchee percentile #{inspect(percentile)} in #{inspect(percentiles)}")
  end

  defp print_summary(summaries) when is_list(summaries) do
    IO.puts("\nRaft command latency summary (ms):")

    Enum.each(summaries, fn summary ->
      IO.puts(
        "  #{summary.scenario}: ips=#{format_float(summary.ips, 2)} " <>
          "avg=#{format_ms(summary.avg_ms)} p99=#{format_ms(summary.p99_ms)} " <>
          "p99.9=#{format_ms(summary.p999_ms)} max=#{format_ms(summary.max_ms)}"
      )
    end)
  end

  defp maybe_put_hook(options, _key, nil), do: options

  defp maybe_put_hook(options, key, hook)
       when is_list(options) and is_atom(key) and is_function(hook, 1) do
    Keyword.put(options, key, hook)
  end

  defp process_command!(server_id, command, timeout_ms)
       when is_atom(server_id) and is_tuple(command) and is_integer(timeout_ms) and timeout_ms > 0 do
    case :ra.process_command({server_id, Node.self()}, command, timeout_ms) do
      {:ok, {:reply, {:ok, reply}}, _leader} ->
        reply

      {:ok, {:reply, {:error, reason}}, _leader} ->
        raise "raft command failed: #{inspect(command)} reason=#{inspect(reason)}"

      {:timeout, leader} ->
        raise "raft command timeout: #{inspect(command)} leader=#{inspect(leader)}"

      {:error, reason} ->
        raise "raft command error: #{inspect(command)} reason=#{inspect(reason)}"
    end
  end

  defp payload_text(payload_bytes) when is_integer(payload_bytes) and payload_bytes > 0 do
    String.duplicate("x", payload_bytes)
  end

  defp event_payload(payload_text, producer_id, producer_seq, scenario)
       when is_binary(payload_text) and is_binary(producer_id) and producer_id != "" and
              is_integer(producer_seq) and producer_seq > 0 and is_binary(scenario) do
    %{
      type: "content",
      payload: %{text: payload_text},
      actor: "agent:benchee",
      source: "benchmark",
      metadata: %{bench: true, scenario: scenario},
      refs: %{},
      idempotency_key: nil,
      producer_id: producer_id,
      producer_seq: producer_seq
    }
  end

  defp ensure_benchee_available! do
    benchee = :"Elixir.Benchee"

    unless Code.ensure_loaded?(benchee) do
      Mix.raise("Benchee is not available. Run benchmarks with MIX_ENV=dev.")
    end

    :ok
  end

  defp clear_event_store do
    EventStore.clear()
  end

  defp bench_principal! do
    case Principal.new(@tenant_id, "bench", :service) do
      {:ok, principal} -> principal
      {:error, reason} -> raise "failed to build bench principal: #{inspect(reason)}"
    end
  end

  defp ensure_group_capacity!(num_groups) when is_integer(num_groups) and num_groups > 0 do
    required = length(@scenario_keys)

    if num_groups < required do
      Mix.raise(
        "BENCH_NUM_GROUPS must be >= #{required} for raft command bench, got #{num_groups}"
      )
    end
  end

  defp start_group!(group_id) when is_integer(group_id) and group_id >= 0 do
    case RaftManager.start_group(group_id) do
      :ok -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> raise "failed to start Raft group #{group_id}: #{inspect(reason)}"
    end

    server_id = RaftManager.server_id(group_id)
    wait_for_server_ready!(server_id, 200)
    server_id
  end

  defp wait_for_server_ready!(_server_id, 0), do: raise("Raft server did not become ready")

  defp wait_for_server_ready!(server_id, attempts)
       when is_atom(server_id) and is_integer(attempts) and attempts > 0 do
    case :ra.members({server_id, Node.self()}) do
      {:ok, _members, {_leader_id, leader_node}}
      when is_atom(leader_node) and not is_nil(leader_node) ->
        :ok

      _other ->
        Process.sleep(25)
        wait_for_server_ready!(server_id, attempts - 1)
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
    Application.put_env(:starcite, :write_replication_factor, 1)
    Application.put_env(:starcite, :write_node_ids, [Node.self()])
    Application.put_env(:starcite, :raft_data_dir, config.raft_data_dir)

    if config.clean_raft_data_dir do
      File.rm_rf!(config.raft_data_dir)
    end

    File.mkdir_p!(config.raft_data_dir)
    ra_system_dir = Path.join(config.raft_data_dir, "ra_system")
    File.mkdir_p!(ra_system_dir)

    Application.put_env(:ra, :data_dir, to_charlist(ra_system_dir))
    Application.delete_env(:ra, :wal_data_dir)

    Application.put_env(:starcite, :archive_flush_interval_ms, config.archive_flush_interval_ms)

    Application.put_env(
      :starcite,
      :event_store_max_bytes,
      Size.parse_bytes!(
        System.get_env("BENCH_EVENT_STORE_MAX_SIZE", "8GB"),
        "BENCH_EVENT_STORE_MAX_SIZE",
        examples: "256MB, 4G, 1024M"
      )
    )
  end

  defp benchmark_config do
    %{
      raft_data_dir: System.get_env("BENCH_RAFT_DATA_DIR", "tmp/bench_raft_commands"),
      clean_raft_data_dir: env_boolean("BENCH_CLEAN_RAFT_DATA_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      num_groups: env_integer("BENCH_NUM_GROUPS", 16),
      raft_timeout_ms: env_integer("BENCH_RAFT_TIMEOUT_MS", @default_timeout_ms),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      batch_size: env_integer("BENCH_BATCH_SIZE", 8),
      hydrate_archived_seq:
        env_integer("BENCH_RAFT_HYDRATE_ARCHIVED_SEQ", @default_hydrate_archived_seq),
      archive_flush_interval_ms:
        env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", @default_archive_flush_interval_ms),
      parallel: env_integer("BENCH_PARALLEL", 4),
      warmup_seconds: env_integer("BENCH_WARMUP_SECONDS", 1),
      time_seconds: env_integer("BENCH_TIME_SECONDS", 5)
    }
  end

  defp print_config(config) do
    IO.puts("Raft command Benchee config:")
    IO.puts("  raft_data_dir: #{config.raft_data_dir}")
    IO.puts("  clean_raft_data_dir: #{config.clean_raft_data_dir}")
    IO.puts("  log_level: #{config.log_level}")
    IO.puts("  num_groups: #{config.num_groups}")
    IO.puts("  raft_timeout_ms: #{config.raft_timeout_ms}")
    IO.puts("  payload_bytes: #{config.payload_bytes}")
    IO.puts("  batch_size: #{config.batch_size}")
    IO.puts("  hydrate_archived_seq: #{config.hydrate_archived_seq}")
    IO.puts("  archive_flush_interval_ms: #{config.archive_flush_interval_ms}")
    IO.puts("  parallel: #{config.parallel}")
    IO.puts("  warmup_seconds: #{config.warmup_seconds}")
    IO.puts("  time_seconds: #{config.time_seconds}")
  end

  defp fetch_any!(map, [key], context) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Mix.raise("missing #{inspect(key)} in #{context}")
    end
  end

  defp fetch_map!(map, key, context) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) -> value
      {:ok, value} when is_struct(value) -> Map.from_struct(value)
      {:ok, value} -> Mix.raise("invalid #{context}[#{inspect(key)}]: #{inspect(value)}")
      :error -> Mix.raise("missing #{inspect(key)} in #{context}")
    end
  end

  defp fetch_map!(map, key, context) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} when is_map(value) -> value
      {:ok, value} when is_struct(value) -> Map.from_struct(value)
      {:ok, value} -> Mix.raise("invalid #{context}[#{inspect(key)}]: #{inspect(value)}")
      :error -> Mix.raise("missing #{inspect(key)} in #{context}")
    end
  end

  defp fetch_number!(map, key, context) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} when is_number(value) -> value
      {:ok, value} -> Mix.raise("invalid #{context}[#{inspect(key)}]: #{inspect(value)}")
      :error -> Mix.raise("missing #{inspect(key)} in #{context}")
    end
  end

  defp nanos_to_ms(nanos) when is_number(nanos), do: nanos / 1_000_000

  defp format_ms(value) when is_number(value), do: "#{format_float(value, 3)}ms"

  defp format_float(value, decimals)
       when is_number(value) and is_integer(decimals) and decimals >= 0 do
    :erlang.float_to_binary(value * 1.0, decimals: decimals)
  end

  defp env_integer(name, default)
       when is_binary(name) and is_integer(default) and default > 0 do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(String.trim(value)) do
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
