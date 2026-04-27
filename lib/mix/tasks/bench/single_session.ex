defmodule Mix.Tasks.Bench.SingleSession do
  require Logger

  alias Starcite.Config.Size
  alias Starcite.ReadPath
  alias Starcite.WritePath
  alias Starcite.Auth.Principal
  alias StarciteWeb.Auth.Context
  alias StarciteWeb.SessionController

  @bench_scope "single_session"

  def run do
    ensure_apps_stopped()
    config = benchmark_config()
    configure_runtime(config)
    Mix.Task.run("app.start")
    Logger.configure(level: config.log_level)
    print_config(config)

    auth = Context.none()
    controller_session_id = prepare_controller_session(auth)
    jwt_auth = jwt_auth_context(controller_session_id)

    controller_producers =
      prepare_shared_producers(
        config.producer_pool_size,
        "#{controller_session_id}:controller"
      )

    write_path_session_id = prepare_write_path_session()
    read_path_session_id = prepare_read_path_session()

    write_path_producers =
      prepare_shared_producers(
        config.producer_pool_size,
        "#{write_path_session_id}:write_path"
      )

    controller_event_template = controller_event_template(config.payload_bytes)
    write_path_event_template = write_path_event_template(config.payload_bytes)

    controller_append_none = fn ->
      input = next_shared_input_string_keys(controller_event_template, controller_producers)
      append_event_controller_api!(controller_session_id, input, auth)
    end

    controller_append_jwt = fn ->
      input = next_shared_input_string_keys(controller_event_template, controller_producers)
      append_event_controller_api!(controller_session_id, input, jwt_auth)
    end

    write_path_append = fn ->
      input = next_shared_input_atom_keys(write_path_event_template, write_path_producers)
      append_event_write_path!(write_path_session_id, input)
    end

    read_path_get_session = fn ->
      get_session_routed!(read_path_session_id)
    end

    read_path_replay = fn ->
      replay_from_cursor!(read_path_session_id, 0, 1)
    end

    run_benchee(
      %{
        "web.controller.append_api(routed,auth=none)" => controller_append_none,
        "web.controller.append_api(routed,auth=jwt)" => controller_append_jwt,
        "api.write_path.append(routed)" => write_path_append,
        "api.read_path.get_session_routed" => read_path_get_session,
        "api.read_path.replay_from_cursor" => read_path_replay
      },
      parallel: config.parallel,
      warmup: config.warmup_seconds,
      time: config.time_seconds,
      memory_time: config.memory_time_seconds,
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
    _ = Application.stop(:khepri)
    :ok
  end

  defp configure_runtime(config) do
    Application.put_env(:logger, :level, config.log_level)
    Logger.configure(level: config.log_level)
    Application.put_env(:starcite, :routing_store_dir, config.routing_store_dir)
    Application.put_env(:starcite, :archive_flush_interval_ms, config.archive_flush_interval_ms)
    Application.put_env(:starcite, StarciteWeb.Auth, mode: :none)

    Application.put_env(
      :starcite,
      :event_store_max_bytes,
      Size.parse_bytes!(
        System.get_env("BENCH_EVENT_STORE_MAX_SIZE", "8GB"),
        "BENCH_EVENT_STORE_MAX_SIZE",
        examples: "256MB, 4G, 1024M"
      )
    )

    if config.clean_routing_store_dir do
      File.rm_rf!(config.routing_store_dir)
    end

    File.mkdir_p!(config.routing_store_dir)
  end

  defp benchmark_config do
    parallel = env_integer("BENCH_PARALLEL", 4)

    %{
      routing_store_dir: System.get_env("BENCH_ROUTING_STORE_DIR", "tmp/bench_routing_store"),
      clean_routing_store_dir: env_boolean("BENCH_CLEAN_ROUTING_STORE_DIR", true),
      log_level: env_log_level("BENCH_LOG_LEVEL", :error),
      payload_bytes: env_integer("BENCH_PAYLOAD_BYTES", 256),
      producer_pool_size: env_integer("BENCH_PRODUCER_POOL_SIZE", max(parallel * 8, 64)),
      parallel: parallel,
      warmup_seconds: env_integer("BENCH_WARMUP_SECONDS", 3),
      time_seconds: env_integer("BENCH_TIME_SECONDS", 12),
      memory_time_seconds: env_integer("BENCH_MEMORY_TIME_SECONDS", 2),
      archive_flush_interval_ms: env_integer("BENCH_ARCHIVE_FLUSH_INTERVAL_MS", 60_000)
    }
  end

  defp print_config(config) do
    IO.puts("Single-session Benchee config:")
    IO.puts("  routing_store_dir: #{config.routing_store_dir}")
    IO.puts("  clean_routing_store_dir: #{config.clean_routing_store_dir}")
    IO.puts("  log_level: #{config.log_level}")
    IO.puts("  payload_bytes: #{config.payload_bytes}")
    IO.puts("  producer_pool_size: #{config.producer_pool_size}")
    IO.puts("  parallel: #{config.parallel}")
    IO.puts("  warmup_seconds: #{config.warmup_seconds}")
    IO.puts("  time_seconds: #{config.time_seconds}")
    IO.puts("  memory_time_seconds: #{config.memory_time_seconds}")
    IO.puts("  archive_flush_interval_ms: #{config.archive_flush_interval_ms}")

    IO.puts(
      "  event_store_max_bytes: #{inspect(Application.get_env(:starcite, :event_store_max_bytes))}"
    )
  end

  defp prepare_controller_session(%Context{} = auth) do
    run_id = System.system_time(:millisecond)
    session_id = "single-benchee-controller-#{run_id}"

    params = %{
      "id" => session_id,
      "metadata" => %{"bench" => true, "scenario" => @bench_scope}
    }

    case SessionController.create(controller_conn("/v1/sessions", auth), params) do
      %Plug.Conn{status: 201} ->
        session_id

      %Plug.Conn{status: status, resp_body: body} ->
        raise "controller session create failed status=#{status} body=#{inspect(body)}"

      {:error, reason} ->
        raise "controller session create failed: #{inspect(reason)}"
    end
  end

  defp prepare_write_path_session do
    run_id = System.system_time(:millisecond)
    session_id = "single-benchee-write-path-#{run_id}"

    case WritePath.create_session(
           id: session_id,
           tenant_id: "service",
           metadata: %{bench: true, scenario: @bench_scope}
         ) do
      {:ok, _session} ->
        session_id

      {:error, reason} ->
        raise "write_path session create failed: #{inspect(reason)}"
    end
  end

  defp prepare_read_path_session do
    run_id = System.system_time(:millisecond)
    session_id = "single-benchee-read-path-#{run_id}"

    case WritePath.create_session(
           id: session_id,
           tenant_id: "service",
           metadata: %{bench: true, scenario: @bench_scope}
         ) do
      {:ok, _session} ->
        seed_read_path_session!(session_id)
        session_id

      {:error, reason} ->
        raise "read_path session create failed: #{inspect(reason)}"
    end
  end

  defp prepare_shared_producers(producer_pool_size, session_id)
       when is_integer(producer_pool_size) and producer_pool_size > 0 and
              is_binary(session_id) and session_id != "" do
    producer_seqs = :atomics.new(producer_pool_size, [])
    worker_counter = :atomics.new(1, [])
    :atomics.put(worker_counter, 1, 0)

    producer_ids =
      1..producer_pool_size
      |> Enum.map(&"#{session_id}:producer:#{&1}")
      |> List.to_tuple()

    %{
      pool_size: producer_pool_size,
      producer_seqs: producer_seqs,
      worker_counter: worker_counter,
      producer_ids: producer_ids
    }
  end

  defp controller_event_template(payload_bytes)
       when is_integer(payload_bytes) and payload_bytes > 0 do
    %{
      "type" => "content",
      "payload" => %{"text" => String.duplicate("x", payload_bytes)},
      "source" => "benchmark",
      "metadata" => %{"bench" => true, "scenario" => @bench_scope},
      "producer_id" => "single-benchee-producer:bootstrap",
      "producer_seq" => 1
    }
  end

  defp write_path_event_template(payload_bytes)
       when is_integer(payload_bytes) and payload_bytes > 0 do
    %{
      type: "content",
      payload: %{text: String.duplicate("x", payload_bytes)},
      actor: "service:service",
      source: "benchmark",
      metadata: %{bench: true, scenario: @bench_scope},
      refs: %{},
      idempotency_key: nil,
      producer_id: "single-benchee-producer:bootstrap",
      producer_seq: 1
    }
  end

  defp next_shared_input_string_keys(template, %{
         pool_size: pool_size,
         producer_seqs: producer_seqs,
         worker_counter: worker_counter,
         producer_ids: producer_ids
       })
       when is_map(template) and is_integer(pool_size) and pool_size > 0 and
              is_tuple(producer_ids) do
    slot = worker_slot(pool_size, worker_counter)
    producer_seq = :atomics.add_get(producer_seqs, slot, 1)
    producer_id = elem(producer_ids, slot - 1)

    template
    |> Map.put("producer_id", producer_id)
    |> Map.put("producer_seq", producer_seq)
  end

  defp next_shared_input_atom_keys(template, %{
         pool_size: pool_size,
         producer_seqs: producer_seqs,
         worker_counter: worker_counter,
         producer_ids: producer_ids
       })
       when is_map(template) and is_integer(pool_size) and pool_size > 0 and
              is_tuple(producer_ids) do
    slot = worker_slot(pool_size, worker_counter)
    producer_seq = :atomics.add_get(producer_seqs, slot, 1)
    producer_id = elem(producer_ids, slot - 1)

    %{template | producer_id: producer_id, producer_seq: producer_seq}
  end

  defp append_event_controller_api!(session_id, input, %Context{} = auth)
       when is_binary(session_id) and session_id != "" and is_map(input) do
    params = Map.put(input, "id", session_id)

    case SessionController.append_api(auth, session_id, params) do
      {:ok, _reply} ->
        :ok

      {:error, reason} ->
        raise "controller append api failed: #{inspect(reason)}"

      {:timeout, leader} ->
        raise "controller append api timeout: #{inspect(leader)}"
    end
  end

  defp append_event_write_path!(session_id, input)
       when is_binary(session_id) and session_id != "" and is_map(input) do
    case WritePath.append_event(session_id, input, []) do
      {:ok, _reply} ->
        :ok

      {:error, reason} ->
        raise "write_path append failed: #{inspect(reason)}"

      {:timeout, leader} ->
        raise "write_path append timeout: #{inspect(leader)}"
    end
  end

  defp get_session_routed!(session_id) when is_binary(session_id) and session_id != "" do
    case ReadPath.get_session_routed(session_id) do
      {:ok, _session} ->
        :ok

      {:error, reason} ->
        raise "read_path get_session_routed failed: #{inspect(reason)}"

      {:timeout, leader} ->
        raise "read_path get_session_routed timeout: #{inspect(leader)}"
    end
  end

  defp replay_from_cursor!(session_id, cursor, limit)
       when is_binary(session_id) and session_id != "" and is_integer(cursor) and cursor >= 0 and
              is_integer(limit) and limit > 0 do
    case ReadPath.replay_from_cursor(session_id, cursor, limit, :raw) do
      {:ok, _events} ->
        :ok

      {:gap, gap_signal} ->
        raise "read_path replay_from_cursor gap: #{inspect(gap_signal)}"

      {:error, reason} ->
        raise "read_path replay_from_cursor failed: #{inspect(reason)}"
    end
  end

  defp controller_conn(path, %Context{} = auth) when is_binary(path) and path != "" do
    Plug.Test.conn("POST", path, "")
    |> Plug.Conn.assign(:auth, auth)
  end

  defp jwt_auth_context(session_id) when is_binary(session_id) and session_id != "" do
    {:ok, principal} = Principal.new("service", "single-benchee-jwt", :service)

    %Context{
      kind: :jwt,
      principal: principal,
      scopes: ["session:append", "session:read"],
      session_id: session_id,
      expires_at: System.system_time(:second) + 3600
    }
  end

  defp seed_read_path_session!(session_id) when is_binary(session_id) and session_id != "" do
    case WritePath.append_event(session_id, %{
           type: "content",
           payload: %{text: "seed"},
           actor: "service:service",
           source: "benchmark",
           metadata: %{bench: true, scenario: @bench_scope},
           refs: %{},
           idempotency_key: nil,
           producer_id: "#{session_id}:seed",
           producer_seq: 1
         }) do
      {:ok, _reply} ->
        :ok

      {:error, reason} ->
        raise "read_path seed append failed: #{inspect(reason)}"

      {:timeout, leader} ->
        raise "read_path seed append timeout: #{inspect(leader)}"
    end
  end

  defp worker_slot(pool_size, worker_counter) when is_integer(pool_size) and pool_size > 0 do
    key = :bench_single_session_worker_slot

    case Process.get(key) do
      slot when is_integer(slot) and slot >= 1 and slot <= pool_size ->
        slot

      _ ->
        slot = rem(:atomics.add_get(worker_counter, 1, 1) - 1, pool_size) + 1
        Process.put(key, slot)
        slot
    end
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
      nil ->
        default

      value ->
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
