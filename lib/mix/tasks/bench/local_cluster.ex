defmodule Mix.Tasks.Bench.LocalCluster do
  use Mix.Task

  alias Starcite.Bench.LocalClusterReport

  @shortdoc "Run a repeatable local Docker cluster benchmark and print a summary"

  @moduledoc """
  Runs a repeatable local cluster benchmark against the current branch using
  Docker Compose, the committed benchmark corpus, k6, and sampled container
  memory.

  This task:

    * starts a temporary 3-node cluster plus Postgres and MinIO
    * runs the hot-path k6 benchmark with JSON summary export
    * samples `docker stats` while the benchmark runs
    * writes raw artifacts under `tmp/potato-bench/...`
    * prints a compact summary including throughput, p95, p99, p99.9, and RSS
    * tears the cluster down unless `--keep-cluster` is set

  Usage:

      mix bench.local_cluster
      mix bench.local_cluster --rate 2500 --duration 30s --session-count 256
      mix bench.local_cluster --keep-cluster

  Options:

    * `--rate` - offered append rate for constant-arrival-rate k6. Default `1200`.
    * `--duration` - k6 steady-state duration. Default `15s`.
    * `--session-count` - number of sessions created in setup. Default `64`.
    * `--producers-per-vu` - producers per k6 VU. Default `2`.
    * `--pipeline-depth` - appends per iteration. Default `1`.
    * `--max-vus` - k6 `maxVUs`. Default `32`.
    * `--pre-allocated-vus` - k6 `preAllocatedVUs`. Default `8`.
    * `--stats-interval-ms` - `docker stats` sample interval. Default `2000`.
    * `--compose-file` - Compose file path. Default `docker-compose.integration.yml`.
    * `--corpus-file` - k6 corpus file path under `bench/`. Default `bench/corpus/default.json`.
    * `--output-dir` - host artifact directory under `tmp/`. Default `tmp/potato-bench/<project>`.
    * `--project` - Compose project name. Default `starcite-bench-<timestamp>`.
    * `--skip-build` - reuse the existing image instead of rebuilding.
    * `--keep-cluster` - skip `docker compose down` for debugging.
  """

  @switches [
    help: :boolean,
    rate: :integer,
    duration: :string,
    session_count: :integer,
    producers_per_vu: :integer,
    pipeline_depth: :integer,
    max_vus: :integer,
    pre_allocated_vus: :integer,
    stats_interval_ms: :integer,
    compose_file: :string,
    corpus_file: :string,
    output_dir: :string,
    project: :string,
    skip_build: :boolean,
    keep_cluster: :boolean
  ]

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {invalid, rest, Keyword.get(opts, :help, false)} do
      {_, _, true} ->
        Mix.shell().info(@moduledoc)

      {[], [], false} ->
        config = build_config(opts)
        File.mkdir_p!(config.output_dir)

        Mix.shell().info("running local cluster benchmark in #{config.output_dir}")
        run_benchmark!(config)

      {[], _rest, false} ->
        Mix.raise("unexpected positional arguments: #{Enum.join(rest, ", ")}")

      {_invalid, _rest, false} ->
        Mix.raise("invalid option(s): #{format_invalid(invalid)}")
    end
  end

  defp run_benchmark!(config) when is_map(config) do
    try do
      up_cluster!(config)
      sampler_pid = start_stats_sampler(config)

      try do
        run_k6!(config)
      after
        stop_stats_sampler(sampler_pid)
      end

      report =
        config
        |> report_metadata()
        |> LocalClusterReport.build(
          LocalClusterReport.read_k6_summary!(config.k6_summary_path),
          File.read!(config.docker_stats_path)
        )

      :ok = LocalClusterReport.write_report!(report, config.report_path)
      :ok = LocalClusterReport.write_summary!(report, config.report_summary_path)
      Mix.shell().info(LocalClusterReport.format(report))
    after
      maybe_down_cluster(config)
    end
  end

  defp build_config(opts) when is_list(opts) do
    compose_project =
      Keyword.get_lazy(opts, :project, fn ->
        "starcite-bench-#{System.unique_integer([:positive])}"
      end)

    compose_file = Path.expand(Keyword.get(opts, :compose_file, "docker-compose.integration.yml"))
    corpus_file = Path.expand(Keyword.get(opts, :corpus_file, "bench/corpus/default.json"))

    output_dir =
      opts
      |> Keyword.get_lazy(:output_dir, fn ->
        Path.join(["tmp", "potato-bench", compose_project]) |> Path.expand()
      end)
      |> Path.expand()

    ensure_file_exists!(compose_file, "compose_file")
    ensure_file_exists!(corpus_file, "corpus_file")

    container_corpus_file = bench_container_path!(corpus_file)
    results_dir = results_container_path!(output_dir)

    %{
      compose_file: compose_file,
      compose_project: compose_project,
      corpus_file: corpus_file,
      container_corpus_file: container_corpus_file,
      duration: Keyword.get(opts, :duration, "15s"),
      docker_stats_path: Path.join(output_dir, "docker-stats.txt"),
      keep_cluster: Keyword.get(opts, :keep_cluster, false),
      k6_output_path: Path.join(output_dir, "k6-output.txt"),
      k6_summary_path: Path.join(output_dir, "k6-summary.json"),
      max_vus: positive_integer_opt!(opts, :max_vus, 32),
      offered_rate: positive_integer_opt!(opts, :rate, 1200),
      output_dir: output_dir,
      pipeline_depth: positive_integer_opt!(opts, :pipeline_depth, 1),
      pre_allocated_vus: positive_integer_opt!(opts, :pre_allocated_vus, 8),
      producers_per_vu: positive_integer_opt!(opts, :producers_per_vu, 2),
      report_path: Path.join(output_dir, "report.json"),
      report_summary_path: Path.join(output_dir, "report.txt"),
      results_dir: results_dir,
      session_count: positive_integer_opt!(opts, :session_count, 64),
      skip_build: Keyword.get(opts, :skip_build, false),
      stats_interval_ms: positive_integer_opt!(opts, :stats_interval_ms, 2000)
    }
  end

  defp report_metadata(config) when is_map(config) do
    %{
      compose_project: config.compose_project,
      duration: config.duration,
      docker_stats_path: config.docker_stats_path,
      k6_output_path: config.k6_output_path,
      k6_summary_path: config.k6_summary_path,
      offered_rate: config.offered_rate,
      output_dir: config.output_dir,
      pipeline_depth: config.pipeline_depth,
      producers_per_vu: config.producers_per_vu,
      report_path: config.report_path,
      session_count: config.session_count
    }
  end

  defp up_cluster!(config) when is_map(config) do
    Mix.shell().info("starting docker compose cluster #{config.compose_project}")

    args =
      ["compose", "-f", config.compose_file, "-p", config.compose_project, "up", "-d"] ++
        build_args(config)

    _output = system_cmd!("docker", args)
    :ok
  end

  defp maybe_down_cluster(%{keep_cluster: true} = config) do
    Mix.shell().info("keeping cluster #{config.compose_project} running")
  end

  defp maybe_down_cluster(config) when is_map(config) do
    Mix.shell().info("tearing down docker compose cluster #{config.compose_project}")

    _output =
      system_cmd_no_raise("docker", [
        "compose",
        "-f",
        config.compose_file,
        "-p",
        config.compose_project,
        "down",
        "-v",
        "--remove-orphans"
      ])

    :ok
  end

  defp build_args(%{skip_build: true}), do: []
  defp build_args(_config), do: ["--build"]

  defp start_stats_sampler(config) when is_map(config) do
    File.write!(config.docker_stats_path, "")

    spawn_link(fn ->
      stats_sampler_loop(
        container_names(config.compose_project),
        config.docker_stats_path,
        config.stats_interval_ms
      )
    end)
  end

  defp stop_stats_sampler(pid) when is_pid(pid) do
    ref = make_ref()
    send(pid, {:stop, self(), ref})

    receive do
      {:stats_sampler_stopped, ^ref} ->
        :ok
    after
      5_000 ->
        Process.exit(pid, :kill)
        :ok
    end
  end

  defp stats_sampler_loop(containers, path, interval_ms)
       when is_list(containers) and is_binary(path) and is_integer(interval_ms) do
    append_stats(containers, path)

    receive do
      {:stop, caller, ref} ->
        send(caller, {:stats_sampler_stopped, ref})
    after
      interval_ms ->
        stats_sampler_loop(containers, path, interval_ms)
    end
  end

  defp append_stats(containers, path) when is_list(containers) and is_binary(path) do
    case System.cmd(
           "docker",
           ["stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"] ++
             containers,
           stderr_to_stdout: true
         ) do
      {output, 0} ->
        File.write!(path, output, [:append])

      {_output, _status} ->
        :ok
    end
  end

  defp run_k6!(config) when is_map(config) do
    Mix.shell().info("running k6 hot-path benchmark")

    {output, status} =
      System.cmd(
        "docker",
        [
          "compose",
          "-f",
          config.compose_file,
          "-p",
          config.compose_project,
          "--profile",
          "tools",
          "run",
          "--rm"
        ] ++
          k6_env_args(config) ++
          [
            "k6",
            "run",
            "--summary-export",
            Path.join(config.results_dir, "k6-summary.json"),
            "/bench/k6-hot-path-throughput.js"
          ],
        stderr_to_stdout: true
      )

    File.write!(config.k6_output_path, output)

    if status != 0 do
      Mix.raise("""
      k6 benchmark failed with exit status #{status}
      raw output: #{config.k6_output_path}
      """)
    end

    unless File.exists?(config.k6_summary_path) do
      Mix.raise("k6 summary export was not written to #{config.k6_summary_path}")
    end

    :ok
  end

  defp k6_env_args(config) when is_map(config) do
    [
      "-e",
      "EXECUTOR=constant-arrival-rate",
      "-e",
      "RATE=#{config.offered_rate}",
      "-e",
      "DURATION=#{config.duration}",
      "-e",
      "SESSION_COUNT=#{config.session_count}",
      "-e",
      "PRODUCERS_PER_VU=#{config.producers_per_vu}",
      "-e",
      "PIPELINE_DEPTH=#{config.pipeline_depth}",
      "-e",
      "MAX_VUS=#{config.max_vus}",
      "-e",
      "PRE_ALLOCATED_VUS=#{config.pre_allocated_vus}",
      "-e",
      "BENCH_EVENT_CORPUS_FILE=#{config.container_corpus_file}"
    ]
  end

  defp container_names(project) when is_binary(project) do
    [
      "#{project}-node1-1",
      "#{project}-node2-1",
      "#{project}-node3-1",
      "#{project}-minio-1",
      "#{project}-db-1"
    ]
  end

  defp bench_container_path!(path) when is_binary(path) do
    bench_root = Path.expand("bench")
    assert_under_root!(path, bench_root, "corpus_file must live under bench/")
    Path.join("/bench", Path.relative_to(path, bench_root))
  end

  defp results_container_path!(path) when is_binary(path) do
    tmp_root = Path.expand("tmp")
    assert_under_root!(path, tmp_root, "output_dir must live under tmp/")
    Path.join("/results", Path.relative_to(path, tmp_root))
  end

  defp assert_under_root!(path, root, message)
       when is_binary(path) and is_binary(root) and is_binary(message) do
    cond do
      path == root ->
        :ok

      String.starts_with?(path, root <> "/") ->
        :ok

      true ->
        Mix.raise("#{message}: #{path}")
    end
  end

  defp ensure_file_exists!(path, label) when is_binary(path) and is_binary(label) do
    unless File.exists?(path) do
      Mix.raise("missing #{label}: #{path}")
    end
  end

  defp system_cmd!(program, args) when is_binary(program) and is_list(args) do
    case System.cmd(program, args, stderr_to_stdout: true) do
      {output, 0} ->
        output

      {output, status} ->
        Mix.raise("""
        command failed: #{program} #{Enum.join(args, " ")}
        exit_status=#{status}
        output:
        #{output}
        """)
    end
  end

  defp system_cmd_no_raise(program, args) when is_binary(program) and is_list(args) do
    case System.cmd(program, args, stderr_to_stdout: true) do
      {output, _status} -> output
    end
  end

  defp positive_integer_opt!(opts, key, default)
       when is_list(opts) and is_atom(key) and is_integer(default) and default > 0 do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 ->
        value

      value ->
        Mix.raise(
          "invalid #{inspect(key)} for local cluster benchmark: #{inspect(value)} (expected positive integer)"
        )
    end
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {name, value} -> "#{name}=#{inspect(value)}" end)
    |> Enum.join(", ")
  end
end
