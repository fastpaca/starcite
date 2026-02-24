defmodule Mix.Tasks.Bench do
  use Mix.Task

  @shortdoc "Run Starcite benchmark scenarios"

  @moduledoc """
  Run Starcite benchmark scenarios from a canonical Mix entrypoint.

  Usage:

      mix bench
      mix bench hot-path
      mix bench routing
      mix bench internal
      mix bench k6
      mix bench --help
  """

  @impl Mix.Task
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args, strict: [help: :boolean], aliases: [h: :help])

    case invalid do
      [] -> :ok
      _ -> Mix.raise("invalid option(s): #{format_invalid(invalid)}")
    end

    if opts[:help] do
      Mix.shell().info(@moduledoc)
    else
      scenario = parse_scenario(rest)
      run_scenario(scenario)
    end
  end

  defp parse_scenario([]), do: :hot_path
  defp parse_scenario(["hot-path"]), do: :hot_path
  defp parse_scenario(["routing"]), do: :routing
  defp parse_scenario(["internal"]), do: :internal
  defp parse_scenario(["k6"]), do: :k6

  defp parse_scenario([value]) do
    Mix.raise("""
    unknown benchmark scenario: #{inspect(value)}
    expected one of: hot-path, routing, internal, k6
    """)
  end

  defp parse_scenario(values) do
    Mix.raise("expected at most one scenario argument, got: #{Enum.join(values, ", ")}")
  end

  defp run_scenario(:hot_path) do
    configure_local_archive_adapter()
    Mix.Tasks.Bench.HotPath.run()
  end

  defp run_scenario(:routing) do
    configure_local_archive_adapter()
    Mix.Tasks.Bench.Routing.run()
  end

  defp run_scenario(:internal) do
    configure_local_archive_adapter()
    Mix.Tasks.Bench.Internal.run()
  end

  defp run_scenario(:k6) do
    configure_local_archive_adapter()

    case System.find_executable("k6") do
      nil ->
        Mix.raise("""
        k6 executable not found in PATH.
        Run with Docker Compose instead:
          docker compose -f docker-compose.integration.yml -p <project> --profile tools run --rm \\
            k6 run /bench/k6-hot-path-throughput.js
        """)

      executable ->
        {_, status} =
          System.cmd(executable, ["run", "bench/k6-hot-path-throughput.js"],
            stderr_to_stdout: true,
            into: IO.stream(:stdio, :line)
          )

        if status != 0 do
          Mix.raise("k6 benchmark failed with exit status #{status}")
        end
    end
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {name, value} -> "#{name}=#{inspect(value)}" end)
    |> Enum.join(", ")
  end

  defp configure_local_archive_adapter do
    default_opts = [
      bucket: System.get_env("BENCH_S3_BUCKET", "starcite-archive"),
      region: System.get_env("BENCH_S3_REGION", "us-east-1"),
      endpoint: System.get_env("BENCH_S3_ENDPOINT", "http://127.0.0.1:9000"),
      access_key_id: System.get_env("BENCH_S3_ACCESS_KEY_ID", "minioadmin"),
      secret_access_key: System.get_env("BENCH_S3_SECRET_ACCESS_KEY", "minioadmin"),
      path_style: env_bool("BENCH_S3_PATH_STYLE", true)
    ]

    Application.put_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.S3)

    Application.put_env(
      :starcite,
      :archive_adapter_opts,
      Keyword.merge(default_opts, Application.get_env(:starcite, :archive_adapter_opts, []))
    )
  end

  defp env_bool(name, default) when is_binary(name) and is_boolean(default) do
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
          other -> Mix.raise("invalid boolean for #{name}: #{inspect(other)}")
        end
    end
  end
end
