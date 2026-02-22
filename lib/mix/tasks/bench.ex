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
    if Application.get_env(:starcite, :archive_adapter, Starcite.Archive.Adapter.S3) ==
         Starcite.Archive.Adapter.S3 do
      default_opts = [
        bucket: "starcite-archive",
        region: "us-east-1",
        endpoint: "http://127.0.0.1:9000",
        access_key_id: "minioadmin",
        secret_access_key: "minioadmin",
        path_style: true
      ]

      Application.put_env(
        :starcite,
        :archive_adapter_opts,
        Keyword.merge(default_opts, Application.get_env(:starcite, :archive_adapter_opts, []))
      )
    end
  end
end
