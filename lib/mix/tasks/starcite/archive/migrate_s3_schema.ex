defmodule Mix.Tasks.Starcite.Archive.MigrateS3Schema do
  use Mix.Task

  @shortdoc "Migrate S3 archive objects to current schema versions"
  @moduledoc """
  Migrate S3-backed archive object payloads to the current schema versions.

  Usage:

      mix starcite.archive.migrate_s3_schema
      mix starcite.archive.migrate_s3_schema --dry-run
  """

  alias Starcite.Storage.EventArchive.S3.{Config, SchemaControl}

  @impl true
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args, strict: [dry_run: :boolean, help: :boolean], aliases: [h: :help])

    case invalid do
      [] -> :ok
      _ -> Mix.raise("invalid option(s): #{format_invalid(invalid)}")
    end

    case rest do
      [] -> :ok
      _ -> Mix.raise("unexpected argument(s): #{Enum.join(rest, " ")}")
    end

    if opts[:help] do
      Mix.shell().info(@moduledoc)
    else
      migrate(opts)
    end
  end

  defp migrate(opts) when is_list(opts) do
    Mix.Task.run("loadpaths")
    Mix.Task.run("app.config")
    ensure_s3_client_apps_started!()

    runtime_opts = Application.get_env(:starcite, :event_archive_opts, [])
    config = Config.build!(runtime_opts, [])

    case SchemaControl.migrate(config,
           dry_run: Keyword.get(opts, :dry_run, false),
           actor: "manual"
         ) do
      {:ok, stats} ->
        print_stats(stats)

      {:error, reason} ->
        Mix.raise("S3 schema migration failed: #{inspect(reason)}")
    end
  end

  defp ensure_s3_client_apps_started! do
    Enum.each([:req, :ex_aws_s3], fn app ->
      case Application.ensure_all_started(app) do
        {:ok, _started_apps} ->
          :ok

        {:error, reason} ->
          Mix.raise("failed to start #{app} for S3 schema migration: #{inspect(reason)}")
      end
    end)
  end

  defp print_stats(stats) when is_map(stats) do
    IO.puts("dry_run=#{stats.dry_run}")
    IO.puts("event_scanned=#{stats.event_scanned}")
    IO.puts("event_migrations_needed=#{stats.event_migrations_needed}")
    IO.puts("event_rewritten=#{stats.event_rewritten}")
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {name, value} -> "#{name}=#{inspect(value)}" end)
    |> Enum.join(", ")
  end
end
