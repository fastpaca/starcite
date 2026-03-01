defmodule Mix.Tasks.Starcite.Archive.MigrateS3Schema do
  use Mix.Task

  @shortdoc "Migrate S3 archive objects to current schema versions"
  @moduledoc """
  Migrate S3-backed archive object payloads to the current schema versions.

  Usage:

      mix starcite.archive.migrate_s3_schema
      mix starcite.archive.migrate_s3_schema --dry-run
  """

  alias Starcite.Archive.Adapter.S3
  alias Starcite.Archive.Adapter.S3.{Config, SchemaControl}

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
    Mix.Task.run("app.start")

    archive_adapter = Application.get_env(:starcite, :archive_adapter, S3)

    if archive_adapter != S3 do
      Mix.raise("""
      S3 schema migration is only available when :archive_adapter is #{inspect(S3)}.
      Current adapter: #{inspect(archive_adapter)}
      """)
    end

    runtime_opts = Application.get_env(:starcite, :archive_adapter_opts, [])
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

  defp print_stats(stats) when is_map(stats) do
    IO.puts("dry_run=#{stats.dry_run}")
    IO.puts("index_scanned=#{stats.index_scanned}")
    IO.puts("index_migrations_needed=#{stats.index_migrations_needed}")
    IO.puts("index_rewritten=#{stats.index_rewritten}")
    IO.puts("session_scanned=#{stats.session_scanned}")
    IO.puts("session_migrations_needed=#{stats.session_migrations_needed}")
    IO.puts("session_rewritten=#{stats.session_rewritten}")
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
