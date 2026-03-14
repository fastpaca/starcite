defmodule Mix.Tasks.Starcite.Archive.BackfillS3SessionOrder do
  use Mix.Task

  @shortdoc "Backfill S3 session-order indexes for fast session listing"
  @moduledoc """
  Backfill the ordered S3 session indexes used by fast `list_sessions` pagination.

  Usage:

      mix starcite.archive.backfill_s3_session_order
  """

  alias Starcite.Archive.Adapter.S3
  alias Starcite.Archive.Adapter.S3.Config

  @impl true
  def run(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args, strict: [help: :boolean], aliases: [h: :help])

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
      backfill()
    end
  end

  defp backfill do
    Mix.Task.run("loadpaths")
    Mix.Task.run("app.config")
    ensure_s3_client_apps_started!()

    archive_adapter = Application.get_env(:starcite, :archive_adapter, S3)

    if archive_adapter != S3 do
      Mix.raise("""
      S3 session-order backfill is only available when :archive_adapter is #{inspect(S3)}.
      Current adapter: #{inspect(archive_adapter)}
      """)
    end

    runtime_opts = Application.get_env(:starcite, :archive_adapter_opts, [])
    config = Config.build!(runtime_opts, [])

    case S3.backfill_session_order_indexes(config) do
      {:ok, stats} ->
        print_stats(stats)

      {:error, reason} ->
        Mix.raise("S3 session-order backfill failed: #{inspect(reason)}")
    end
  end

  defp ensure_s3_client_apps_started! do
    Enum.each([:req, :ex_aws_s3], fn app ->
      case Application.ensure_all_started(app) do
        {:ok, _started_apps} ->
          :ok

        {:error, reason} ->
          Mix.raise("failed to start #{app} for S3 session-order backfill: #{inspect(reason)}")
      end
    end)
  end

  defp print_stats(stats) when is_map(stats) do
    IO.puts("ready_already?=#{stats.ready_already?}")
    IO.puts("tenants=#{stats.tenants}")
    IO.puts("tenant_sessions=#{stats.tenant_sessions}")
    IO.puts("legacy_sessions=#{stats.legacy_sessions}")
    IO.puts("sessions_total=#{stats.sessions_total}")
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {name, value} -> "#{name}=#{inspect(value)}" end)
    |> Enum.join(", ")
  end
end
