defmodule Mix.Tasks.Starcite.Archive.ImportS3Export do
  @moduledoc """
  Import archived event chunks from a legacy S3 export directory into Postgres.

  This task only imports archive rows. It does not update `sessions.archived_seq`
  or verify cutover readiness. Use the dedicated reconcile and verify tasks for
  those steps.
  """

  use Mix.Task

  alias Starcite.Storage.EventArchive.Cutover

  @shortdoc "Import legacy S3 archive exports into Postgres"
  @switches [root: :string, dry_run: :boolean]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {rest, invalid} do
      {[], []} ->
        run_import(opts)

      _ ->
        Mix.raise(usage())
    end
  end

  defp run_import(opts) when is_list(opts) do
    root =
      case Keyword.get(opts, :root) do
        value when is_binary(value) and value != "" -> value
        _ -> Mix.raise(usage())
      end

    dry_run? = Keyword.get(opts, :dry_run, false)

    case Cutover.import_s3_export(root, dry_run: dry_run?) do
      {:ok, summary} ->
        IO.puts(
          """
          import_complete files=#{summary.files} rows=#{summary.rows} inserted=#{summary.inserted} \
          dry_run=#{dry_run?}
          """
          |> String.replace("\n", "")
        )

      {:error, :archive_export_empty} ->
        Mix.raise("no legacy archive export files found under #{inspect(root)}")

      {:error, :archive_export_root_missing} ->
        Mix.raise("archive export root does not exist: #{inspect(root)}")

      {:error, {:archive_import_failed, path, reason}} ->
        Mix.raise("failed to import #{path}: #{inspect(reason)}")

      {:error, reason} ->
        Mix.raise("failed to import legacy archive export #{inspect(root)}: #{inspect(reason)}")
    end
  end

  defp usage do
    """
    usage:
      mix starcite.archive.import_s3_export --root /path/to/export [--dry-run]
    """
  end
end
