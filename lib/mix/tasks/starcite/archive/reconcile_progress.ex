defmodule Mix.Tasks.Starcite.Archive.ReconcileProgress do
  @moduledoc """
  Recompute `sessions.archived_seq` from contiguous archived rows in Postgres.

  This is a one-off cutover task intended to run after archive rows have been
  imported. It does not verify cutover readiness; use the dedicated verify task
  after reconciliation.
  """

  use Mix.Task

  alias Starcite.Storage.EventArchive.Cutover

  @shortdoc "Recompute archived_seq from imported archive rows"
  @switches [dry_run: :boolean]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {rest, invalid} do
      {[], []} ->
        run_reconcile(opts)

      _ ->
        Mix.raise(usage())
    end
  end

  defp run_reconcile(opts) when is_list(opts) do
    dry_run? = Keyword.get(opts, :dry_run, false)

    case Cutover.reconcile_progress(dry_run: dry_run?) do
      {:ok, summary} ->
        IO.puts(
          """
          reconcile_complete total_sessions=#{summary.total_sessions} \
          archived_sessions=#{summary.archived_sessions} \
          updated_sessions=#{summary.updated_sessions} \
          orphan_sessions=#{summary.orphan_session_count} dry_run=#{summary.dry_run}
          """
          |> String.replace("\n", "")
        )

      {:error, reason} ->
        Mix.raise("failed to reconcile archived progress: #{inspect(reason)}")
    end
  end

  defp usage do
    """
    usage:
      mix starcite.archive.reconcile_progress [--dry-run]
    """
  end
end
