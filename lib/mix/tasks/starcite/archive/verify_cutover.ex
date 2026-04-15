defmodule Mix.Tasks.Starcite.Archive.VerifyCutover do
  @moduledoc """
  Verify that Postgres is ready to replace the legacy archive store.

  Cutover is considered ready only when:

  - every session's `archived_seq` matches the contiguous archived prefix in
    `events`
  - there are no archived event rows whose `session_id` is missing from the
    session catalog
  """

  use Mix.Task

  alias Starcite.Storage.EventArchive.Cutover

  @shortdoc "Verify Postgres archive cutover readiness"
  @default_limit 20
  @switches [limit: :integer]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, rest, invalid} = OptionParser.parse(args, strict: @switches)

    case {rest, invalid} do
      {[], []} ->
        run_verify(opts)

      _ ->
        Mix.raise(usage())
    end
  end

  defp run_verify(opts) when is_list(opts) do
    limit =
      case Keyword.get(opts, :limit, @default_limit) do
        value when is_integer(value) and value > 0 -> value
        _value -> Mix.raise(usage())
      end

    case Cutover.verify_cutover(limit: limit) do
      {:ok, summary} ->
        IO.puts(
          """
          cutover_ready total_sessions=#{summary.total_sessions} \
          archived_sessions=#{summary.archived_sessions} mismatch_sessions=0 orphan_sessions=0
          """
          |> String.replace("\n", "")
        )

      {:error, {:archive_cutover_blocked, summary}} ->
        IO.puts(
          """
          cutover_blocked total_sessions=#{summary.total_sessions} \
          archived_sessions=#{summary.archived_sessions} \
          mismatch_sessions=#{summary.mismatch_count} \
          orphan_sessions=#{summary.orphan_session_count} limit=#{limit}
          """
          |> String.replace("\n", "")
        )

        Enum.each(summary.mismatches, fn mismatch ->
          IO.puts(
            "mismatch session_id=#{mismatch.session_id} archived_seq=#{mismatch.archived_seq} contiguous_seq=#{mismatch.contiguous_seq}"
          )
        end)

        Enum.each(summary.orphan_sessions, fn orphan ->
          IO.puts(
            "orphan session_id=#{orphan.session_id} event_count=#{orphan.event_count} max_seq=#{orphan.max_seq}"
          )
        end)

        Mix.raise("archive cutover verification failed")

      {:error, reason} ->
        Mix.raise("failed to verify archive cutover: #{inspect(reason)}")
    end
  end

  defp usage do
    """
    usage:
      mix starcite.archive.verify_cutover [--limit #{@default_limit}]
    """
  end
end
