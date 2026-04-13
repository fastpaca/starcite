defmodule Starcite.Storage.EventArchive.Cutover do
  @moduledoc """
  One-off archive import and cutover helpers.

  These functions exist for operator-run migration workflows and are not used by
  the runtime archive read/write path.
  """

  alias Starcite.Repo
  alias Starcite.Storage.EventArchive
  alias Starcite.Storage.EventArchive.LegacyS3Export
  alias Starcite.Storage.SessionCatalog

  @type import_summary :: %{
          required(:files) => non_neg_integer(),
          required(:rows) => non_neg_integer(),
          required(:inserted) => non_neg_integer()
        }

  @type reconcile_summary :: %{
          required(:dry_run) => boolean(),
          required(:total_sessions) => non_neg_integer(),
          required(:archived_sessions) => non_neg_integer(),
          required(:updated_sessions) => non_neg_integer(),
          required(:orphan_session_count) => non_neg_integer()
        }

  @type verify_mismatch :: %{
          required(:session_id) => String.t(),
          required(:archived_seq) => non_neg_integer(),
          required(:contiguous_seq) => non_neg_integer()
        }

  @type verify_orphan :: %{
          required(:session_id) => String.t(),
          required(:event_count) => non_neg_integer(),
          required(:max_seq) => non_neg_integer()
        }

  @type verify_summary :: %{
          required(:clean?) => boolean(),
          required(:total_sessions) => non_neg_integer(),
          required(:archived_sessions) => non_neg_integer(),
          required(:mismatch_count) => non_neg_integer(),
          required(:orphan_session_count) => non_neg_integer(),
          required(:mismatches) => [verify_mismatch()],
          required(:orphan_sessions) => [verify_orphan()]
        }

  @spec import_s3_export(String.t(), keyword()) :: {:ok, import_summary()} | {:error, term()}
  def import_s3_export(root, opts \\ [])
      when is_binary(root) and root != "" and is_list(opts) do
    dry_run? = Keyword.get(opts, :dry_run, false)

    case LegacyS3Export.exported_files(root) do
      {:ok, []} ->
        {:error, :archive_export_empty}

      {:ok, files} ->
        files
        |> Enum.reduce_while({:ok, zero_import_summary(), %{}}, fn file,
                                                                   {:ok, summary, tenant_cache} ->
          with {:ok, tenant_id, next_tenant_cache} <- resolve_session_tenant(file, tenant_cache),
               {:ok, rows} <- LegacyS3Export.read_file(file, tenant_id: tenant_id),
               {:ok, inserted} <- import_rows(rows, dry_run?) do
            {:cont,
             {:ok,
              %{
                files: summary.files + 1,
                rows: summary.rows + length(rows),
                inserted: summary.inserted + inserted
              }, next_tenant_cache}}
          else
            {:error, reason} ->
              {:halt, {:error, {:archive_import_failed, file.path, reason}}}
          end
        end)
        |> case do
          {:ok, summary, _tenant_cache} -> {:ok, summary}
          {:error, _reason} = error -> error
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec reconcile_progress(keyword()) :: {:ok, reconcile_summary()} | {:error, term()}
  def reconcile_progress(opts \\ []) when is_list(opts) do
    dry_run? = Keyword.get(opts, :dry_run, false)

    with {:ok, snapshot} <- build_verify_summary(0),
         {:ok, updated_sessions} <- maybe_reconcile_progress(dry_run?) do
      {:ok,
       %{
         dry_run: dry_run?,
         total_sessions: snapshot.total_sessions,
         archived_sessions: snapshot.archived_sessions,
         updated_sessions: updated_sessions,
         orphan_session_count: snapshot.orphan_session_count
       }}
    end
  end

  @spec verify_cutover(keyword()) :: {:ok, verify_summary()} | {:error, term()}
  def verify_cutover(opts \\ []) when is_list(opts) do
    limit =
      case Keyword.get(opts, :limit, 20) do
        value when is_integer(value) and value >= 0 -> value
        value -> raise ArgumentError, "invalid verify limit: #{inspect(value)}"
      end

    with {:ok, summary} <- build_verify_summary(limit) do
      if summary.clean? do
        {:ok, summary}
      else
        {:error, {:archive_cutover_blocked, summary}}
      end
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp zero_import_summary do
    %{
      files: 0,
      rows: 0,
      inserted: 0
    }
  end

  defp resolve_session_tenant(%{session_id: session_id} = file, tenant_cache)
       when is_binary(session_id) and session_id != "" and is_map(file) and is_map(tenant_cache) do
    case Map.fetch(tenant_cache, session_id) do
      {:ok, tenant_id} ->
        validate_session_tenant(file, session_id, tenant_id, tenant_cache)

      :error ->
        case SessionCatalog.get_tenant_id(session_id) do
          {:ok, tenant_id} ->
            validate_session_tenant(
              file,
              session_id,
              tenant_id,
              Map.put(tenant_cache, session_id, tenant_id)
            )

          {:error, reason} ->
            {:error, {:session_catalog_lookup_failed, session_id, reason}}
        end
    end
  end

  defp validate_session_tenant(file, session_id, tenant_id, tenant_cache)
       when is_map(file) and is_binary(session_id) and session_id != "" and
              is_binary(tenant_id) and tenant_id != "" and is_map(tenant_cache) do
    case Map.get(file, :tenant_id) do
      nil ->
        {:ok, tenant_id, tenant_cache}

      ^tenant_id ->
        {:ok, tenant_id, tenant_cache}

      export_tenant_id ->
        {:error, {:session_tenant_mismatch, session_id, export_tenant_id, tenant_id}}
    end
  end

  defp import_rows(_rows, true), do: {:ok, 0}
  defp import_rows([], false), do: {:ok, 0}

  defp import_rows(rows, false) when is_list(rows) do
    EventArchive.write_events_postgres(rows)
  end

  defp maybe_reconcile_progress(true), do: {:ok, 0}

  defp maybe_reconcile_progress(false) do
    case Repo.query(reconcile_progress_sql()) do
      {:ok, %{num_rows: updated_sessions}}
      when is_integer(updated_sessions) and updated_sessions >= 0 ->
        {:ok, updated_sessions}

      {:ok, _result} ->
        {:error, :archive_cutover_unavailable}

      {:error, _reason} ->
        {:error, :archive_cutover_unavailable}
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp build_verify_summary(limit) when is_integer(limit) and limit >= 0 do
    with {:ok, total_sessions, archived_sessions} <- load_session_counts(),
         {:ok, mismatch_count} <- load_mismatch_count(),
         {:ok, orphan_session_count} <- load_orphan_session_count(),
         {:ok, mismatches} <- load_mismatch_sample(limit),
         {:ok, orphan_sessions} <- load_orphan_sample(limit) do
      {:ok,
       %{
         clean?: mismatch_count == 0 and orphan_session_count == 0,
         total_sessions: total_sessions,
         archived_sessions: archived_sessions,
         mismatch_count: mismatch_count,
         orphan_session_count: orphan_session_count,
         mismatches: mismatches,
         orphan_sessions: orphan_sessions
       }}
    end
  end

  defp load_session_counts do
    sql = """
    #{frontier_cte_sql()}
    SELECT
      COUNT(*)::bigint AS total_sessions,
      COUNT(*) FILTER (WHERE COALESCE(f.contiguous_seq, 0) > 0)::bigint AS archived_sessions
    FROM sessions AS s
    LEFT JOIN frontier AS f
      ON f.session_id = s.id
    """

    case Repo.query(sql, []) do
      {:ok, %{rows: [[total_sessions, archived_sessions]]}} ->
        {:ok, to_non_neg_integer(total_sessions), to_non_neg_integer(archived_sessions)}

      {:error, _reason} ->
        {:error, :archive_cutover_unavailable}

      _other ->
        {:error, :archive_cutover_unavailable}
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp load_mismatch_count do
    sql = """
    #{frontier_cte_sql()}
    SELECT COUNT(*)::bigint
    FROM sessions AS s
    LEFT JOIN frontier AS f
      ON f.session_id = s.id
    WHERE s.archived_seq <> COALESCE(f.contiguous_seq, 0)
    """

    load_single_integer(sql)
  end

  defp load_orphan_session_count do
    sql = """
    SELECT COUNT(*)::bigint
    FROM (
      SELECT e.session_id
      FROM events AS e
      LEFT JOIN sessions AS s
        ON s.id = e.session_id
      WHERE s.id IS NULL
      GROUP BY e.session_id
    ) AS orphan_sessions
    """

    load_single_integer(sql)
  end

  defp load_mismatch_sample(0), do: {:ok, []}

  defp load_mismatch_sample(limit) when is_integer(limit) and limit > 0 do
    sql = """
    #{frontier_cte_sql()}
    SELECT
      s.id,
      s.archived_seq,
      COALESCE(f.contiguous_seq, 0) AS contiguous_seq
    FROM sessions AS s
    LEFT JOIN frontier AS f
      ON f.session_id = s.id
    WHERE s.archived_seq <> COALESCE(f.contiguous_seq, 0)
    ORDER BY s.id
    LIMIT $1
    """

    case Repo.query(sql, [limit]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [session_id, archived_seq, contiguous_seq] ->
           %{
             session_id: session_id,
             archived_seq: to_non_neg_integer(archived_seq),
             contiguous_seq: to_non_neg_integer(contiguous_seq)
           }
         end)}

      {:error, _reason} ->
        {:error, :archive_cutover_unavailable}
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp load_orphan_sample(0), do: {:ok, []}

  defp load_orphan_sample(limit) when is_integer(limit) and limit > 0 do
    sql = """
    SELECT
      e.session_id,
      COUNT(*)::bigint AS event_count,
      COALESCE(MAX(e.seq), 0)::bigint AS max_seq
    FROM events AS e
    LEFT JOIN sessions AS s
      ON s.id = e.session_id
    WHERE s.id IS NULL
    GROUP BY e.session_id
    ORDER BY e.session_id
    LIMIT $1
    """

    case Repo.query(sql, [limit]) do
      {:ok, %{rows: rows}} ->
        {:ok,
         Enum.map(rows, fn [session_id, event_count, max_seq] ->
           %{
             session_id: session_id,
             event_count: to_non_neg_integer(event_count),
             max_seq: to_non_neg_integer(max_seq)
           }
         end)}

      {:error, _reason} ->
        {:error, :archive_cutover_unavailable}
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp load_single_integer(sql) when is_binary(sql) and sql != "" do
    case Repo.query(sql, []) do
      {:ok, %{rows: [[value]]}} ->
        {:ok, to_non_neg_integer(value)}

      {:error, _reason} ->
        {:error, :archive_cutover_unavailable}
    end
  rescue
    _ -> {:error, :archive_cutover_unavailable}
  end

  defp to_non_neg_integer(value) when is_integer(value) and value >= 0, do: value

  defp to_non_neg_integer(value) do
    raise ArgumentError,
          "expected non-negative integer result from cutover query, got: #{inspect(value)}"
  end

  defp frontier_cte_sql do
    """
    WITH ordered AS (
      SELECT
        session_id,
        seq,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY seq) AS rn
      FROM events
    ),
    frontier AS (
      SELECT
        session_id,
        MAX(seq) AS contiguous_seq
      FROM ordered
      WHERE seq = rn
      GROUP BY session_id
    )
    """
  end

  defp reconcile_progress_sql do
    """
    #{frontier_cte_sql()}
    ,
    targets AS (
      SELECT
        s.id AS session_id,
        COALESCE(f.contiguous_seq, 0) AS contiguous_seq
      FROM sessions AS s
      LEFT JOIN frontier AS f
        ON f.session_id = s.id
    )
    UPDATE sessions AS s
    SET archived_seq = t.contiguous_seq
    FROM targets AS t
    WHERE s.id = t.session_id
      AND s.archived_seq IS DISTINCT FROM t.contiguous_seq
    """
  end
end
