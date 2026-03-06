defmodule Starcite.Repo.Migrations.AddArchivedSeqToSessions do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS archived_seq bigint DEFAULT 0")

    execute("""
    UPDATE sessions AS s
    SET archived_seq = e.max_seq
    FROM (
      SELECT session_id, MAX(seq) AS max_seq
      FROM events
      GROUP BY session_id
    ) AS e
    WHERE s.id = e.session_id
    """)

    execute("UPDATE sessions SET archived_seq = 0 WHERE archived_seq IS NULL")

    execute("ALTER TABLE sessions ALTER COLUMN archived_seq SET DEFAULT 0")
    execute("ALTER TABLE sessions ALTER COLUMN archived_seq SET NOT NULL")
  end

  def down do
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS archived_seq")
  end
end
