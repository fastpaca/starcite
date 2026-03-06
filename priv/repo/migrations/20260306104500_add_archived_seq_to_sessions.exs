defmodule Starcite.Repo.Migrations.AddArchivedSeqToSessions do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS archived_seq bigint DEFAULT 0")

    execute("""
    UPDATE sessions
    SET archived_seq = COALESCE(
      NULLIF(metadata->'__starcite_runtime_v1'->>'archived_seq', '')::bigint,
      archived_seq,
      0
    )
    """)

    execute("""
    UPDATE sessions
    SET metadata = CASE
      WHEN jsonb_typeof(metadata) = 'object' THEN metadata - '__starcite_runtime_v1'
      ELSE metadata
    END
    WHERE metadata ? '__starcite_runtime_v1'
    """)

    execute("ALTER TABLE sessions ALTER COLUMN archived_seq SET DEFAULT 0")
    execute("ALTER TABLE sessions ALTER COLUMN archived_seq SET NOT NULL")
  end

  def down do
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS archived_seq")
  end
end
