defmodule Starcite.Repo.Migrations.AddArchivedToSessions do
  use Ecto.Migration

  def up do
    execute("""
    ALTER TABLE sessions
    ADD COLUMN IF NOT EXISTS archived boolean NOT NULL DEFAULT false
    """)

    execute("""
    CREATE INDEX IF NOT EXISTS sessions_tenant_id_archived_id_index
    ON sessions (tenant_id, archived, id)
    """)
  end

  def down do
    execute("DROP INDEX IF EXISTS sessions_tenant_id_archived_id_index")
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS archived")
  end
end
