defmodule Starcite.Repo.Migrations.ReplaceSessionCreatorPrincipalWithCreatorColumns do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS creator_id text")
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS creator_type text")

    execute("""
    UPDATE sessions
    SET creator_id = NULLIF(creator_principal->>'id', ''),
        creator_type = NULLIF(creator_principal->>'type', '')
    WHERE creator_principal IS NOT NULL
      AND (creator_id IS NULL OR creator_type IS NULL)
    """)

    execute("""
    CREATE INDEX IF NOT EXISTS sessions_tenant_id_creator_id_id_index
    ON sessions (tenant_id, creator_id, id)
    WHERE creator_id IS NOT NULL
    """)

    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS creator_principal")
  end

  def down do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS creator_principal jsonb")

    execute("""
    UPDATE sessions
    SET creator_principal = CASE
      WHEN creator_id IS NOT NULL AND creator_type IS NOT NULL THEN
        jsonb_build_object('tenant_id', tenant_id, 'id', creator_id, 'type', creator_type)
      ELSE NULL
    END
    """)

    execute("DROP INDEX IF EXISTS sessions_tenant_id_creator_id_id_index")
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS creator_id")
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS creator_type")
  end
end
