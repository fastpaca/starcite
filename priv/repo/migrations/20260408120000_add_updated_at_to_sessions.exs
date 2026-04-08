defmodule Starcite.Repo.Migrations.AddUpdatedAtToSessions do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS updated_at timestamptz")

    execute("""
    UPDATE sessions
    SET updated_at = created_at
    WHERE updated_at IS NULL
    """)

    execute("ALTER TABLE sessions ALTER COLUMN updated_at SET NOT NULL")
  end

  def down do
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS updated_at")
  end
end
