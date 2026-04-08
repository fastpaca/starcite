defmodule Starcite.Repo.Migrations.AddVersionToSessions do
  use Ecto.Migration

  def up do
    execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS version bigint")

    execute("""
    UPDATE sessions
    SET version = 1
    WHERE version IS NULL
    """)

    execute("ALTER TABLE sessions ALTER COLUMN version SET NOT NULL")
  end

  def down do
    execute("ALTER TABLE sessions DROP COLUMN IF EXISTS version")
  end
end
