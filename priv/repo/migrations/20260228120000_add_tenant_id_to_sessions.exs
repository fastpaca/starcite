defmodule Starcite.Repo.Migrations.AddTenantIdToSessions do
  use Ecto.Migration

  def up do
    alter table(:sessions) do
      add(:tenant_id, :text)
    end

    execute("""
    UPDATE sessions
    SET tenant_id = COALESCE(
      NULLIF(creator_principal->>'tenant_id', ''),
      NULLIF(metadata->>'tenant_id', ''),
      'service'
    )
    WHERE tenant_id IS NULL OR tenant_id = ''
    """)

    alter table(:sessions) do
      modify(:tenant_id, :text, null: false)
    end

    create(index(:sessions, [:tenant_id]))
  end

  def down do
    drop(index(:sessions, [:tenant_id]))

    alter table(:sessions) do
      remove(:tenant_id)
    end
  end
end
