defmodule Starcite.Repo.Migrations.AddTenantIdToArchiveEvents do
  use Ecto.Migration

  def up do
    alter table(:events) do
      add(:tenant_id, :text)
    end

    execute("""
    UPDATE events AS e
    SET tenant_id = COALESCE(
      NULLIF(s.tenant_id, ''),
      NULLIF(e.metadata->'starcite_principal'->>'tenant_id', ''),
      NULLIF(e.metadata->>'tenant_id', ''),
      'service'
    )
    FROM sessions AS s
    WHERE e.session_id = s.id
      AND (e.tenant_id IS NULL OR e.tenant_id = '')
    """)

    execute("""
    UPDATE events AS e
    SET tenant_id = COALESCE(
      NULLIF(e.metadata->'starcite_principal'->>'tenant_id', ''),
      NULLIF(e.metadata->>'tenant_id', ''),
      'service'
    )
    WHERE e.tenant_id IS NULL OR e.tenant_id = ''
    """)

    alter table(:events) do
      modify(:tenant_id, :text, null: false)
    end

    create(index(:events, [:tenant_id, :session_id, :seq]))
    create(constraint(:events, :events_tenant_id_not_empty, check: "tenant_id <> ''"))
  end

  def down do
    drop(constraint(:events, :events_tenant_id_not_empty))
    drop(index(:events, [:tenant_id, :session_id, :seq]))

    alter table(:events) do
      remove(:tenant_id)
    end
  end
end
