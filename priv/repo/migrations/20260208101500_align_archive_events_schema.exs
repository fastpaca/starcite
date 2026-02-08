defmodule FleetLM.Repo.Migrations.AlignArchiveEventsSchema do
  use Ecto.Migration

  def up do
    rename table(:messages), to: table(:events)
    rename table(:events), :conversation_id, to: :session_id
    rename table(:events), :role, to: :type
    rename table(:events), :parts, to: :payload

    execute("DROP INDEX IF EXISTS messages_context_id_inserted_at_index")
    execute("DROP INDEX IF EXISTS messages_conversation_id_inserted_at_index")
    execute("DROP INDEX IF EXISTS events_conversation_id_inserted_at_index")

    execute("""
    UPDATE events
    SET payload =
      CASE
        WHEN payload IS NULL THEN '{}'::jsonb
        WHEN jsonb_typeof(payload) = 'array' AND jsonb_array_length(payload) > 0 THEN payload->0
        ELSE payload
      END
    """)

    alter table(:events) do
      add :actor, :text
      add :source, :text
      add :refs, :map, null: false, default: %{}
      add :idempotency_key, :text
    end

    execute("""
    UPDATE events
    SET actor = COALESCE(metadata->>'actor', 'unknown'),
        source = metadata->>'source',
        refs = COALESCE(metadata->'refs', '{}'::jsonb),
        idempotency_key = metadata->>'idempotency_key',
        metadata = metadata - 'actor' - 'source' - 'refs' - 'idempotency_key'
    """)

    execute("UPDATE events SET payload = '{}'::jsonb WHERE payload IS NULL")
    execute("UPDATE events SET metadata = '{}'::jsonb WHERE metadata IS NULL")
    execute("UPDATE events SET refs = '{}'::jsonb WHERE refs IS NULL")

    execute("ALTER TABLE events ALTER COLUMN actor SET NOT NULL")
    execute("ALTER TABLE events ALTER COLUMN payload SET NOT NULL")
    execute("ALTER TABLE events ALTER COLUMN metadata SET NOT NULL")
    execute("ALTER TABLE events ALTER COLUMN refs SET NOT NULL")

    alter table(:events) do
      remove :token_count
    end

    create index(:events, [:session_id, :inserted_at])
  end

  def down do
    execute("DROP INDEX IF EXISTS events_session_id_inserted_at_index")

    alter table(:events) do
      add :token_count, :integer, null: false, default: 0
      remove :idempotency_key
      remove :refs
      remove :source
      remove :actor
    end

    execute("""
    UPDATE events
    SET payload = jsonb_build_array(payload)
    """)

    rename table(:events), :payload, to: :parts
    rename table(:events), :type, to: :role
    rename table(:events), :session_id, to: :conversation_id
    rename table(:events), to: table(:messages)

    create index(:messages, [:conversation_id, :inserted_at])
  end
end
