defmodule FleetLM.Repo.Migrations.RenameContextIdToConversationId do
  use Ecto.Migration

  def change do
    # Rename the column
    rename table(:messages), :context_id, to: :conversation_id

    # Drop the old index and create new one with correct name
    drop index(:messages, [:context_id, :inserted_at])
    create index(:messages, [:conversation_id, :inserted_at])
  end
end
