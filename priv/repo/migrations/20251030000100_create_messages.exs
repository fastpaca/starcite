defmodule FleetLM.Repo.Migrations.CreateMessages do
  use Ecto.Migration

  def change do
    create table(:messages, primary_key: false) do
      add :context_id, :text, primary_key: true, null: false
      add :seq, :bigint, primary_key: true, null: false
      add :role, :text, null: false
      add :parts, :map, null: false
      add :metadata, :map, null: false
      add :token_count, :integer, null: false
      add :inserted_at, :utc_datetime, null: false
    end

    create index(:messages, [:context_id, :inserted_at])
  end
end

