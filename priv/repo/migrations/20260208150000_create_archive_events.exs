defmodule Starcite.Repo.Migrations.CreateArchiveEvents do
  use Ecto.Migration

  def change do
    create_if_not_exists table(:events, primary_key: false) do
      add :session_id, :text, primary_key: true, null: false
      add :seq, :bigint, primary_key: true, null: false
      add :type, :text, null: false
      add :payload, :map, null: false
      add :actor, :text, null: false
      add :source, :text
      add :metadata, :map, null: false, default: %{}
      add :refs, :map, null: false, default: %{}
      add :idempotency_key, :text
      add :inserted_at, :utc_datetime, null: false
    end

    create_if_not_exists index(:events, [:session_id, :inserted_at])
  end
end
