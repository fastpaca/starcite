defmodule Starcite.Repo.Migrations.CreateSessionsCatalog do
  use Ecto.Migration

  def change do
    create_if_not_exists table(:sessions, primary_key: false) do
      add :id, :text, primary_key: true, null: false
      add :title, :text
      add :metadata, :map, null: false, default: %{}
      add :created_at, :utc_datetime, null: false
    end

    create_if_not_exists index(:sessions, [:metadata], using: :gin)
  end
end
