defmodule Starcite.Repo.Migrations.AddProjectionStateToSessions do
  use Ecto.Migration

  def up do
    alter table(:sessions) do
      add :projection_state, :map, default: %{}, null: false
      add :projection_version, :bigint, default: 0, null: false
    end
  end

  def down do
    alter table(:sessions) do
      remove :projection_state
      remove :projection_version
    end
  end
end
