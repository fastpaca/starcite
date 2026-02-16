defmodule Starcite.Repo.Migrations.AddSessionAuthColumns do
  use Ecto.Migration

  def change do
    alter table(:sessions) do
      add(:creator_principal, :map)
    end
  end
end
