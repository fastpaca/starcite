defmodule Starcite.Repo.Migrations.AddSessionsTenantIdIdIndex do
  use Ecto.Migration

  def change do
    create_if_not_exists(index(:sessions, [:tenant_id, :id]))
  end
end
