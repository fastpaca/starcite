defmodule Starcite.Repo.Migrations.AddProducerFieldsToArchiveEvents do
  use Ecto.Migration

  def change do
    alter table(:events) do
      add :producer_id, :text
      add :producer_seq, :bigint
    end

    create_if_not_exists index(:events, [:session_id, :producer_id, :producer_seq])
  end
end
