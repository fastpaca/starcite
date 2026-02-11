defmodule Starcite.Repo.Migrations.DropEventsSessionInsertedAtIndex do
  use Ecto.Migration

  def change do
    drop_if_exists(index(:events, [:session_id, :inserted_at], name: :events_session_id_inserted_at_index))
  end
end
