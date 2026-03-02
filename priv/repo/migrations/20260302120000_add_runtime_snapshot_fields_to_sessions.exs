defmodule Starcite.Repo.Migrations.AddRuntimeSnapshotFieldsToSessions do
  use Ecto.Migration

  def up do
    alter table(:sessions) do
      add_if_not_exists(:last_seq, :bigint, null: false, default: 0)
      add_if_not_exists(:archived_seq, :bigint, null: false, default: 0)
      add_if_not_exists(:retention, :map, null: false, default: %{})
      add_if_not_exists(:producer_cursors, :map, null: false, default: %{})
      add_if_not_exists(:last_progress_poll, :bigint, null: false, default: 0)
      add_if_not_exists(:snapshot_version, :text)
    end
  end

  def down do
    alter table(:sessions) do
      remove_if_exists(:snapshot_version)
      remove_if_exists(:last_progress_poll)
      remove_if_exists(:producer_cursors)
      remove_if_exists(:retention)
      remove_if_exists(:archived_seq)
      remove_if_exists(:last_seq)
    end
  end
end
