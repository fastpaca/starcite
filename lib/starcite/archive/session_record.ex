defmodule Starcite.Archive.SessionRecord do
  @moduledoc """
  Ecto schema for archived session catalog entries.
  """

  use Ecto.Schema

  @primary_key {:id, :string, autogenerate: false}
  schema "sessions" do
    field(:title, :string)
    field(:tenant_id, :string)
    field(:creator_principal, :map)
    field(:metadata, :map)
    field(:last_seq, :integer)
    field(:archived_seq, :integer)
    field(:retention, :map)
    field(:producer_cursors, :map)
    field(:last_progress_poll, :integer)
    field(:snapshot_version, :string)
    field(:created_at, :utc_datetime)
  end
end
