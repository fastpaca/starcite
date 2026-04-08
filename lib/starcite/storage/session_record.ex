defmodule Starcite.Storage.SessionRecord do
  @moduledoc """
  Ecto schema for durable session catalog entries.
  """

  use Ecto.Schema

  @primary_key {:id, :string, autogenerate: false}
  schema "sessions" do
    field(:title, :string)
    field(:tenant_id, :string)
    field(:creator_id, :string)
    field(:creator_type, :string)
    field(:metadata, :map)
    field(:archived_seq, :integer)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime_usec)
    field(:version, :integer)
  end
end
