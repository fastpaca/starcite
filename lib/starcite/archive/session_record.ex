defmodule Starcite.Archive.SessionRecord do
  @moduledoc """
  Ecto schema for archived session catalog entries.
  """

  use Ecto.Schema

  @primary_key {:id, :string, autogenerate: false}
  schema "sessions" do
    field(:title, :string)
    field(:metadata, :map)
    field(:created_at, :utc_datetime)
    field(:updated_at, :utc_datetime)
  end
end
