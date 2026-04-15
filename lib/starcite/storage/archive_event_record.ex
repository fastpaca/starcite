defmodule Starcite.Storage.ArchiveEventRecord do
  @moduledoc """
  Ecto schema for durable archived event rows.
  """

  use Ecto.Schema

  @primary_key false
  schema "events" do
    field(:session_id, :string)
    field(:seq, :integer)
    field(:type, :string)
    field(:payload, :map)
    field(:actor, :string)
    field(:producer_id, :string)
    field(:producer_seq, :integer)
    field(:tenant_id, :string)
    field(:source, :string)
    field(:metadata, :map)
    field(:refs, :map)
    field(:idempotency_key, :string)
    field(:inserted_at, :utc_datetime)
  end
end
