defmodule Starcite.Archive.Event do
  @moduledoc """
  Ecto schema for archived session events.
  """

  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  schema "events" do
    field(:session_id, :string, primary_key: true)
    field(:seq, :integer, primary_key: true)
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

  def changeset(event, attrs) do
    event
    |> cast(attrs, [
      :session_id,
      :seq,
      :type,
      :payload,
      :actor,
      :producer_id,
      :producer_seq,
      :tenant_id,
      :source,
      :metadata,
      :refs,
      :idempotency_key,
      :inserted_at
    ])
    |> validate_required([
      :session_id,
      :seq,
      :type,
      :payload,
      :actor,
      :producer_id,
      :producer_seq,
      :tenant_id,
      :metadata,
      :refs,
      :inserted_at
    ])
  end
end
