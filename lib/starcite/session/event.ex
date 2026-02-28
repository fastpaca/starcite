defmodule Starcite.Session.Event do
  @moduledoc """
  Session event type contracts.
  """

  @type input :: %{
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          required(:producer_id) => String.t(),
          required(:producer_seq) => pos_integer(),
          optional(:tenant_id) => String.t() | nil,
          optional(:source) => String.t() | nil,
          optional(:metadata) => map(),
          optional(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil
        }

  @type t :: %{
          required(:seq) => non_neg_integer(),
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          required(:producer_id) => String.t(),
          required(:producer_seq) => pos_integer(),
          optional(:tenant_id) => String.t() | nil,
          optional(:source) => String.t() | nil,
          required(:metadata) => map(),
          required(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t()
        }
end
