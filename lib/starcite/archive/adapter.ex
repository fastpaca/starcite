defmodule Starcite.Archive.Adapter do
  @moduledoc """
  Behaviour for archive backends. Implementations persist events to cold storage.

  An adapter must be idempotent on (session_id, seq) to support at-least-once.
  """

  @type event_row :: %{
          required(:session_id) => String.t(),
          required(:seq) => non_neg_integer(),
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          optional(:source) => String.t() | nil,
          required(:metadata) => map(),
          required(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t()
        }

  @callback start_link(keyword()) :: GenServer.on_start()
  @callback write_events([event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
end
