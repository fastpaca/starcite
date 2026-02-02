defmodule Fastpaca.Archive.Adapter do
  @moduledoc """
  Behaviour for archive backends. Implementations persist messages to cold storage.

  An adapter must be idempotent on (conversation_id, seq) to support at-least-once.
  """

  @type message_row :: %{
          required(:conversation_id) => String.t(),
          required(:seq) => non_neg_integer(),
          required(:role) => String.t(),
          required(:parts) => list(),
          required(:metadata) => map(),
          required(:token_count) => non_neg_integer(),
          required(:inserted_at) => NaiveDateTime.t()
        }

  @callback start_link(keyword()) :: GenServer.on_start()
  @callback write_messages([message_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
end
