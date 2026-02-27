defmodule Starcite.Archive.Adapter do
  @moduledoc """
  Behaviour for archive backends. Implementations persist events to cold storage.

  An adapter must be idempotent on (session_id, seq) to support at-least-once.
  """

  alias Starcite.Auth.Principal

  @type event_row :: %{
          required(:session_id) => String.t(),
          required(:seq) => non_neg_integer(),
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          required(:producer_id) => String.t(),
          required(:producer_seq) => pos_integer(),
          optional(:source) => String.t() | nil,
          required(:metadata) => map(),
          required(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil,
          required(:inserted_at) => NaiveDateTime.t()
        }

  @type session_row :: %{
          required(:id) => String.t(),
          optional(:title) => String.t() | nil,
          required(:creator_principal) => Principal.t() | map() | nil,
          optional(:metadata) => map(),
          required(:created_at) => DateTime.t() | String.t()
        }

  @type session_query :: %{
          optional(:limit) => pos_integer(),
          optional(:cursor) => String.t() | nil,
          optional(:metadata) => map(),
          optional(:owner_principal_ids) => [String.t()],
          optional(:tenant_id) => String.t()
        }

  @type session_page :: %{
          required(:sessions) => [session_row()],
          required(:next_cursor) => String.t() | nil
        }

  @callback start_link(keyword()) :: GenServer.on_start()
  @callback write_events([event_row()]) :: {:ok, non_neg_integer()} | {:error, term()}
  @callback read_events(String.t(), pos_integer(), pos_integer()) ::
              {:ok, [map()]} | {:error, term()}
  @callback upsert_session(session_row()) :: :ok | {:error, term()}
  @callback list_sessions(session_query()) :: {:ok, session_page()} | {:error, term()}
  @callback list_sessions_by_ids([String.t()], session_query()) ::
              {:ok, session_page()} | {:error, term()}
end
