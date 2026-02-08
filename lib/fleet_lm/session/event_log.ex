defmodule FleetLM.Session.EventLog do
  @moduledoc """
  Append-only event history for a session.

  Events are stored newest-first for efficient live-tail operations.
  """

  alias __MODULE__, as: EventLog

  @type event_input :: %{
          required(:type) => String.t(),
          required(:payload) => map(),
          required(:actor) => String.t(),
          optional(:source) => String.t() | nil,
          optional(:metadata) => map(),
          optional(:refs) => map(),
          optional(:idempotency_key) => String.t() | nil
        }

  @type event :: %{
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

  @enforce_keys [:entries]
  defstruct entries: []

  @type t :: %EventLog{entries: [event()]}

  @spec new() :: t()
  def new, do: %EventLog{entries: []}

  @spec entries(t()) :: [event()]
  def entries(%EventLog{entries: entries}), do: Enum.reverse(entries)

  @spec append_event(t(), event_input(), non_neg_integer()) :: {t(), event(), non_neg_integer()}
  def append_event(%EventLog{entries: entries} = log, input, last_seq) do
    next_seq = last_seq + 1

    event = %{
      seq: next_seq,
      type: input.type,
      payload: input.payload,
      actor: input.actor,
      source: Map.get(input, :source),
      metadata: Map.get(input, :metadata, %{}),
      refs: Map.get(input, :refs, %{}),
      idempotency_key: Map.get(input, :idempotency_key),
      inserted_at: NaiveDateTime.utc_now()
    }

    {%EventLog{log | entries: [event | entries]}, event, next_seq}
  end

  @spec from_cursor(t(), non_neg_integer(), pos_integer()) :: [event()]
  def from_cursor(%EventLog{entries: entries}, cursor, limit) do
    entries
    |> Enum.filter(fn %{seq: seq} -> seq > cursor end)
    |> Enum.reverse()
    |> Enum.take(limit)
  end

  @doc """
  Trim acknowledged events while retaining a bounded tail.
  """
  @spec trim_ack(t(), non_neg_integer(), pos_integer()) :: {t(), non_neg_integer()}
  def trim_ack(%EventLog{entries: entries} = log, upto_seq, tail_keep)
      when is_integer(upto_seq) and upto_seq >= 0 and is_integer(tail_keep) and tail_keep > 0 do
    {newer, older_or_equal} = Enum.split_with(entries, fn %{seq: seq} -> seq > upto_seq end)

    retained_from_older = Enum.take(older_or_equal, tail_keep)
    kept = newer ++ retained_from_older
    removed = length(entries) - length(kept)

    {%EventLog{log | entries: kept}, max(removed, 0)}
  end
end
