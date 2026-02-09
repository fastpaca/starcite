defmodule Starcite.Session.EventLog do
  @moduledoc """
  Append-only event history for a session.

  Events are indexed by sequence using a compact array window so cursor reads
  and trim operations remain bounded as sessions grow.
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

  @enforce_keys [:entries_by_index, :base_seq, :last_seq]
  defstruct entries_by_index: :array.new(), base_seq: 1, last_seq: 0

  @type t :: %EventLog{
          entries_by_index: :array.array(),
          base_seq: pos_integer(),
          last_seq: non_neg_integer()
        }

  @spec new() :: t()
  def new, do: %EventLog{entries_by_index: :array.new(), base_seq: 1, last_seq: 0}

  @spec entries(t()) :: [event()]
  def entries(%EventLog{} = log) do
    range(log.base_seq, log.last_seq)
    |> Enum.reduce([], fn seq, acc ->
      case fetch_event(log, seq) do
        nil -> acc
        event -> [event | acc]
      end
    end)
    |> Enum.reverse()
  end

  @spec append_event(t(), event_input(), non_neg_integer()) :: {t(), event(), non_neg_integer()}
  def append_event(%EventLog{} = log, input, last_seq) do
    next_seq = last_seq + 1
    index = seq_to_index(log.base_seq, next_seq)

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

    updated_log = %EventLog{
      log
      | entries_by_index: :array.set(index, event, log.entries_by_index),
        last_seq: next_seq
    }

    {updated_log, event, next_seq}
  end

  @spec from_cursor(t(), non_neg_integer(), pos_integer()) :: [event()]
  def from_cursor(%EventLog{} = log, cursor, limit) do
    start_seq = max(cursor + 1, log.base_seq)
    end_seq = min(log.last_seq, start_seq + limit - 1)

    if start_seq > end_seq do
      []
    else
      range(start_seq, end_seq)
      |> Enum.reduce([], fn seq, acc ->
        case fetch_event(log, seq) do
          nil -> acc
          event -> [event | acc]
        end
      end)
      |> Enum.reverse()
    end
  end

  @doc """
  Trim acknowledged events while retaining a bounded tail.
  """
  @spec trim_ack(t(), non_neg_integer(), pos_integer()) :: {t(), non_neg_integer()}
  def trim_ack(%EventLog{} = log, upto_seq, tail_keep)
      when is_integer(upto_seq) and upto_seq >= 0 and is_integer(tail_keep) and tail_keep > 0 do
    if log.last_seq < log.base_seq do
      {log, 0}
    else
      effective_upto = min(upto_seq, log.last_seq)
      new_base_seq = max(log.base_seq, effective_upto - tail_keep + 1)
      removed = max(new_base_seq - log.base_seq, 0)

      if removed == 0 do
        {log, 0}
      else
        compacted =
          range(new_base_seq, log.last_seq)
          |> Enum.map(&fetch_event!(log, &1))
          |> build_array()

        updated_log = %EventLog{
          log
          | entries_by_index: compacted,
            base_seq: new_base_seq
        }

        {updated_log, removed}
      end
    end
  end

  defp range(from, to) when is_integer(from) and is_integer(to) and from <= to, do: from..to
  defp range(_from, _to), do: []

  defp build_array(events) when is_list(events) do
    events
    |> Enum.with_index()
    |> Enum.reduce(:array.new(), fn {event, idx}, acc ->
      :array.set(idx, event, acc)
    end)
  end

  defp fetch_event(
         %EventLog{base_seq: base_seq, last_seq: last_seq, entries_by_index: entries},
         seq
       )
       when is_integer(seq) and seq >= base_seq and seq <= last_seq do
    case :array.get(seq_to_index(base_seq, seq), entries) do
      :undefined -> nil
      event -> event
    end
  end

  defp fetch_event(_log, _seq), do: nil

  defp fetch_event!(%EventLog{} = log, seq) do
    case fetch_event(log, seq) do
      nil -> raise ArgumentError, "missing event seq=#{seq}"
      event -> event
    end
  end

  defp seq_to_index(base_seq, seq), do: seq - base_seq
end
