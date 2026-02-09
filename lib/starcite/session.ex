defmodule Starcite.Session do
  @moduledoc """
  Core session entity for Starcite.

  A session is an append-only sequence of events with deterministic ordering.
  Starcite stores event streams and replay semantics; it does not impose an
  application protocol on event payloads.
  """

  alias __MODULE__, as: Session
  alias Starcite.Session.EventLog

  @type event_input :: EventLog.event_input()
  @type event :: EventLog.event()

  @enforce_keys [
    :id,
    :event_log,
    :last_seq,
    :archived_seq,
    :inserted_at,
    :updated_at,
    :retention,
    :idempotency_index
  ]
  defstruct [
    :id,
    :title,
    :metadata,
    :event_log,
    :last_seq,
    :archived_seq,
    :inserted_at,
    :updated_at,
    :retention,
    :idempotency_index
  ]

  @type t :: %Session{
          id: String.t(),
          title: String.t() | nil,
          metadata: map(),
          event_log: EventLog.t(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer(),
          inserted_at: NaiveDateTime.t(),
          updated_at: NaiveDateTime.t(),
          retention: %{tail_keep: pos_integer()},
          idempotency_index: %{optional(String.t()) => %{seq: non_neg_integer(), hash: binary()}}
        }

  @doc """
  Create a new session.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) when is_binary(id) do
    now = Keyword.get(opts, :timestamp, NaiveDateTime.utc_now())
    tail_keep = opts[:tail_keep] || Application.get_env(:starcite, :tail_keep, 1_000)

    %Session{
      id: id,
      title: Keyword.get(opts, :title),
      metadata: Keyword.get(opts, :metadata, %{}),
      event_log: EventLog.new(),
      last_seq: 0,
      archived_seq: 0,
      inserted_at: now,
      updated_at: now,
      retention: %{tail_keep: tail_keep},
      idempotency_index: %{}
    }
  end

  @doc """
  Append one event to the session.

  Returns one of:

    - `{:appended, updated_session, event}`
    - `{:deduped, unchanged_session, existing_seq}`
    - `{:error, :idempotency_conflict}`
  """
  @spec append_event(t(), event_input()) ::
          {:appended, t(), event()}
          | {:deduped, t(), non_neg_integer()}
          | {:error, :idempotency_conflict}
  def append_event(%Session{} = session, input) do
    idempotency_key = Map.get(input, :idempotency_key)
    hash = event_hash(input)

    case dedupe_action(session.idempotency_index, idempotency_key, hash) do
      {:deduped, seq} ->
        {:deduped, session, seq}

      {:error, :idempotency_conflict} ->
        {:error, :idempotency_conflict}

      :append ->
        {event_log, event, last_seq} =
          EventLog.append_event(session.event_log, input, session.last_seq)

        idempotency_index =
          put_idempotency(session.idempotency_index, idempotency_key, hash, event.seq)

        updated = %Session{
          session
          | event_log: event_log,
            last_seq: last_seq,
            updated_at: NaiveDateTime.utc_now(),
            idempotency_index: idempotency_index
        }

        {:appended, updated, event}
    end
  end

  @spec events_from_cursor(t(), non_neg_integer(), pos_integer()) :: [event()]
  def events_from_cursor(%Session{} = session, cursor, limit),
    do: EventLog.from_cursor(session.event_log, cursor, limit)

  @doc """
  Apply an archive acknowledgement up to `upto_seq`, trimming the in-memory
  event log while keeping a bounded tail.
  """
  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  def persist_ack(%Session{} = session, upto_seq)
      when is_integer(upto_seq) and upto_seq >= 0 do
    tail_keep = session.retention.tail_keep
    archived_seq = max(session.archived_seq, upto_seq)
    {event_log, trimmed} = EventLog.trim_ack(session.event_log, archived_seq, tail_keep)

    {%Session{
       session
       | archived_seq: archived_seq,
         event_log: event_log,
         updated_at: NaiveDateTime.utc_now()
     }, trimmed}
  end

  @spec to_map(t()) :: map()
  def to_map(%Session{} = session) do
    %{
      id: session.id,
      title: session.title,
      metadata: session.metadata,
      last_seq: session.last_seq,
      created_at: iso8601_utc(session.inserted_at),
      updated_at: iso8601_utc(session.updated_at)
    }
  end

  defp dedupe_action(_index, nil, _hash), do: :append

  defp dedupe_action(index, idempotency_key, hash) do
    case Map.get(index, idempotency_key) do
      nil ->
        :append

      %{hash: ^hash, seq: seq} ->
        {:deduped, seq}

      %{hash: _other_hash} ->
        {:error, :idempotency_conflict}
    end
  end

  defp put_idempotency(index, nil, _hash, _seq), do: index

  defp put_idempotency(index, idempotency_key, hash, seq) do
    Map.put(index, idempotency_key, %{hash: hash, seq: seq})
  end

  defp event_hash(input) do
    input
    |> Map.take([:type, :payload, :actor, :source, :metadata, :refs])
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp iso8601_utc(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
end
