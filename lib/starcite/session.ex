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
        next_seq = session.last_seq + 1

        event = %{
          seq: next_seq,
          type: input.type,
          payload: input.payload,
          actor: input.actor,
          source: Map.get(input, :source),
          metadata: Map.get(input, :metadata, %{}),
          refs: Map.get(input, :refs, %{}),
          idempotency_key: idempotency_key,
          inserted_at: NaiveDateTime.utc_now()
        }

        idempotency_index =
          put_idempotency(session.idempotency_index, idempotency_key, hash, event.seq)

        updated = %Session{
          session
          | last_seq: next_seq,
            updated_at: NaiveDateTime.utc_now(),
            idempotency_index: idempotency_index
        }

        {:appended, updated, event}
    end
  end

  @doc """
  Apply an archive acknowledgement up to `upto_seq` and update retention
  metadata used for idempotency pruning and telemetry.
  """
  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  def persist_ack(%Session{} = session, upto_seq)
      when is_integer(upto_seq) and upto_seq >= 0 do
    tail_keep = session.retention.tail_keep
    archived_seq = max(session.archived_seq, min(upto_seq, session.last_seq))
    old_floor = retained_floor(session.archived_seq, session.last_seq, tail_keep)
    new_floor = retained_floor(archived_seq, session.last_seq, tail_keep)
    trimmed = max(new_floor - old_floor, 0)
    idempotency_index = prune_idempotency(session.idempotency_index, new_floor)

    {%Session{
       session
       | archived_seq: archived_seq,
         idempotency_index: idempotency_index,
         updated_at: NaiveDateTime.utc_now()
     }, trimmed}
  end

  @doc """
  Return the virtual tail size implied by retention metadata.
  """
  @spec tail_size(t()) :: non_neg_integer()
  def tail_size(%Session{archived_seq: archived_seq, last_seq: last_seq, retention: retention}) do
    floor = retained_floor(archived_seq, last_seq, retention.tail_keep)

    if last_seq < floor do
      0
    else
      last_seq - floor + 1
    end
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

  defp retained_floor(archived_seq, last_seq, tail_keep)
       when is_integer(archived_seq) and archived_seq >= 0 and is_integer(last_seq) and
              last_seq >= 0 and
              is_integer(tail_keep) and tail_keep > 0 do
    archived_seq
    |> min(last_seq)
    |> Kernel.-(tail_keep)
    |> Kernel.+(1)
    |> max(1)
  end

  defp prune_idempotency(index, min_seq_to_keep)
       when is_map(index) and is_integer(min_seq_to_keep) and min_seq_to_keep >= 0 do
    Enum.reduce(index, %{}, fn
      {key, %{seq: seq} = value}, acc
      when is_binary(key) and is_integer(seq) and seq >= min_seq_to_keep ->
        Map.put(acc, key, value)

      _, acc ->
        acc
    end)
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
