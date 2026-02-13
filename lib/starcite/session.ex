defmodule Starcite.Session do
  @moduledoc """
  Core session entity for Starcite.

  A session is an append-only sequence of events with deterministic ordering.
  Starcite stores event streams and replay semantics; it does not impose an
  application protocol on event payloads.
  """

  alias __MODULE__, as: Session
  alias Starcite.Session.{Event, ProducerIndex}

  @type event_input :: Event.input()
  @type event :: Event.t()

  @enforce_keys [
    :id,
    :last_seq,
    :archived_seq,
    :inserted_at,
    :updated_at,
    :retention,
    :producer_cursors
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
    :producer_cursors
  ]

  @type t :: %Session{
          id: String.t(),
          title: String.t() | nil,
          metadata: map(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer(),
          inserted_at: NaiveDateTime.t(),
          updated_at: NaiveDateTime.t(),
          retention: %{tail_keep: pos_integer(), producer_max_entries: pos_integer()},
          producer_cursors: %{optional(String.t()) => ProducerIndex.cursor()}
        }

  @default_tail_keep 1_000
  @default_producer_max_entries 10_000

  @doc """
  Create a new session.
  """
  @spec new(String.t(), keyword()) :: t()
  def new(id, opts \\ []) when is_binary(id) do
    now = Keyword.get(opts, :timestamp, NaiveDateTime.utc_now())

    tail_keep =
      normalize_positive_integer(
        opts[:tail_keep],
        Application.get_env(:starcite, :tail_keep, @default_tail_keep)
      )

    producer_max_entries =
      normalize_positive_integer(
        opts[:producer_max_entries],
        Application.get_env(
          :starcite,
          :producer_max_entries,
          @default_producer_max_entries
        )
      )

    %Session{
      id: id,
      title: Keyword.get(opts, :title),
      metadata: Keyword.get(opts, :metadata, %{}),
      last_seq: 0,
      archived_seq: 0,
      inserted_at: now,
      updated_at: now,
      retention: %{tail_keep: tail_keep, producer_max_entries: producer_max_entries},
      producer_cursors: %{}
    }
  end

  @doc """
  Append one event to the session.

  Returns one of:

    - `{:appended, updated_session, event}`
    - `{:deduped, updated_session, existing_seq}`
    - `{:error, :producer_replay_conflict}`
    - `{:error, {:producer_seq_conflict, producer_id, expected_seq, producer_seq}}`
  """
  @spec append_event(t(), event_input()) ::
          {:appended, t(), event()}
          | {:deduped, t(), non_neg_integer()}
          | {:error, :producer_replay_conflict}
          | {:error, {:producer_seq_conflict, String.t(), pos_integer(), pos_integer()}}
          | {:error, :invalid_event}
  def append_event(
        %Session{} = session,
        %{producer_id: producer_id, producer_seq: producer_seq} = input
      )
      when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
             producer_seq > 0 do
    hash = event_hash(input)
    next_seq = session.last_seq + 1

    case ProducerIndex.decide(
           session.producer_cursors,
           producer_id,
           producer_seq,
           hash,
           next_seq,
           session.retention.producer_max_entries
         ) do
      {:deduped, seq, updated_index} ->
        updated =
          maybe_update_cursors(session, updated_index)

        {:deduped, updated, seq}

      {:append, updated_index} ->
        now = NaiveDateTime.utc_now()

        event = %{
          seq: next_seq,
          type: input.type,
          payload: input.payload,
          actor: input.actor,
          source: Map.get(input, :source),
          metadata: Map.get(input, :metadata, %{}),
          refs: Map.get(input, :refs, %{}),
          idempotency_key: Map.get(input, :idempotency_key),
          producer_id: producer_id,
          producer_seq: producer_seq,
          inserted_at: now
        }

        updated = %Session{
          session
          | last_seq: next_seq,
            updated_at: now,
            producer_cursors: updated_index
        }

        {:appended, updated, event}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def append_event(%Session{}, _input), do: {:error, :invalid_event}

  @doc """
  Apply an archive acknowledgement up to `upto_seq` and update retention
  metadata used for telemetry and tail accounting.
  """
  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  def persist_ack(%Session{} = session, upto_seq)
      when is_integer(upto_seq) and upto_seq >= 0 do
    tail_keep = session.retention.tail_keep
    archived_seq = max(session.archived_seq, min(upto_seq, session.last_seq))
    old_floor = retained_floor(session.archived_seq, session.last_seq, tail_keep)
    new_floor = retained_floor(archived_seq, session.last_seq, tail_keep)
    trimmed = max(new_floor - old_floor, 0)

    {%Session{session | archived_seq: archived_seq, updated_at: NaiveDateTime.utc_now()}, trimmed}
  end

  @doc """
  Return the virtual tail size implied by retention metadata.
  """
  @spec tail_size(t()) :: non_neg_integer()
  def tail_size(%Session{archived_seq: archived_seq, last_seq: last_seq}) do
    max(last_seq - archived_seq, 0)
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

  defp maybe_update_cursors(%Session{} = session, updated_index)
       when updated_index == session.producer_cursors do
    session
  end

  defp maybe_update_cursors(%Session{} = session, updated_index) when is_map(updated_index) do
    %Session{session | producer_cursors: updated_index, updated_at: NaiveDateTime.utc_now()}
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

  defp event_hash(input) do
    input
    |> Map.take([
      :type,
      :payload,
      :actor,
      :source,
      :metadata,
      :refs,
      :idempotency_key,
      :producer_id,
      :producer_seq
    ])
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
  end

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp iso8601_utc(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp normalize_positive_integer(nil, default) when is_integer(default) and default > 0,
    do: default

  defp normalize_positive_integer(value, _default) when is_integer(value) and value > 0,
    do: value

  defp normalize_positive_integer(_value, default) when is_integer(default) and default > 0,
    do: default
end
