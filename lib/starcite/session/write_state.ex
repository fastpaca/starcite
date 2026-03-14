defmodule Starcite.Session.WriteState do
  @moduledoc """
  Active append state for a session.
  """

  alias __MODULE__, as: WriteState
  alias Starcite.Session
  alias Starcite.Session.{Event, ProducerIndex}

  @enforce_keys [:session, :producer_cursors]
  # Only the Raft FSM keeps this heavier shape; read paths use Session alone.
  defstruct [:session, :producer_cursors]

  @type event_input :: Event.input()
  @type event :: Event.t()

  @type t :: %WriteState{
          session: Session.t(),
          producer_cursors: ProducerIndex.t()
        }

  @spec new(Session.t(), ProducerIndex.t()) :: t()
  def new(%Session{} = session, producer_cursors \\ %{}) when is_map(producer_cursors) do
    %WriteState{
      session: session,
      producer_cursors: producer_cursors
    }
  end

  @spec session(t()) :: Session.t()
  def session(%WriteState{session: %Session{} = session}), do: session

  @spec append_event(t(), event_input()) ::
          {:appended, t(), event()}
          | {:deduped, t(), non_neg_integer()}
          | {:error, :producer_replay_conflict}
          | {:error, {:producer_seq_conflict, String.t(), pos_integer(), pos_integer()}}
          | {:error, :invalid_event}
  def append_event(
        %WriteState{} = write_state,
        %{
          type: type,
          payload: payload,
          actor: actor,
          source: source,
          metadata: metadata,
          refs: refs,
          idempotency_key: idempotency_key,
          producer_id: producer_id,
          producer_seq: producer_seq
        }
      )
      when is_binary(type) and type != "" and is_map(payload) and is_binary(actor) and actor != "" and
             (is_binary(source) or is_nil(source)) and is_map(metadata) and is_map(refs) and
             (is_binary(idempotency_key) or is_nil(idempotency_key)) and is_binary(producer_id) and
             producer_id != "" and is_integer(producer_seq) and producer_seq > 0 do
    do_append_event(
      write_state,
      producer_id,
      producer_seq,
      type,
      payload,
      actor,
      source,
      metadata,
      refs,
      idempotency_key
    )
  end

  def append_event(
        %WriteState{} = write_state,
        %{producer_id: producer_id, producer_seq: producer_seq} = input
      )
      when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
             producer_seq > 0 do
    do_append_event(
      write_state,
      producer_id,
      producer_seq,
      input.type,
      input.payload,
      input.actor,
      Map.get(input, :source),
      Map.get(input, :metadata, %{}),
      Map.get(input, :refs, %{}),
      Map.get(input, :idempotency_key)
    )
  end

  def append_event(%WriteState{}, _input), do: {:error, :invalid_event}

  @spec persist_ack(t(), non_neg_integer()) :: {t(), non_neg_integer()}
  def persist_ack(%WriteState{session: %Session{} = session} = write_state, upto_seq)
      when is_integer(upto_seq) and upto_seq >= 0 do
    tail_keep = Session.tail_keep()
    archived_seq = max(session.archived_seq, min(upto_seq, session.last_seq))
    old_floor = retained_floor(session.archived_seq, session.last_seq, tail_keep)
    new_floor = retained_floor(archived_seq, session.last_seq, tail_keep)
    trimmed = max(new_floor - old_floor, 0)

    updated_session = %Session{session | archived_seq: archived_seq}
    {%WriteState{write_state | session: updated_session}, trimmed}
  end

  defp do_append_event(
         %WriteState{session: %Session{} = session, producer_cursors: producer_cursors} =
           write_state,
         producer_id,
         producer_seq,
         type,
         payload,
         actor,
         source,
         metadata,
         refs,
         idempotency_key
       ) do
    fingerprint =
      event_fingerprint(
        type,
        payload,
        actor,
        source,
        metadata,
        refs,
        idempotency_key,
        producer_id,
        producer_seq
      )

    next_seq = session.last_seq + 1

    case ProducerIndex.decide(
           producer_cursors,
           producer_id,
           producer_seq,
           fingerprint,
           next_seq,
           Session.producer_max_entries()
         ) do
      {:deduped, seq, _updated_index} ->
        {:deduped, write_state, seq}

      {:append, updated_index} ->
        now = NaiveDateTime.utc_now()

        event = %{
          seq: next_seq,
          type: type,
          payload: payload,
          actor: actor,
          source: source,
          metadata: metadata,
          refs: refs,
          idempotency_key: idempotency_key,
          producer_id: producer_id,
          producer_seq: producer_seq,
          tenant_id: session.tenant_id,
          inserted_at: now
        }

        updated_session = %Session{session | last_seq: next_seq}

        updated_write_state = %WriteState{
          write_state
          | session: updated_session,
            producer_cursors: updated_index
        }

        {:appended, updated_write_state, event}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp retained_floor(archived_seq, last_seq, tail_keep)
       when is_integer(archived_seq) and archived_seq >= 0 and is_integer(last_seq) and
              last_seq >= 0 and is_integer(tail_keep) and tail_keep > 0 do
    archived_seq
    |> min(last_seq)
    |> Kernel.-(tail_keep)
    |> Kernel.+(1)
    |> max(1)
  end

  defp event_fingerprint(
         type,
         payload,
         actor,
         source,
         metadata,
         refs,
         idempotency_key,
         producer_id,
         producer_seq
       )
       when is_binary(type) and is_map(payload) and is_binary(actor) and
              (is_binary(source) or is_nil(source)) and is_map(metadata) and is_map(refs) and
              (is_binary(idempotency_key) or is_nil(idempotency_key)) and is_binary(producer_id) and
              is_integer(producer_seq) do
    fingerprint_term = {
      :event_fingerprint,
      type,
      payload,
      actor,
      source,
      metadata,
      refs,
      idempotency_key,
      producer_id,
      producer_seq
    }

    <<:erlang.phash2(fingerprint_term)::32, :erlang.phash2({:v2, fingerprint_term})::32>>
  end
end
