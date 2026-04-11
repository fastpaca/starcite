defmodule Starcite.Session.Events do
  @moduledoc """
  Raw event timeline operations for a session.
  """

  alias Starcite.Session
  alias Starcite.Session.{Event, ProducerIndex}

  @default_tail_keep 1_000
  @default_producer_max_entries 10_000
  @retention_defaults_cache_key {__MODULE__, :retention_defaults}

  @type event_input :: Event.input()
  @type event :: Event.t()

  @spec append_event(Session.t(), ProducerIndex.t(), event_input()) ::
          {:appended, Session.t(), ProducerIndex.t(), event()}
          | {:deduped, Session.t(), ProducerIndex.t(), non_neg_integer()}
          | {:error, :producer_replay_conflict}
          | {:error, {:producer_seq_conflict, String.t(), pos_integer(), pos_integer()}}
          | {:error, :invalid_event}
  def append_event(
        %Session{} = session,
        producer_cursors,
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
             producer_id != "" and is_integer(producer_seq) and producer_seq > 0 and
             is_map(producer_cursors) do
    do_append_event(
      session,
      producer_cursors,
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
        %Session{} = session,
        producer_cursors,
        %{producer_id: producer_id, producer_seq: producer_seq} = input
      )
      when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
             producer_seq > 0 and is_map(producer_cursors) do
    do_append_event(
      session,
      producer_cursors,
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

  def append_event(%Session{}, _producer_cursors, _input), do: {:error, :invalid_event}

  @spec persist_ack(Session.t(), non_neg_integer()) :: {Session.t(), non_neg_integer()}
  def persist_ack(%Session{} = session, upto_seq)
      when is_integer(upto_seq) and upto_seq >= 0 do
    persist_ack(session, upto_seq, tail_keep())
  end

  @spec persist_ack(Session.t(), non_neg_integer(), pos_integer()) ::
          {Session.t(), non_neg_integer()}
  def persist_ack(%Session{} = session, upto_seq, tail_keep)
      when is_integer(upto_seq) and upto_seq >= 0 and is_integer(tail_keep) and tail_keep > 0 do
    archived_seq = max(session.archived_seq, min(upto_seq, session.last_seq))
    old_floor = retained_floor(session.archived_seq, session.last_seq, tail_keep)
    new_floor = retained_floor(archived_seq, session.last_seq, tail_keep)
    trimmed = max(new_floor - old_floor, 0)

    {%Session{session | archived_seq: archived_seq}, trimmed}
  end

  @spec tail_size(Session.t()) :: non_neg_integer()
  def tail_size(%Session{archived_seq: archived_seq, last_seq: last_seq}) do
    max(last_seq - archived_seq, 0)
  end

  @spec tail_keep() :: pos_integer()
  def tail_keep do
    retention_defaults().tail_keep
  end

  @spec producer_max_entries() :: pos_integer()
  def producer_max_entries do
    retention_defaults().producer_max_entries
  end

  @spec retention_defaults() :: %{tail_keep: pos_integer(), producer_max_entries: pos_integer()}
  def retention_defaults do
    raw = {default_tail_keep(), default_producer_max_entries()}

    case :persistent_term.get(@retention_defaults_cache_key, :undefined) do
      {^raw, defaults} ->
        defaults

      _ ->
        defaults = %{tail_keep: elem(raw, 0), producer_max_entries: elem(raw, 1)}
        :persistent_term.put(@retention_defaults_cache_key, {raw, defaults})
        defaults
    end
  end

  @spec normalize_epoch(Session.t()) :: Session.t()
  def normalize_epoch(%Session{epoch: epoch} = session)
      when is_integer(epoch) and epoch >= 0,
      do: session

  def normalize_epoch(%Session{} = session), do: %Session{session | epoch: 0}

  @spec normalize_epoch_value(term()) :: non_neg_integer()
  def normalize_epoch_value(epoch) when is_integer(epoch) and epoch >= 0, do: epoch
  def normalize_epoch_value(_epoch), do: 0

  defp do_append_event(
         %Session{} = session,
         producer_cursors,
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
           producer_max_entries()
         ) do
      {:deduped, seq, updated_index} ->
        {:deduped, session, updated_index, seq}

      {:append, updated_index} ->
        now = NaiveDateTime.utc_now()

        event = %{
          seq: next_seq,
          epoch: session.epoch,
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

        updated = %Session{session | last_seq: next_seq}

        {:appended, updated, updated_index, event}

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

  defp default_tail_keep do
    Application.get_env(:starcite, :tail_keep, @default_tail_keep)
  end

  defp default_producer_max_entries do
    Application.get_env(:starcite, :producer_max_entries, @default_producer_max_entries)
  end
end
