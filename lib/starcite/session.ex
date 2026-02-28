defmodule Starcite.Session do
  @moduledoc """
  Core session entity for Starcite.

  A session is an append-only sequence of events with deterministic ordering.
  Starcite stores event streams and replay semantics; it does not impose an
  application protocol on event payloads.
  """

  alias __MODULE__, as: Session
  alias Starcite.Auth.Principal
  alias Starcite.Session.{Event, ProducerIndex}

  @type event_input :: Event.input()
  @type event :: Event.t()

  @enforce_keys [
    :id,
    :last_seq,
    :archived_seq,
    :inserted_at,
    :retention,
    :producer_cursors
  ]
  defstruct [
    :id,
    :title,
    :creator_principal,
    :metadata,
    :last_seq,
    :archived_seq,
    :inserted_at,
    :retention,
    :producer_cursors
  ]

  @type t :: %Session{
          id: String.t(),
          title: String.t() | nil,
          creator_principal: Principal.t() | nil,
          metadata: map(),
          last_seq: non_neg_integer(),
          archived_seq: non_neg_integer(),
          inserted_at: NaiveDateTime.t(),
          retention: %{tail_keep: pos_integer(), producer_max_entries: pos_integer()},
          producer_cursors: ProducerIndex.t()
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
      Keyword.get(
        opts,
        :tail_keep,
        Application.get_env(:starcite, :tail_keep, @default_tail_keep)
      )

    producer_max_entries =
      Keyword.get(
        opts,
        :producer_max_entries,
        Application.get_env(:starcite, :producer_max_entries, @default_producer_max_entries)
      )

    %Session{
      id: id,
      title: Keyword.get(opts, :title),
      creator_principal:
        optional_principal!(Keyword.get(opts, :creator_principal), :creator_principal),
      metadata: Keyword.get(opts, :metadata, %{}),
      last_seq: 0,
      archived_seq: 0,
      inserted_at: now,
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
        } = input
      )
      when is_binary(type) and type != "" and is_map(payload) and is_binary(actor) and actor != "" and
             (is_binary(source) or is_nil(source)) and is_map(metadata) and is_map(refs) and
             (is_binary(idempotency_key) or is_nil(idempotency_key)) and is_binary(producer_id) and
             producer_id != "" and is_integer(producer_seq) and producer_seq > 0 do
    do_append_event(
      session,
      input,
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
        %{producer_id: producer_id, producer_seq: producer_seq} = input
      )
      when is_binary(producer_id) and producer_id != "" and is_integer(producer_seq) and
             producer_seq > 0 do
    do_append_event(
      session,
      input,
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

  def append_event(%Session{}, _input), do: {:error, :invalid_event}

  defp do_append_event(
         %Session{} = session,
         _input,
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
           session.producer_cursors,
           producer_id,
           producer_seq,
           fingerprint,
           next_seq,
           session.retention.producer_max_entries
         ) do
      {:deduped, seq, updated_index} ->
        updated =
          maybe_update_cursors(session, updated_index)

        {:deduped, updated, seq}

      {:append, updated_index} ->
        now = NaiveDateTime.utc_now()
        tenant_id = session_tenant_id(session)

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
          tenant_id: tenant_id,
          inserted_at: now
        }

        updated = %Session{
          session
          | last_seq: next_seq,
            producer_cursors: updated_index
        }

        {:appended, updated, event}

      {:error, reason} ->
        {:error, reason}
    end
  end

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

    {%Session{session | archived_seq: archived_seq}, trimmed}
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
    created_at = iso8601_utc(session.inserted_at)

    %{
      id: session.id,
      title: session.title,
      creator_principal: session.creator_principal,
      metadata: session.metadata,
      last_seq: session.last_seq,
      created_at: created_at,
      updated_at: created_at
    }
  end

  defp maybe_update_cursors(%Session{} = session, updated_index)
       when updated_index == session.producer_cursors do
    session
  end

  defp maybe_update_cursors(%Session{} = session, updated_index) when is_map(updated_index) do
    %Session{session | producer_cursors: updated_index}
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

  defp iso8601_utc(%NaiveDateTime{} = datetime) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> DateTime.to_iso8601()
  end

  defp optional_principal!(nil, _field), do: nil
  defp optional_principal!(%Principal{} = principal, _field), do: principal

  defp optional_principal!(
         %{"tenant_id" => tenant_id, "id" => id, "type" => type},
         field
       )
       when is_binary(tenant_id) and tenant_id != "" and is_binary(id) and id != "" and
              is_binary(type) and type != "" do
    type = principal_type!(type, field)

    case Principal.new(tenant_id, id, type) do
      {:ok, principal} -> principal
      {:error, :invalid_principal} -> raise ArgumentError, "invalid session #{field}"
    end
  end

  defp optional_principal!(value, field) do
    raise ArgumentError, "invalid session #{field}: #{inspect(value)}"
  end

  defp principal_type!("user", _field), do: :user
  defp principal_type!("agent", _field), do: :agent

  defp principal_type!(value, field) do
    raise ArgumentError, "invalid session #{field} type: #{inspect(value)}"
  end

  defp session_tenant_id(%Session{metadata: metadata, creator_principal: creator_principal}) do
    metadata_tenant_id(metadata) || principal_tenant_id(creator_principal)
  end

  defp metadata_tenant_id(metadata) when is_map(metadata) do
    case Map.get(metadata, "tenant_id") || Map.get(metadata, :tenant_id) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" -> tenant_id
      _other -> nil
    end
  end

  defp metadata_tenant_id(_metadata), do: nil

  defp principal_tenant_id(%Principal{tenant_id: tenant_id})
       when is_binary(tenant_id) and tenant_id != "",
       do: tenant_id

  defp principal_tenant_id(principal) when is_map(principal) do
    case Map.get(principal, "tenant_id") || Map.get(principal, :tenant_id) do
      tenant_id when is_binary(tenant_id) and tenant_id != "" -> tenant_id
      _other -> nil
    end
  end

  defp principal_tenant_id(_principal), do: nil
end
