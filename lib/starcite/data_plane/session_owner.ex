defmodule Starcite.DataPlane.SessionOwner do
  @moduledoc """
  In-memory owner process for one session.

  The owner is the sequencing authority for append operations and is
  responsible for publishing cursor updates to tail subscribers.
  """

  use GenServer

  alias Starcite.ControlPlane.{SessionReplicator, SessionRouter}
  alias Starcite.DataPlane.{CursorUpdate, EventStore, SessionStore}
  alias Starcite.Observability.Telemetry
  alias Starcite.Session

  @registry Starcite.DataPlane.SessionOwnerRegistry

  @type state :: %{
          required(:session) => Session.t()
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)
    GenServer.start_link(__MODULE__, session, name: via(session.id))
  end

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    session = Keyword.fetch!(opts, :session)

    %{
      id: {__MODULE__, session.id},
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: 500,
      type: :worker
    }
  end

  @spec via(String.t()) :: {:via, Registry, {module(), String.t()}}
  def via(session_id) when is_binary(session_id) and session_id != "" do
    {:via, Registry, {@registry, session_id}}
  end

  @impl true
  def init(%Session{} = session) do
    resolved_session =
      case SessionStore.get_session(session.id) do
        {:ok, %Session{} = loaded} -> loaded
        _ -> session
      end

    owner_session =
      resolved_session
      |> normalize_session_epoch()
      |> apply_owner_epoch()

    {:ok, %{session: owner_session}}
  end

  @impl true
  def handle_call(:get_session, _from, %{session: session} = state) do
    {:reply, {:ok, session}, state}
  end

  def handle_call(:fetch_cursor_snapshot, _from, %{session: session} = state) do
    {:reply, {:ok, cursor_snapshot(session)}, state}
  end

  def handle_call(:fetch_archived_seq, _from, %{session: session} = state) do
    {:reply, {:ok, session.archived_seq}, state}
  end

  def handle_call({:append_event, input, expected_seq}, _from, %{session: session} = state)
      when is_map(input) do
    with :ok <- guard_expected_seq(session, expected_seq),
         {:ok, updated_session, reply, event_to_store} <-
           append_one_to_session(session, input),
         normalized_session <- normalize_session_epoch(updated_session),
         :ok <-
           SessionReplicator.replicate_state(normalized_session, maybe_event_list(event_to_store)) do
      :ok =
        maybe_put_appended_event(
          normalized_session.id,
          normalized_session.tenant_id,
          event_to_store
        )

      :ok = maybe_publish_event(normalized_session.id, event_to_store)
      :ok = SessionStore.put_session(normalized_session)

      reply =
        decorate_append_reply(
          reply,
          normalized_session.epoch,
          normalized_session.archived_seq
        )

      {:reply, {:ok, reply}, %{state | session: normalized_session}}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:append_event, _input, _expected_seq}, _from, state) do
    {:reply, {:error, :invalid_event}, state}
  end

  def handle_call({:append_events, inputs, expected_seq}, _from, %{session: session} = state)
      when is_list(inputs) do
    with :ok <- guard_expected_seq(session, expected_seq),
         {:ok, updated_session, replies, events_to_store} <-
           append_to_session(session, inputs),
         normalized_session <- normalize_session_epoch(updated_session),
         :ok <- SessionReplicator.replicate_state(normalized_session, events_to_store) do
      :ok =
        maybe_put_appended_events(
          normalized_session.id,
          normalized_session.tenant_id,
          events_to_store
        )

      :ok = publish_events(normalized_session.id, events_to_store)
      :ok = SessionStore.put_session(normalized_session)

      replies =
        Enum.map(replies, fn reply ->
          decorate_append_reply(
            reply,
            normalized_session.epoch,
            normalized_session.archived_seq
          )
        end)

      response = %{
        results: replies,
        last_seq: normalized_session.last_seq,
        epoch: normalized_session.epoch,
        cursor: %{epoch: normalized_session.epoch, seq: normalized_session.last_seq},
        committed_cursor: %{
          epoch: normalized_session.epoch,
          seq: normalized_session.archived_seq
        }
      }

      {:reply, {:ok, response}, %{state | session: normalized_session}}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:append_events, _inputs, _expected_seq}, _from, state) do
    {:reply, {:error, :invalid_event}, state}
  end

  def handle_call({:ack_archived, upto_seq}, _from, %{session: session} = state)
      when is_integer(upto_seq) and upto_seq >= 0 do
    previous_archived_seq = session.archived_seq
    {updated_session, _trimmed} = Session.persist_ack(session, upto_seq)
    normalized_session = normalize_session_epoch(updated_session)

    with :ok <- SessionReplicator.replicate_state(normalized_session, []) do
      evicted =
        evict_archived_events(
          normalized_session.id,
          previous_archived_seq,
          normalized_session.archived_seq
        )

      tail_size = Session.tail_size(normalized_session)
      tenant_id = normalized_session.tenant_id

      Telemetry.archive_ack_applied(
        normalized_session.id,
        tenant_id,
        normalized_session.last_seq,
        normalized_session.archived_seq,
        evicted,
        normalized_session.retention.tail_keep,
        tail_size
      )

      :ok = SessionStore.put_session(normalized_session)

      response = %{
        archived_seq: normalized_session.archived_seq,
        trimmed: evicted,
        committed_cursor: %{
          epoch: normalized_session.epoch,
          seq: normalized_session.archived_seq
        }
      }

      {:reply, {:ok, response}, %{state | session: normalized_session}}
    else
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:ack_archived, _upto_seq}, _from, state) do
    {:reply, {:error, :invalid_event}, state}
  end

  def handle_call(
        {:replicate_state, %Session{id: session_id} = incoming_session, incoming_events},
        _from,
        state
      )
      when is_binary(session_id) and session_id != "" and is_list(incoming_events) do
    normalized_session = normalize_session_epoch(incoming_session)
    normalized_events = put_events_epoch(incoming_events, normalized_session.epoch)
    current_session = state.session

    if should_apply_replication?(normalized_session, current_session) do
      previous_archived_seq = current_session.archived_seq

      :ok =
        maybe_put_appended_events(
          normalized_session.id,
          normalized_session.tenant_id,
          normalized_events
        )

      _evicted =
        evict_archived_events(
          normalized_session.id,
          previous_archived_seq,
          normalized_session.archived_seq
        )

      :ok = SessionStore.put_session(normalized_session)
      {:reply, :ok, %{state | session: normalized_session}}
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:replicate_state, _incoming_session, _incoming_events}, _from, state) do
    {:reply, {:error, :invalid_event}, state}
  end

  defp cursor_snapshot(%Session{} = session) do
    %{
      epoch: session.epoch,
      last_seq: session.last_seq,
      committed_seq: session.archived_seq
    }
  end

  defp guard_expected_seq(_session, nil), do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0 and last_seq == expected_seq,
       do: :ok

  defp guard_expected_seq(%Session{last_seq: last_seq}, expected_seq)
       when is_integer(expected_seq) and expected_seq >= 0,
       do: {:error, {:expected_seq_conflict, expected_seq, last_seq}}

  defp guard_expected_seq(_session, _expected_seq), do: {:error, :invalid_event}

  defp append_to_session(%Session{} = session, inputs)
       when is_list(inputs) and inputs != [] do
    do_append_to_session(session, inputs, [], [])
  end

  defp append_to_session(%Session{}, []), do: {:error, :invalid_event}

  defp append_one_to_session(%Session{} = session, input) when is_map(input) do
    case Session.append_event(session, input) do
      {:appended, updated_session, event} ->
        reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
        {:ok, updated_session, reply, event}

      {:deduped, updated_session, seq} ->
        reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
        {:ok, updated_session, reply, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_append_to_session(%Session{} = session, [input | rest], replies, events) do
    case Session.append_event(session, input) do
      {:appended, updated_session, event} ->
        reply = %{seq: event.seq, last_seq: updated_session.last_seq, deduped: false}
        do_append_to_session(updated_session, rest, [reply | replies], [event | events])

      {:deduped, updated_session, seq} ->
        reply = %{seq: seq, last_seq: updated_session.last_seq, deduped: true}
        do_append_to_session(updated_session, rest, [reply | replies], events)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_append_to_session(%Session{} = session, [], replies, events) do
    {:ok, session, Enum.reverse(replies), Enum.reverse(events)}
  end

  defp normalize_session_epoch(%Session{epoch: epoch} = session)
       when is_integer(epoch) and epoch >= 0 do
    session
  end

  defp normalize_session_epoch(%Session{} = session), do: %Session{session | epoch: 0}

  defp apply_owner_epoch(%Session{id: session_id} = session)
       when is_binary(session_id) and session_id != "" do
    normalized_session = normalize_session_epoch(session)
    fallback_epoch = normalize_epoch(normalized_session.epoch)
    owner_epoch = SessionRouter.local_owner_epoch(session_id, fallback_epoch)
    %Session{normalized_session | epoch: owner_epoch}
  end

  defp put_event_epoch(nil, _epoch), do: nil

  defp put_event_epoch(%{seq: seq} = event, epoch)
       when is_integer(seq) and seq > 0 and is_integer(epoch) and epoch >= 0 do
    Map.put(event, :epoch, epoch)
  end

  defp put_events_epoch(events, epoch)
       when is_list(events) and is_integer(epoch) and epoch >= 0 do
    Enum.map(events, &put_event_epoch(&1, epoch))
  end

  defp maybe_event_list(nil), do: []
  defp maybe_event_list(event) when is_map(event), do: [event]

  defp maybe_put_appended_event(_session_id, _tenant_id, nil), do: :ok

  defp maybe_put_appended_event(
         session_id,
         tenant_id,
         %{seq: seq} = event
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(seq) and seq > 0 do
    EventStore.put_event(session_id, tenant_id, event)
  end

  defp maybe_put_appended_events(_session_id, _tenant_id, []), do: :ok

  defp maybe_put_appended_events(
         session_id,
         tenant_id,
         [%{seq: seq} | _rest] = events
       )
       when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
              tenant_id != "" and is_integer(seq) and seq > 0 do
    EventStore.put_events(session_id, tenant_id, events)
  end

  defp maybe_publish_event(_session_id, nil), do: :ok

  defp maybe_publish_event(session_id, %{seq: seq} = event)
       when is_binary(session_id) and session_id != "" and is_integer(seq) and seq > 0 do
    Phoenix.PubSub.broadcast(
      Starcite.PubSub,
      CursorUpdate.topic(session_id),
      CursorUpdate.message(session_id, event, seq)
    )
  end

  defp publish_events(_session_id, []), do: :ok

  defp publish_events(session_id, events)
       when is_binary(session_id) and session_id != "" and is_list(events) do
    Enum.each(events, fn %{seq: seq} = event ->
      :ok = maybe_publish_event(session_id, event)
      seq
    end)

    :ok
  end

  defp evict_archived_events(_session_id, previous_archived_seq, updated_archived_seq)
       when updated_archived_seq <= previous_archived_seq do
    0
  end

  defp evict_archived_events(session_id, _previous_archived_seq, updated_archived_seq) do
    EventStore.delete_below(session_id, updated_archived_seq + 1)
  end

  defp should_apply_replication?(%Session{} = incoming, %Session{} = current) do
    incoming_epoch = normalize_epoch(incoming.epoch)
    current_epoch = normalize_epoch(current.epoch)

    cond do
      incoming_epoch > current_epoch ->
        true

      incoming_epoch < current_epoch ->
        false

      incoming.last_seq > current.last_seq ->
        true

      incoming.last_seq < current.last_seq ->
        false

      incoming.archived_seq > current.archived_seq ->
        true

      incoming.archived_seq < current.archived_seq ->
        false

      true ->
        incoming != current
    end
  end

  defp normalize_epoch(epoch) when is_integer(epoch) and epoch >= 0, do: epoch
  defp normalize_epoch(_epoch), do: 0

  defp decorate_append_reply(%{seq: seq} = reply, epoch, committed_seq)
       when is_integer(seq) and seq >= 0 and is_integer(epoch) and epoch >= 0 and
              is_integer(committed_seq) and committed_seq >= 0 do
    Map.merge(reply, %{
      epoch: epoch,
      cursor: %{epoch: epoch, seq: seq},
      committed_cursor: %{epoch: epoch, seq: committed_seq}
    })
  end
end
