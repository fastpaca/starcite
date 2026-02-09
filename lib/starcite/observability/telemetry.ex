defmodule Starcite.Observability.Telemetry do
  @moduledoc """
  Centralised telemetry helpers so event names and metadata are consistent.

  Exposes small, purpose-built emitters that wrap `:telemetry.execute/3`.

  Note: This is an event substrate - no LLM token budgets or compaction metrics.
  """

  @archive_reconcile_outcomes [:ok, :error]

  @doc """
  Emit an event when an event is appended to a session.

  Measurements:
    - `:payload_bytes` – approximate JSON payload bytes for the event

  Metadata:
    - `:session_id` – session identifier
    - `:type` – event type
    - `:actor` – producer identifier
    - `:source` – optional producer class (for example `agent`, `user`, `system`)
  """
  @spec event_appended(String.t(), String.t(), String.t(), String.t() | nil, non_neg_integer()) ::
          :ok
  def event_appended(session_id, type, actor, source, payload_bytes)
      when is_binary(session_id) and is_binary(type) and type != "" and is_binary(actor) and
             actor != "" and (is_binary(source) or is_nil(source)) and is_integer(payload_bytes) and
             payload_bytes >= 0 do
    :telemetry.execute(
      [:starcite, :events, :append],
      %{payload_bytes: payload_bytes},
      %{session_id: session_id, type: type, actor: actor, source: source}
    )

    :ok
  end

  @doc """
  Emit an event describing a single archive flush tick.

  Measurements:
    - `:elapsed_ms` – time spent in flush loop
    - `:attempted` – rows attempted to write
    - `:inserted` – rows successfully inserted (idempotent)
    - `:pending_events` – total pending refs in archive queue after flush
    - `:pending_sessions` – total sessions with pending refs after flush
  """
  @spec archive_flush(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def archive_flush(
        elapsed_ms,
        attempted,
        inserted,
        pending_events,
        pending_sessions,
        bytes_attempted,
        bytes_inserted
      )
      when is_integer(elapsed_ms) and elapsed_ms >= 0 and is_integer(attempted) and attempted >= 0 and
             is_integer(inserted) and inserted >= 0 and is_integer(pending_events) and
             pending_events >= 0 and
             is_integer(pending_sessions) and pending_sessions >= 0 do
    :telemetry.execute(
      [:starcite, :archive, :flush],
      %{
        elapsed_ms: elapsed_ms,
        attempted: attempted,
        inserted: inserted,
        pending_events: pending_events,
        pending_sessions: pending_sessions,
        bytes_attempted: bytes_attempted,
        bytes_inserted: bytes_inserted
      },
      %{}
    )

    :ok
  end

  @doc """
  Emit an event when an archive acknowledgement is applied by Raft.

  Measurements:
    - `:lag` – last_seq - archived_seq (events)
    - `:trimmed` – number of entries removed from the tail
    - `:tail_size` – entries currently in the tail after trim

  Metadata:
    - `:session_id`
    - `:archived_seq`
    - `:last_seq`
    - `:tail_keep`
  """
  @spec archive_ack_applied(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer()
        ) :: :ok
  def archive_ack_applied(
        session_id,
        last_seq,
        archived_seq,
        trimmed,
        tail_keep,
        tail_size
      )
      when is_binary(session_id) and is_integer(last_seq) and last_seq >= 0 and
             is_integer(archived_seq) and
             archived_seq >= 0 and is_integer(trimmed) and trimmed >= 0 and is_integer(tail_keep) and
             tail_keep > 0 do
    lag = max(last_seq - archived_seq, 0)

    :telemetry.execute(
      [:starcite, :archive, :ack],
      %{lag: lag, trimmed: trimmed, tail_size: tail_size},
      %{
        session_id: session_id,
        archived_seq: archived_seq,
        last_seq: last_seq,
        tail_keep: tail_keep
      }
    )

    :ok
  end

  @doc """
  Emit an event per archived batch (per session).

  Measurements:
    - `:batch_rows` – number of rows in batch
    - `:batch_bytes` – total payload bytes in batch (approx)
    - `:avg_event_bytes` – average payload bytes per event in batch
    - `:pending_after` – pending rows left for this session after trim

  Metadata:
    - `:session_id`
  """
  @spec archive_batch(
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def archive_batch(session_id, batch_rows, batch_bytes, avg_event_bytes, pending_after)
      when is_binary(session_id) and is_integer(batch_rows) and batch_rows >= 0 and
             is_integer(batch_bytes) and batch_bytes >= 0 and is_integer(avg_event_bytes) and
             avg_event_bytes >= 0 and is_integer(pending_after) and pending_after >= 0 do
    :telemetry.execute(
      [:starcite, :archive, :batch],
      %{
        batch_rows: batch_rows,
        batch_bytes: batch_bytes,
        avg_event_bytes: avg_event_bytes,
        pending_after: pending_after
      },
      %{session_id: session_id}
    )

    :ok
  end

  @doc """
  Emit queue age gauge (seconds) across all sessions.
  """
  @spec archive_queue_age(non_neg_integer()) :: :ok
  def archive_queue_age(seconds) when is_integer(seconds) and seconds >= 0 do
    :telemetry.execute(
      [:starcite, :archive, :queue_age],
      %{seconds: seconds},
      %{}
    )

    :ok
  end

  @doc """
  Emit reconciliation scan telemetry.

  Measurements:
    - `:count` – number of scan runs emitted by this call
    - `:elapsed_ms` – scan duration
    - `:discovered` – lagging sessions returned by this scan page
    - `:marked` – sessions marked dirty from this scan page

  Metadata:
    - `:group_id` – scanned Raft group id
    - `:has_more` – whether this group has more pages
    - `:outcome` – scan result classification
  """
  @spec archive_reconcile(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          boolean(),
          :ok | :error
        ) :: :ok
  def archive_reconcile(group_id, elapsed_ms, discovered, marked, has_more, outcome)
      when is_integer(group_id) and group_id >= 0 and is_integer(elapsed_ms) and elapsed_ms >= 0 and
             is_integer(discovered) and discovered >= 0 and is_integer(marked) and marked >= 0 and
             is_boolean(has_more) and outcome in @archive_reconcile_outcomes do
    :telemetry.execute(
      [:starcite, :archive, :reconcile],
      %{count: 1, elapsed_ms: elapsed_ms, discovered: discovered, marked: marked},
      %{group_id: group_id, has_more: has_more, outcome: outcome}
    )

    :ok
  end
end
