defmodule Starcite.Observability.Telemetry do
  @moduledoc """
  Centralised telemetry helpers so event names and metadata are consistent.

  Exposes small, purpose-built emitters that wrap `:telemetry.execute/3`.

  Note: This is an event substrate - no LLM token budgets or compaction metrics.
  """

  @doc """
  Returns whether telemetry emission is globally enabled.
  """
  @spec enabled?() :: boolean()
  def enabled? do
    Application.get_env(:starcite, :telemetry_enabled, true) == true
  end

  @doc """
  Emit an event when an event is appended to a session.

  Measurements:
    - `:payload_bytes` – approximate binary payload bytes for the event

  Metadata:
    - `:session_id` – session identifier
    - `:tenant_id` – normalized tenancy label
    - `:type` – event type
    - `:actor` – producer identifier
    - `:source` – optional producer class (for example `agent`, `user`, `system`)
  """
  @spec event_appended(
          String.t(),
          String.t(),
          String.t(),
          String.t(),
          String.t() | nil,
          non_neg_integer()
        ) :: :ok
  def event_appended(session_id, tenant_id, type, actor, source, payload_bytes)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_binary(type) and type != "" and is_binary(actor) and
             actor != "" and (is_binary(source) or is_nil(source)) and is_integer(payload_bytes) and
             payload_bytes >= 0 do
    execute_if_enabled(
      [:starcite, :events, :append],
      %{payload_bytes: payload_bytes},
      %{session_id: session_id, tenant_id: tenant_id, type: type, actor: actor, source: source}
    )

    :ok
  end

  @doc """
  Emit one ingestion-edge request event.

  Measurements:
    - `:count` – fixed at 1 per request

  Metadata:
    - `:operation` (`:create_session` or `:append_event`)
    - `:tenant_id` – normalized tenancy label
    - `:outcome` (`:ok` or `:error`)
    - `:error_reason` – reason atom for failures (`:none` for success)
  """
  @type ingest_edge_operation :: :create_session | :append_event
  @type ingest_edge_outcome :: :ok | :error
  @type ingest_edge_error_reason :: atom()

  @spec ingest_edge(ingest_edge_operation(), String.t(), :ok) :: :ok
  def ingest_edge(operation, tenant_id, :ok), do: ingest_edge(operation, tenant_id, :ok, :none)

  @spec ingest_edge(
          ingest_edge_operation(),
          String.t(),
          ingest_edge_outcome(),
          ingest_edge_error_reason()
        ) :: :ok
  def ingest_edge(operation, tenant_id, :ok, _error_reason)
      when operation in [:create_session, :append_event] and is_binary(tenant_id) and
             tenant_id != "" do
    execute_if_enabled(
      [:starcite, :ingest, :edge],
      %{count: 1},
      %{
        operation: operation,
        tenant_id: tenant_id,
        outcome: :ok,
        error_reason: :none
      }
    )

    :ok
  end

  def ingest_edge(operation, tenant_id, :error, error_reason)
      when operation in [:create_session, :append_event] and is_binary(tenant_id) and
             tenant_id != "" and is_atom(error_reason) do
    execute_if_enabled(
      [:starcite, :ingest, :edge],
      %{count: 1},
      %{
        operation: operation,
        tenant_id: tenant_id,
        outcome: :error,
        error_reason: error_reason
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one write request outcome.

  Measurements:
    - `:count` – fixed at 1 per request
    - `:duration_ms` – elapsed request time in milliseconds

  Metadata:
    - `:operation` (`:append_event` or `:append_events`)
    - `:phase` (`:ack`)
    - `:outcome` (`:ok`, `:error`, or `:timeout`)
  """
  @spec request(
          :append_event | :append_events,
          :ack,
          :ok | :error | :timeout,
          non_neg_integer()
        ) :: :ok
  def request(operation, phase, outcome, duration_ms)
      when operation in [:append_event, :append_events] and phase in [:ack] and
             outcome in [:ok, :error, :timeout] and is_integer(duration_ms) and duration_ms >= 0 do
    execute_if_enabled(
      [:starcite, :request],
      %{count: 1, duration_ms: duration_ms},
      %{operation: operation, phase: phase, outcome: outcome}
    )

    :ok
  end

  @doc """
  Emit telemetry for one tail read delivery outcome.

  Measurements:
    - `:count` – fixed at 1 per delivery attempt
    - `:duration_ms` – elapsed delivery time in milliseconds

  Metadata:
    - `:operation` (`:tail_catchup` or `:tail_live`)
    - `:phase` (`:deliver`)
    - `:outcome` (`:ok`, `:error`, or `:timeout`)
  """
  @spec read(
          :tail_catchup | :tail_live,
          :deliver,
          :ok | :error | :timeout,
          non_neg_integer()
        ) :: :ok
  def read(operation, phase, outcome, duration_ms)
      when operation in [:tail_catchup, :tail_live] and phase in [:deliver] and
             outcome in [:ok, :error, :timeout] and is_integer(duration_ms) and duration_ms >= 0 do
    execute_if_enabled(
      [:starcite, :read],
      %{count: 1, duration_ms: duration_ms},
      %{operation: operation, phase: phase, outcome: outcome}
    )

    :ok
  end

  @doc """
  Emit telemetry for one event-store write.

  Measurements:
    - `:count` – fixed at 1 per write
    - `:payload_bytes` – payload size in bytes

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:seq`
  """
  @spec event_store_write(
          String.t(),
          String.t(),
          pos_integer(),
          non_neg_integer()
        ) :: :ok
  def event_store_write(session_id, tenant_id, seq, payload_bytes)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(seq) and seq > 0 and is_integer(payload_bytes) and
             payload_bytes >= 0 do
    execute_if_enabled(
      [:starcite, :event_store, :write],
      %{count: 1, payload_bytes: payload_bytes},
      %{session_id: session_id, tenant_id: tenant_id, seq: seq}
    )

    :ok
  end

  @doc """
  Emit telemetry when an event-store write is rejected due to capacity.

  Measurements:
    - `:count` – fixed at 1 per rejection
    - `:current_memory_bytes` – current ETS table memory usage in bytes

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:max_memory_bytes`
    - `:reason` (`:memory_limit`)
  """
  @spec event_store_backpressure(
          String.t(),
          String.t(),
          non_neg_integer(),
          pos_integer(),
          :memory_limit
        ) :: :ok
  def event_store_backpressure(
        session_id,
        tenant_id,
        current_memory_bytes,
        max_memory_bytes,
        reason
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(current_memory_bytes) and current_memory_bytes >= 0 and
             is_integer(max_memory_bytes) and max_memory_bytes > 0 and reason in [:memory_limit] do
    execute_if_enabled(
      [:starcite, :event_store, :backpressure],
      %{
        count: 1,
        current_memory_bytes: current_memory_bytes
      },
      %{
        session_id: session_id,
        tenant_id: tenant_id,
        max_memory_bytes: max_memory_bytes,
        reason: reason
      }
    )

    :ok
  end

  @doc """
  Emit telemetry when a cursor update is published on PubSub.

  Measurements:
    - `:count` – fixed at 1 per emitted update
    - `:lag` – `last_seq - seq` at emission time

  Metadata:
    - `:session_id`
    - `:tenant_id`
  """
  @spec cursor_update_emitted(String.t(), String.t(), non_neg_integer(), non_neg_integer()) :: :ok
  def cursor_update_emitted(session_id, tenant_id, seq, last_seq)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(seq) and seq >= 0 and is_integer(last_seq) and
             last_seq >= seq do
    execute_if_enabled(
      [:starcite, :cursor, :update],
      %{count: 1, lag: last_seq - seq},
      %{session_id: session_id, tenant_id: tenant_id}
    )

    :ok
  end

  @doc """
  Emit telemetry for tail cursor event lookups.

  Measurements:
    - `:count` – fixed at 1 per lookup

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:seq`
    - `:source` (`:ets` or `:storage`)
    - `:result` (`:hit` or `:miss`)
  """
  @spec tail_cursor_lookup(
          String.t(),
          String.t(),
          pos_integer(),
          :ets | :storage,
          :hit | :miss
        ) :: :ok
  def tail_cursor_lookup(session_id, tenant_id, seq, source, result)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(seq) and seq > 0 and source in [:ets, :storage] and
             result in [:hit, :miss] do
    execute_if_enabled(
      [:starcite, :tail, :cursor_lookup],
      %{count: 1},
      %{session_id: session_id, tenant_id: tenant_id, seq: seq, source: source, result: result}
    )

    :ok
  end

  @doc """
  Emit telemetry for one Raft command execution outcome.

  Measurements:
    - `:count` – fixed at 1 per command execution

  Metadata:
    - `:command` (`:create_session`, `:append_event`, `:append_events`, `:ack_archived`, `:other`)
    - `:tenant_id` – normalized tenancy label
    - `:outcome`
      (`:local_ok`, `:local_error`, `:local_timeout`, `:leader_retry_ok`,
      `:leader_retry_error`, or `:leader_retry_timeout`)
  """
  @spec raft_command_result(
          :create_session | :append_event | :append_events | :ack_archived | :other,
          :local_ok
          | :local_error
          | :local_timeout
          | :leader_retry_ok
          | :leader_retry_error
          | :leader_retry_timeout,
          String.t()
        ) :: :ok
  def raft_command_result(command, outcome, tenant_id)
      when command in [:create_session, :append_event, :append_events, :ack_archived, :other] and
             outcome in [
               :local_ok,
               :local_error,
               :local_timeout,
               :leader_retry_ok,
               :leader_retry_error,
               :leader_retry_timeout
             ] and is_binary(tenant_id) and tenant_id != "" do
    execute_if_enabled(
      [:starcite, :raft, :command],
      %{count: 1},
      %{command: command, outcome: outcome, tenant_id: tenant_id}
    )

    :ok
  end

  @doc """
  Emit telemetry for one write/read-path routing decision before execution.

  Measurements:
    - `:count` – fixed at 1 per routed request
    - `:replica_count` – configured replicas for the group
    - `:ready_count` – currently ready replicas for the group

  Metadata:
    - `:group_id`
    - `:tenant_id`
    - `:target` (`:local` or `:remote`)
    - `:prefer_leader` (`true` or `false`)
    - `:leader_hint` (`:disabled`, `:hit`, or `:miss`)
  """
  @spec routing_decision(
          non_neg_integer(),
          String.t(),
          :local | :remote,
          boolean(),
          :disabled | :hit | :miss,
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def routing_decision(
        group_id,
        tenant_id,
        target,
        prefer_leader,
        leader_hint,
        replica_count,
        ready_count
      )
      when is_integer(group_id) and group_id >= 0 and is_binary(tenant_id) and tenant_id != "" and
             target in [:local, :remote] and is_boolean(prefer_leader) and
             leader_hint in [:disabled, :hit, :miss] and is_integer(replica_count) and
             replica_count >= 0 and is_integer(ready_count) and ready_count >= 0 do
    execute_if_enabled(
      [:starcite, :routing, :decision],
      %{count: 1, replica_count: replica_count, ready_count: ready_count},
      %{
        group_id: group_id,
        tenant_id: tenant_id,
        target: target,
        prefer_leader: prefer_leader,
        leader_hint: leader_hint
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one routing execution result after request execution.

  Measurements:
    - `:count` – fixed at 1 per routed request
    - `:attempts` – replica attempts performed for this request
    - `:retries` – additional attempts beyond first (`max(attempts - 1, 0)`)
    - `:leader_redirects` – number of leader redirect hints observed

  Metadata:
    - `:group_id`
    - `:tenant_id`
    - `:path` (`:local` or `:remote`)
    - `:outcome` (`:ok`, `:error`, `:timeout`, `:badrpc`, `:no_candidates`, or `:other`)
  """
  @spec routing_result(
          non_neg_integer(),
          String.t(),
          :local | :remote,
          :ok | :error | :timeout | :badrpc | :no_candidates | :other,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def routing_result(group_id, tenant_id, path, outcome, attempts, retries, leader_redirects)
      when is_integer(group_id) and group_id >= 0 and is_binary(tenant_id) and tenant_id != "" and
             path in [:local, :remote] and
             outcome in [:ok, :error, :timeout, :badrpc, :no_candidates, :other] and
             is_integer(attempts) and attempts >= 0 and is_integer(retries) and retries >= 0 and
             is_integer(leader_redirects) and leader_redirects >= 0 do
    execute_if_enabled(
      [:starcite, :routing, :result],
      %{
        count: 1,
        attempts: attempts,
        retries: retries,
        leader_redirects: leader_redirects
      },
      %{group_id: group_id, tenant_id: tenant_id, path: path, outcome: outcome}
    )

    :ok
  end

  @doc """
  Emit an event describing a single archive flush tick.

  Measurements:
    - `:elapsed_ms` – time spent in flush loop
    - `:attempted` – rows attempted to write
    - `:inserted` – rows successfully inserted (idempotent)
    - `:pending_events` – estimated unarchived events after flush
    - `:pending_sessions` – sessions still pending archival after flush
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
    execute_if_enabled(
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
    - `:tenant_id`
    - `:archived_seq`
    - `:last_seq`
    - `:tail_keep`
  """
  @spec archive_ack_applied(
          String.t(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          non_neg_integer()
        ) :: :ok
  def archive_ack_applied(
        session_id,
        tenant_id,
        last_seq,
        archived_seq,
        trimmed,
        tail_keep,
        tail_size
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(last_seq) and last_seq >= 0 and
             is_integer(archived_seq) and archived_seq >= 0 and is_integer(trimmed) and
             trimmed >= 0 and is_integer(tail_keep) and tail_keep > 0 and
             is_integer(tail_size) and tail_size >= 0 do
    lag = max(last_seq - archived_seq, 0)

    execute_if_enabled(
      [:starcite, :archive, :ack],
      %{lag: lag, trimmed: trimmed, tail_size: tail_size},
      %{
        session_id: session_id,
        tenant_id: tenant_id,
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
    - `:tenant_id`
  """
  @spec archive_batch(
          String.t(),
          String.t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def archive_batch(
        session_id,
        tenant_id,
        batch_rows,
        batch_bytes,
        avg_event_bytes,
        pending_after
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" and
             is_integer(batch_rows) and batch_rows >= 0 and is_integer(batch_bytes) and
             batch_bytes >= 0 and is_integer(avg_event_bytes) and avg_event_bytes >= 0 and
             is_integer(pending_after) and pending_after >= 0 do
    execute_if_enabled(
      [:starcite, :archive, :batch],
      %{
        batch_rows: batch_rows,
        batch_bytes: batch_bytes,
        avg_event_bytes: avg_event_bytes,
        pending_after: pending_after
      },
      %{session_id: session_id, tenant_id: tenant_id}
    )

    :ok
  end

  @doc """
  Emit archive backlog age gauge (seconds) across all pending sessions.
  """
  @spec archive_queue_age(non_neg_integer()) :: :ok
  def archive_queue_age(seconds) when is_integer(seconds) and seconds >= 0 do
    execute_if_enabled(
      [:starcite, :archive, :queue_age],
      %{seconds: seconds},
      %{}
    )

    :ok
  end

  defp execute_if_enabled(event_name, measurements, metadata)
       when is_list(event_name) and is_map(measurements) and is_map(metadata) do
    if enabled?() do
      :telemetry.execute(event_name, measurements, metadata)
    end

    :ok
  end
end
