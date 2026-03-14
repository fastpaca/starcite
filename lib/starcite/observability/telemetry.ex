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
    - `:node` – local Erlang node name as a string
    - `:operation` (`:append_event` or `:append_events`)
    - `:phase` (`:total`, `:route`, `:dispatch`, or `:ack`)
    - `:outcome` (`:ok`, `:error`, or `:timeout`)
    - `:error_reason` – stable failure class atom (`:none` for success)
  """
  @spec request(
          :append_event | :append_events,
          :total | :route | :dispatch | :ack,
          :ok | :error | :timeout,
          non_neg_integer()
        ) :: :ok
  def request(operation, phase, outcome, duration_ms)
      when operation in [:append_event, :append_events] and
             phase in [:total, :route, :dispatch, :ack] and
             outcome in [:ok, :error, :timeout] and is_integer(duration_ms) and duration_ms >= 0 do
    request(operation, phase, outcome, duration_ms, :none, current_node_name())
  end

  @spec request_result(
          :append_event | :append_events,
          :total | :route | :dispatch | :ack,
          {:ok, term()} | {:error, term()} | {:timeout, term()},
          non_neg_integer()
        ) :: :ok
  def request_result(operation, phase, result, duration_ms)
      when operation in [:append_event, :append_events] and
             phase in [:total, :route, :dispatch, :ack] and
             is_integer(duration_ms) and duration_ms >= 0 do
    request(
      operation,
      phase,
      request_outcome(result),
      duration_ms,
      request_error_reason(result)
    )
  end

  @spec request(
          :append_event | :append_events,
          :total | :route | :dispatch | :ack,
          :ok | :error | :timeout,
          non_neg_integer(),
          atom()
        ) :: :ok
  def request(operation, phase, outcome, duration_ms, error_reason)
      when operation in [:append_event, :append_events] and
             phase in [:total, :route, :dispatch, :ack] and
             outcome in [:ok, :error, :timeout] and is_integer(duration_ms) and duration_ms >= 0 and
             is_atom(error_reason) do
    request(operation, phase, outcome, duration_ms, error_reason, current_node_name())
  end

  @spec request(
          :append_event | :append_events,
          :total | :route | :dispatch | :ack,
          :ok | :error | :timeout,
          non_neg_integer(),
          atom(),
          String.t()
        ) :: :ok
  def request(operation, phase, outcome, duration_ms, error_reason, node_name)
      when operation in [:append_event, :append_events] and
             phase in [:total, :route, :dispatch, :ack] and
             outcome in [:ok, :error, :timeout] and is_integer(duration_ms) and duration_ms >= 0 and
             is_atom(error_reason) and is_binary(node_name) and node_name != "" do
    execute_if_enabled(
      [:starcite, :request],
      %{count: 1, duration_ms: duration_ms},
      %{
        node: node_name,
        operation: operation,
        phase: phase,
        outcome: outcome,
        error_reason: error_reason
      }
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
  Emit telemetry for one auth-stage outcome.

  Measurements:
    - `:count` - fixed at 1 per auth stage
    - `:duration_ms` - elapsed stage time in milliseconds

  Metadata:
    - `:stage` (`:plug`, `:jwt_verify`, or `:jwks_fetch`)
    - `:mode` (`:none` or `:jwt`)
    - `:outcome` (`:ok` or `:error`)
    - `:error_reason` (`:none` for success)
    - `:source` (`:none`, `:cache`, or `:refresh`)
  """
  @spec auth(
          :plug | :jwt_verify | :jwks_fetch,
          :none | :jwt,
          :ok | :error,
          non_neg_integer(),
          atom(),
          :none | :cache | :refresh
        ) :: :ok
  def auth(stage, mode, outcome, duration_ms, error_reason, source \\ :none)
      when stage in [:plug, :jwt_verify, :jwks_fetch] and mode in [:none, :jwt] and
             outcome in [:ok, :error] and is_integer(duration_ms) and duration_ms >= 0 and
             is_atom(error_reason) and source in [:none, :cache, :refresh] do
    execute_if_enabled(
      [:starcite, :auth],
      %{count: 1, duration_ms: duration_ms},
      %{
        stage: stage,
        mode: mode,
        outcome: outcome,
        error_reason: error_reason,
        source: source
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one explicit edge-stage timing.

  Measurements:
    - `:count` - fixed at 1 per stage
    - `:duration_ms` - elapsed stage time in milliseconds

  Metadata:
    - `:stage` (`:controller_entry`)
    - `:method`
  """
  @spec edge_stage(:controller_entry, String.t(), non_neg_integer()) :: :ok
  def edge_stage(stage, method, duration_ms)
      when stage in [:controller_entry] and is_binary(method) and method != "" and
             is_integer(duration_ms) and duration_ms >= 0 do
    execute_if_enabled(
      [:starcite, :edge, :stage],
      %{count: 1, duration_ms: duration_ms},
      %{stage: stage, method: method}
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
    - `:source` (`:embedded`, `:event_store`, or `:storage`)
    - `:result` (`:hit` or `:miss`)
  """
  @spec tail_cursor_lookup(
          String.t(),
          String.t(),
          pos_integer(),
          :embedded | :event_store | :storage,
          :hit | :miss
        ) :: :ok
  def tail_cursor_lookup(session_id, tenant_id, seq, source, result)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and is_integer(seq) and seq > 0 and
             source in [:embedded, :event_store, :storage] and
             result in [:hit, :miss] do
    execute_if_enabled(
      [:starcite, :tail, :cursor_lookup],
      %{count: 1},
      %{session_id: session_id, tenant_id: tenant_id, seq: seq, source: source, result: result}
    )

    :ok
  end

  @doc """
  Emit telemetry when a tail socket receives a committed cursor update.

  Measurements:
    - `:count` – fixed at 1 per received update
    - `:publish_to_receive_ms` – wall-clock lag from commit publish to socket receipt
    - `:replay_queue_size` – queued replay events before processing this update
    - `:live_buffer_size` – buffered live updates before processing this update

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:seq`
    - `:mode` (`:stale`, `:live_fast_path`, or `:buffered`)
  """
  @spec tail_visibility(
          String.t(),
          String.t(),
          pos_integer(),
          :stale | :live_fast_path | :buffered,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def tail_visibility(
        session_id,
        tenant_id,
        seq,
        mode,
        publish_to_receive_ms,
        replay_queue_size,
        live_buffer_size
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" and
             is_integer(seq) and seq > 0 and mode in [:stale, :live_fast_path, :buffered] and
             is_integer(publish_to_receive_ms) and publish_to_receive_ms >= 0 and
             is_integer(replay_queue_size) and replay_queue_size >= 0 and
             is_integer(live_buffer_size) and live_buffer_size >= 0 do
    execute_if_enabled(
      [:starcite, :tail, :visibility],
      %{
        count: 1,
        publish_to_receive_ms: publish_to_receive_ms,
        replay_queue_size: replay_queue_size,
        live_buffer_size: live_buffer_size
      },
      %{
        session_id: session_id,
        tenant_id: tenant_id,
        seq: seq,
        mode: mode
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one routing execution result.

  Measurements:
    - `:count` – fixed at 1 per routed request
    - `:refreshes` – authoritative assignment refreshes performed before completion

  Metadata:
    - `:node` – cluster node name as a string
    - `:target` (`:local`, `:remote`, or `:unassigned`)
    - `:outcome` (`:ok`, `:error`, or `:timeout`)
    - `:error_reason` – stable routing failure reason (`:none` for success)
  """
  @spec routing_result(
          :local | :remote | :unassigned,
          term(),
          non_neg_integer()
        ) :: :ok
  def routing_result(target, result, refreshes)
      when target in [:local, :remote, :unassigned] and is_integer(refreshes) and refreshes >= 0 do
    execute_if_enabled(
      [:starcite, :routing, :result],
      %{count: 1, refreshes: refreshes},
      %{
        node: current_node_name(),
        target: target,
        outcome: routing_outcome(result),
        error_reason: routing_error_reason(result)
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for a routing node lifecycle state transition.

  Measurements:
    - `:count` - fixed at 1 per transition

  Metadata:
    - `:node` - cluster node name as a string
    - `:from` - previous node lifecycle state
    - `:to` - next node lifecycle state
    - `:source` - transition initiator (`:maintenance`, `:startup`, `:watcher`, or `:shutdown`)
  """
  @type routing_node_state :: :unknown | :ready | :draining | :drained

  @spec routing_node_state(node(), routing_node_state(), :ready | :draining | :drained, atom()) ::
          :ok
  def routing_node_state(node, from, to, source)
      when is_atom(node) and from in [:unknown, :ready, :draining, :drained] and
             to in [:ready, :draining, :drained] and is_atom(source) do
    execute_if_enabled(
      [:starcite, :routing, :node_state],
      %{count: 1},
      %{node: Atom.to_string(node), from: from, to: to, source: source}
    )

    :ok
  end

  @doc """
  Emit telemetry for one ownership fence on the routing/data plane.

  Measurements:
    - `:count` - fixed at 1 per fence

  Metadata:
    - `:node` - local node name
    - `:session_id`
    - `:source` (`:session_router` or `:session_log`)
    - `:reason` (`:not_leader`, `:ownership_transfer_in_progress`, or `:not_owner`)
  """
  @spec routing_fence(String.t(), :session_router | :session_log, atom()) :: :ok
  def routing_fence(session_id, source, reason)
      when is_binary(session_id) and session_id != "" and
             source in [:session_router, :session_log] and
             reason in [:not_leader, :ownership_transfer_in_progress, :not_owner] do
    execute_if_enabled(
      [:starcite, :routing, :fence],
      %{count: 1},
      %{
        node: current_node_name(),
        session_id: session_id,
        source: source,
        reason: reason
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one append lifecycle boundary inside the session log.

  Measurements:
    - `:count` - fixed at 1 per boundary event
    - `:event_count` - number of events in the append operation

  Metadata:
    - `:node`
    - `:session_id`
    - `:tenant_id`
    - `:stage` (`:before_quorum_replicate` or `:after_commit_before_reply`)
  """
  @spec append_boundary(
          String.t(),
          String.t(),
          :before_quorum_replicate | :after_commit_before_reply,
          non_neg_integer()
        ) :: :ok
  def append_boundary(session_id, tenant_id, stage, event_count)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and stage in [:before_quorum_replicate, :after_commit_before_reply] and
             is_integer(event_count) and event_count >= 0 do
    execute_if_enabled(
      [:starcite, :append, :boundary],
      %{count: 1, event_count: event_count},
      %{
        node: current_node_name(),
        session_id: session_id,
        tenant_id: tenant_id,
        stage: stage
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one routing transfer lifecycle event.

  Measurements:
    - `:count` - fixed at 1 per transfer event

  Metadata:
    - `:session_id`
    - `:source_node`
    - `:target_node`
    - `:action` (`:started` or `:committed`)
  """
  @spec routing_transfer(String.t(), node(), node(), :started | :committed) :: :ok
  def routing_transfer(session_id, source_node, target_node, action)
      when is_binary(session_id) and session_id != "" and is_atom(source_node) and
             is_atom(target_node) and action in [:started, :committed] do
    execute_if_enabled(
      [:starcite, :routing, :transfer],
      %{count: 1},
      %{
        session_id: session_id,
        source_node: Atom.to_string(source_node),
        target_node: Atom.to_string(target_node),
        action: action
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one routing failover activation.

  Measurements:
    - `:count` - fixed at 1 per failover activation

  Metadata:
    - `:session_id`
    - `:source_node`
    - `:target_node`
    - `:reason` (`:lease_expired`)
  """
  @spec routing_failover(String.t(), node(), node(), :lease_expired) :: :ok
  def routing_failover(session_id, source_node, target_node, reason)
      when is_binary(session_id) and session_id != "" and is_atom(source_node) and
             is_atom(target_node) and reason in [:lease_expired] do
    execute_if_enabled(
      [:starcite, :routing, :failover],
      %{count: 1},
      %{
        session_id: session_id,
        source_node: Atom.to_string(source_node),
        target_node: Atom.to_string(target_node),
        reason: reason
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one routing invariant violation.

  Measurements:
    - `:count` - fixed at 1 per violation

  Metadata:
    - `:session_id`
    - `:source` (`:compare_and_swap_assignment`, `:commit_transfer`, or `:failover_assignment`)
    - `:reason`
  """
  @spec routing_invariant(
          String.t(),
          :compare_and_swap_assignment | :commit_transfer | :failover_assignment,
          :assignment_epoch_regression
          | :commit_transfer_invalid_state
          | :commit_transfer_target_not_ready
          | :failover_target_not_ready
        ) :: :ok
  def routing_invariant(session_id, source, reason)
      when is_binary(session_id) and session_id != "" and
             source in [:compare_and_swap_assignment, :commit_transfer, :failover_assignment] and
             reason in [
               :assignment_epoch_regression,
               :commit_transfer_invalid_state,
               :commit_transfer_target_not_ready,
               :failover_target_not_ready
             ] do
    execute_if_enabled(
      [:starcite, :routing, :invariant],
      %{count: 1},
      %{
        node: current_node_name(),
        session_id: session_id,
        source: source,
        reason: reason
      }
    )

    :ok
  end

  @doc """
  Emit telemetry for one data-plane invariant violation.

  Measurements:
    - `:count` - fixed at 1 per violation

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:source` (`:publish_events`)
    - `:reason` (`:publication_watermark_regression`)
  """
  @spec data_plane_invariant(
          String.t(),
          String.t(),
          :publish_events,
          :publication_watermark_regression
        ) :: :ok
  def data_plane_invariant(session_id, tenant_id, source, reason)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and
             tenant_id != "" and source in [:publish_events] and
             reason in [:publication_watermark_regression] do
    execute_if_enabled(
      [:starcite, :data_plane, :invariant],
      %{count: 1},
      %{
        node: current_node_name(),
        session_id: session_id,
        tenant_id: tenant_id,
        source: source,
        reason: reason
      }
    )

    :ok
  end

  @doc """
  Emit a successful session create event.
  """
  @spec session_create(String.t(), String.t()) :: :ok
  def session_create(session_id, tenant_id)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" do
    execute_if_enabled(
      [:starcite, :session, :create],
      %{count: 1},
      %{session_id: session_id, tenant_id: tenant_id}
    )

    :ok
  end

  @doc """
  Emit one session freeze outcome.
  """
  @spec session_freeze(String.t(), String.t(), :ok | :conflict | :error, atom()) :: :ok
  def session_freeze(session_id, tenant_id, outcome, reason)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" and
             outcome in [:ok, :conflict, :error] and is_atom(reason) do
    execute_if_enabled(
      [:starcite, :session, :freeze],
      %{count: 1},
      %{session_id: session_id, tenant_id: tenant_id, outcome: outcome, reason: reason}
    )

    :ok
  end

  @doc """
  Emit one session hydrate outcome.
  """
  @spec session_hydrate(String.t(), String.t(), :ok | :error, atom()) :: :ok
  def session_hydrate(session_id, tenant_id, outcome, reason)
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" and
             outcome in [:ok, :error] and is_atom(reason) do
    execute_if_enabled(
      [:starcite, :session, :hydrate],
      %{count: 1},
      %{session_id: session_id, tenant_id: tenant_id, outcome: outcome, reason: reason}
    )

    :ok
  end

  @doc """
  Emit telemetry for one in-memory session replication attempt.

  Measurements:
    - `:count` – fixed at 1 per attempt
    - `:duration_ms` – elapsed replication time
    - `:standby_count` – number of standby nodes considered
    - `:required_remote_acks` – quorum-derived remote ack requirement
    - `:successful_remote_acks` – remote acks obtained
    - `:failure_count` – failed remote attempts

  Metadata:
    - `:session_id`
    - `:tenant_id`
    - `:outcome` (`:ok` or `:quorum_not_met`)
    - `:failure_reason` (`:none`, `:badrpc`, `:timeout`, `:error`, etc.)
  """
  @spec session_replication(
          String.t(),
          String.t(),
          :ok | :quorum_not_met,
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          atom()
        ) :: :ok
  def session_replication(
        session_id,
        tenant_id,
        outcome,
        duration_ms,
        standby_count,
        required_remote_acks,
        successful_remote_acks,
        failure_count,
        failure_reason
      )
      when is_binary(session_id) and session_id != "" and is_binary(tenant_id) and tenant_id != "" and
             outcome in [:ok, :quorum_not_met] and is_integer(duration_ms) and duration_ms >= 0 and
             is_integer(standby_count) and standby_count >= 0 and
             is_integer(required_remote_acks) and required_remote_acks >= 0 and
             is_integer(successful_remote_acks) and successful_remote_acks >= 0 and
             is_integer(failure_count) and failure_count >= 0 and is_atom(failure_reason) do
    execute_if_enabled(
      [:starcite, :replication, :session],
      %{
        count: 1,
        duration_ms: duration_ms,
        standby_count: standby_count,
        required_remote_acks: required_remote_acks,
        successful_remote_acks: successful_remote_acks,
        failure_count: failure_count
      },
      %{
        session_id: session_id,
        tenant_id: tenant_id,
        outcome: outcome,
        failure_reason: failure_reason
      }
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
  Emit an event when an archive acknowledgement trims committed hot-tail events.

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

  defp current_node_name do
    Node.self()
    |> Atom.to_string()
  end

  defp request_outcome({:ok, _reply}), do: :ok
  defp request_outcome({:timeout, _reason}), do: :timeout
  defp request_outcome({:error, {:timeout, _reason}}), do: :timeout
  defp request_outcome(_result), do: :error

  defp request_error_reason({:ok, _reply}), do: :none
  defp request_error_reason({:timeout, _reason}), do: :timeout
  defp request_error_reason({:error, {:timeout, _reason}}), do: :timeout
  defp request_error_reason({:error, {:expected_seq_conflict, _, _}}), do: :expected_seq_conflict
  defp request_error_reason({:error, {:expected_seq_conflict, _}}), do: :expected_seq_conflict

  defp request_error_reason({:error, {:producer_seq_conflict, _, _, _}}),
    do: :producer_seq_conflict

  defp request_error_reason({:error, :producer_replay_conflict}), do: :producer_replay_conflict

  defp request_error_reason({:error, {:routing_rpc_failed, _node, _reason}}),
    do: :routing_rpc_failed

  defp request_error_reason({:error, {:replication_quorum_not_met, _details}}),
    do: :replication_quorum_not_met

  defp request_error_reason({:error, {:no_available_replicas, _failures}}),
    do: :no_available_replicas

  defp request_error_reason({:error, reason}) when is_atom(reason), do: reason
  defp request_error_reason({:error, _reason}), do: :error
  defp request_error_reason(_result), do: :error

  defp routing_outcome({:ok, _reply}), do: :ok
  defp routing_outcome({:timeout, _reason}), do: :timeout
  defp routing_outcome({:error, {:timeout, _reason}}), do: :timeout
  defp routing_outcome({:error, _reason}), do: :error
  defp routing_outcome(_result), do: :ok

  defp routing_error_reason({:ok, _reply}), do: :none
  defp routing_error_reason({:timeout, _reason}), do: :timeout
  defp routing_error_reason({:error, {:timeout, _reason}}), do: :timeout

  defp routing_error_reason({:error, {:routing_rpc_failed, _node, reason}})
       when is_atom(reason),
       do: reason

  defp routing_error_reason({:error, {:routing_rpc_failed, _node, _reason}}),
    do: :routing_rpc_failed

  defp routing_error_reason({:error, {:no_available_replicas, _failures}}),
    do: :no_available_replicas

  defp routing_error_reason({:error, reason}) when is_atom(reason), do: reason
  defp routing_error_reason({:error, _reason}), do: :error
  defp routing_error_reason(_result), do: :none
end
