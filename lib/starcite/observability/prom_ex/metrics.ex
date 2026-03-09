defmodule Starcite.Observability.PromEx.Metrics do
  @moduledoc false

  use PromEx.Plugin

  import Telemetry.Metrics, only: [counter: 2, distribution: 2, last_value: 2]

  @impl true
  def event_metrics(_opts) do
    [
      ingestion_metrics(),
      routing_metrics(),
      request_slo_metrics(),
      archive_metrics(),
      session_metrics(),
      event_store_metrics()
    ]
  end

  defp ingestion_metrics do
    Event.build(
      :starcite_ingestion_edge_metrics,
      [
        counter("starcite_ingest_edge_total",
          event_name: [:starcite, :ingest, :edge],
          measurement: :count,
          description: "Total ingestion-edge requests by operation, outcome, and error reason",
          tags: [:operation, :outcome, :error_reason, :tenant_id]
        )
      ]
    )
  end

  defp archive_metrics do
    Event.build(
      :starcite_archive_event_metrics,
      [
        last_value("starcite_archive_pending_rows",
          event_name: [:starcite, :archive, :flush],
          measurement: :pending_events,
          description: "Estimated unarchived events pending after flush"
        ),
        last_value("starcite_archive_pending_sessions",
          event_name: [:starcite, :archive, :flush],
          measurement: :pending_sessions,
          description: "Sessions still pending archival after flush"
        ),
        distribution("starcite_archive_flush_duration_ms",
          event_name: [:starcite, :archive, :flush],
          measurement: :elapsed_ms,
          description: "Archive flush tick duration",
          unit: {:native, :millisecond},
          reporter_options: [
            buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000]
          ]
        ),
        counter("starcite_archive_attempted_total",
          event_name: [:starcite, :archive, :flush],
          measurement: :attempted,
          description: "Total rows attempted to archive"
        ),
        counter("starcite_archive_inserted_total",
          event_name: [:starcite, :archive, :flush],
          measurement: :inserted,
          description: "Total rows inserted into archive"
        ),
        counter("starcite_archive_bytes_attempted_total",
          event_name: [:starcite, :archive, :flush],
          measurement: :bytes_attempted,
          description: "Total payload bytes attempted to archive"
        ),
        counter("starcite_archive_bytes_inserted_total",
          event_name: [:starcite, :archive, :flush],
          measurement: :bytes_inserted,
          description: "Total payload bytes successfully archived"
        ),
        distribution("starcite_archive_batch_rows",
          event_name: [:starcite, :archive, :batch],
          measurement: :batch_rows,
          description: "Rows per archive batch (per session)",
          reporter_options: [buckets: [1, 10, 50, 100, 500, 1_000, 2_000, 5_000, 10_000]],
          tags: [:tenant_id]
        ),
        distribution("starcite_archive_batch_bytes",
          event_name: [:starcite, :archive, :batch],
          measurement: :batch_bytes,
          description: "Payload bytes per archive batch (per session)",
          reporter_options: [buckets: [1_024, 10_240, 102_400, 1_048_576, 10_485_760, 52_428_800]],
          tags: [:tenant_id]
        ),
        distribution("starcite_archive_event_bytes",
          event_name: [:starcite, :archive, :batch],
          measurement: :avg_event_bytes,
          description: "Average event bytes per batch (per session)",
          reporter_options: [buckets: [100, 1_000, 5_000, 10_000, 50_000, 100_000, 1_000_000]],
          tags: [:tenant_id]
        ),
        last_value("starcite_archive_oldest_age_seconds",
          event_name: [:starcite, :archive, :queue_age],
          measurement: :seconds,
          description: "Oldest pending archive backlog age across sessions (seconds)"
        ),
        last_value("starcite_archive_lag",
          event_name: [:starcite, :archive, :ack],
          measurement: :lag,
          description: "Archive lag (events) per session",
          tags: [:tenant_id]
        ),
        last_value("starcite_archive_tail_size",
          event_name: [:starcite, :archive, :ack],
          measurement: :tail_size,
          description: "In-memory hot-tail size after trim",
          tags: [:tenant_id]
        ),
        counter("starcite_archive_trimmed_total",
          event_name: [:starcite, :archive, :ack],
          measurement: :trimmed,
          description: "Total entries trimmed from the in-memory hot tail",
          tags: [:tenant_id]
        )
      ]
    )
  end

  defp request_slo_metrics do
    Event.build(
      :starcite_request_slo_metrics,
      [
        counter("starcite_request_total",
          event_name: [:starcite, :request],
          measurement: :count,
          description: "Write request outcomes by node, operation, phase, and outcome",
          tags: [:node, :operation, :phase, :outcome]
        ),
        distribution("starcite_request_duration_ms",
          event_name: [:starcite, :request],
          measurement: :duration_ms,
          description: "Write request duration in milliseconds by node, operation, and phase",
          tags: [:node, :operation, :phase],
          reporter_options: [
            buckets: [1, 2, 5, 10, 20, 30, 40, 50, 75, 100, 125, 150, 200, 300, 500, 1_000, 2_000]
          ]
        ),
        counter("starcite_read_total",
          event_name: [:starcite, :read],
          measurement: :count,
          description: "Tail read delivery outcomes by operation",
          tags: [:operation, :outcome]
        ),
        distribution("starcite_read_duration_ms",
          event_name: [:starcite, :read],
          measurement: :duration_ms,
          description: "Tail read delivery duration in milliseconds by operation and phase",
          tags: [:operation, :phase],
          reporter_options: [
            buckets: [1, 2, 5, 10, 20, 30, 40, 50, 75, 100, 125, 150, 200, 300, 500, 1_000, 2_000]
          ]
        )
      ]
    )
  end

  defp routing_metrics do
    Event.build(
      :starcite_routing_metrics,
      [
        counter("starcite_routing_decision_total",
          event_name: [:starcite, :routing, :decision],
          measurement: :count,
          description:
            "Routing decisions by node, target, leader preference, and leader hint status",
          tags: [:node, :target, :prefer_leader, :leader_hint]
        ),
        counter("starcite_routing_result_total",
          event_name: [:starcite, :routing, :result],
          measurement: :count,
          description: "Routing execution outcomes by node, path, and outcome",
          tags: [:node, :path, :outcome]
        ),
        distribution("starcite_routing_attempts",
          event_name: [:starcite, :routing, :result],
          measurement: :attempts,
          description: "Replica attempts per routed write request",
          tags: [:node, :path],
          reporter_options: [buckets: [0, 1, 2, 3, 4, 5, 10]]
        ),
        distribution("starcite_routing_retries",
          event_name: [:starcite, :routing, :result],
          measurement: :retries,
          description: "Routing retries per routed write request",
          tags: [:node, :path],
          reporter_options: [buckets: [0, 1, 2, 3, 4, 5, 10]]
        ),
        distribution("starcite_routing_leader_redirects",
          event_name: [:starcite, :routing, :result],
          measurement: :leader_redirects,
          description: "Leader redirect hints observed per routed write request",
          tags: [:node, :path],
          reporter_options: [buckets: [0, 1, 2, 3, 4, 5, 10]]
        )
      ]
    )
  end

  defp session_metrics do
    Event.build(
      :starcite_session_lifecycle_metrics,
      [
        counter("starcite_session_create_total",
          event_name: [:starcite, :session, :create],
          measurement: :count,
          description: "Total successfully created sessions",
          tags: [:tenant_id]
        ),
        counter("starcite_session_freeze_total",
          event_name: [:starcite, :session, :freeze],
          measurement: :count,
          description: "Session freeze outcomes",
          tags: [:tenant_id, :outcome, :reason]
        ),
        counter("starcite_session_hydrate_total",
          event_name: [:starcite, :session, :hydrate],
          measurement: :count,
          description: "Session hydrate outcomes",
          tags: [:tenant_id, :outcome, :reason]
        )
      ]
    )
  end

  defp event_store_metrics do
    Event.build(
      :starcite_event_store_metrics,
      [
        counter("starcite_event_store_backpressure_total",
          event_name: [:starcite, :event_store, :backpressure],
          measurement: :count,
          description: "Total append rejections due to ETS capacity limits",
          tags: [:reason, :tenant_id]
        ),
        last_value("starcite_event_store_current_memory_bytes_on_backpressure",
          event_name: [:starcite, :event_store, :backpressure],
          measurement: :current_memory_bytes,
          description: "Current ETS memory usage when backpressure triggers",
          tags: [:tenant_id]
        ),
        counter("starcite_cursor_updates_total",
          event_name: [:starcite, :cursor, :update],
          measurement: :count,
          description: "Total cursor updates emitted",
          tags: [:tenant_id]
        ),
        last_value("starcite_cursor_lag",
          event_name: [:starcite, :cursor, :update],
          measurement: :lag,
          description: "Cursor update lag (last_seq - seq)",
          tags: [:tenant_id]
        ),
        counter("starcite_tail_cursor_lookup_total",
          event_name: [:starcite, :tail, :cursor_lookup],
          measurement: :count,
          description: "Tail cursor lookups by source and result",
          tags: [:source, :result, :tenant_id]
        )
      ]
    )
  end
end
