defmodule Starcite.Observability.PromEx.Metrics do
  @moduledoc false

  use PromEx.Plugin

  import Telemetry.Metrics, only: [counter: 2, distribution: 2, last_value: 2]

  @impl true
  def event_metrics(_opts) do
    [
      events_metrics(),
      archive_metrics(),
      event_store_metrics(),
      raft_command_metrics(),
      routing_metrics()
    ]
  end

  defp events_metrics do
    Event.build(
      :starcite_events_metrics,
      [
        counter("starcite_events_append_total",
          event_name: [:starcite, :events, :append],
          description: "Total events appended",
          tags: [:type, :source]
        ),
        distribution("starcite_events_payload_bytes",
          event_name: [:starcite, :events, :append],
          measurement: :payload_bytes,
          description: "Payload bytes per appended event",
          unit: :byte,
          tags: [:type, :source],
          reporter_options: [buckets: [128, 512, 1024, 4096, 16384, 65536, 262_144, 1_048_576]]
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
          tags: [:session_id]
        ),
        distribution("starcite_archive_batch_bytes",
          event_name: [:starcite, :archive, :batch],
          measurement: :batch_bytes,
          description: "Payload bytes per archive batch (per session)",
          reporter_options: [buckets: [1_024, 10_240, 102_400, 1_048_576, 10_485_760, 52_428_800]],
          tags: [:session_id]
        ),
        distribution("starcite_archive_event_bytes",
          event_name: [:starcite, :archive, :batch],
          measurement: :avg_event_bytes,
          description: "Average event bytes per batch (per session)",
          reporter_options: [buckets: [100, 1_000, 5_000, 10_000, 50_000, 100_000, 1_000_000]],
          tags: [:session_id]
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
          tags: [:session_id]
        ),
        last_value("starcite_archive_tail_size",
          event_name: [:starcite, :archive, :ack],
          measurement: :tail_size,
          description: "In-Raft tail size after trim",
          tags: [:session_id]
        ),
        counter("starcite_archive_trimmed_total",
          event_name: [:starcite, :archive, :ack],
          measurement: :trimmed,
          description: "Total entries trimmed from Raft tail",
          tags: [:session_id]
        )
      ]
    )
  end

  defp event_store_metrics do
    Event.build(
      :starcite_event_store_metrics,
      [
        counter("starcite_event_store_writes_total",
          event_name: [:starcite, :events, :append],
          description: "Total events mirrored into ETS event store"
        ),
        distribution("starcite_event_store_payload_bytes",
          event_name: [:starcite, :events, :append],
          measurement: :payload_bytes,
          description: "Payload bytes per ETS event-store write",
          unit: :byte,
          reporter_options: [buckets: [128, 512, 1024, 4096, 16384, 65536, 262_144, 1_048_576]]
        ),
        counter("starcite_event_store_backpressure_total",
          event_name: [:starcite, :event_store, :backpressure],
          measurement: :count,
          description: "Total append rejections due to ETS capacity limits",
          tags: [:reason]
        ),
        last_value("starcite_event_store_current_memory_bytes_on_backpressure",
          event_name: [:starcite, :event_store, :backpressure],
          measurement: :current_memory_bytes,
          description: "Current ETS memory usage when backpressure triggers"
        ),
        counter("starcite_cursor_updates_total",
          event_name: [:starcite, :cursor, :update],
          measurement: :count,
          description: "Total cursor updates emitted"
        ),
        last_value("starcite_cursor_lag",
          event_name: [:starcite, :cursor, :update],
          measurement: :lag,
          description: "Cursor update lag (last_seq - seq)"
        ),
        counter("starcite_tail_cursor_lookup_total",
          event_name: [:starcite, :tail, :cursor_lookup],
          measurement: :count,
          description: "Tail cursor lookups by source and result",
          tags: [:source, :result]
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
          description: "Routing decisions by target and leader-hint usage",
          tags: [:target, :leader_hint]
        ),
        distribution("starcite_routing_replica_count",
          event_name: [:starcite, :routing, :decision],
          measurement: :replica_count,
          description: "Replica count observed at routing decision time",
          reporter_options: [buckets: [1, 2, 3, 5, 7]]
        ),
        distribution("starcite_routing_ready_count",
          event_name: [:starcite, :routing, :decision],
          measurement: :ready_count,
          description: "Ready replica count observed at routing decision time",
          reporter_options: [buckets: [0, 1, 2, 3, 5, 7]]
        ),
        counter("starcite_routing_result_total",
          event_name: [:starcite, :routing, :result],
          measurement: :count,
          description: "Routing execution outcomes by path and outcome",
          tags: [:path, :outcome]
        ),
        distribution("starcite_routing_attempts",
          event_name: [:starcite, :routing, :result],
          measurement: :attempts,
          description: "Replica attempts per routed request",
          reporter_options: [buckets: [0, 1, 2, 3, 5, 8]]
        ),
        distribution("starcite_routing_retries",
          event_name: [:starcite, :routing, :result],
          measurement: :retries,
          description: "Retries per routed request (attempts-1)",
          reporter_options: [buckets: [0, 1, 2, 3, 5, 8]]
        ),
        counter("starcite_routing_leader_redirects_total",
          event_name: [:starcite, :routing, :result],
          measurement: :leader_redirects,
          description: "Leader redirect hints observed while routing"
        )
      ]
    )
  end

  defp raft_command_metrics do
    Event.build(
      :starcite_raft_command_metrics,
      [
        counter("starcite_raft_command_total",
          event_name: [:starcite, :raft, :command],
          measurement: :count,
          description: "Raft command outcomes by command type and local/retry path",
          tags: [:command, :outcome]
        )
      ]
    )
  end
end
