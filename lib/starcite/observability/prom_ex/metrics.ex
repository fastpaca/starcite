defmodule Starcite.Observability.PromEx.Metrics do
  @moduledoc false

  use PromEx.Plugin

  import Telemetry.Metrics, only: [counter: 2, distribution: 2, last_value: 2]

  @impl true
  def event_metrics(_opts) do
    [
      events_metrics(),
      archive_metrics(),
      event_plane_metrics()
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
          description: "Pending rows in archive ETS queue"
        ),
        last_value("starcite_archive_pending_sessions",
          event_name: [:starcite, :archive, :flush],
          measurement: :pending_sessions,
          description: "Sessions with pending rows in archive ETS queue"
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
          description: "Oldest pending event age across the archive queue (seconds)"
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

  defp event_plane_metrics do
    Event.build(
      :starcite_event_plane_metrics,
      [
        counter("starcite_event_store_writes_total",
          event_name: [:starcite, :event_store, :write],
          measurement: :count,
          description: "Total events mirrored into ETS event store"
        ),
        distribution("starcite_event_store_payload_bytes",
          event_name: [:starcite, :event_store, :write],
          measurement: :payload_bytes,
          description: "Payload bytes per ETS event-store write",
          unit: :byte,
          reporter_options: [buckets: [128, 512, 1024, 4096, 16384, 65536, 262_144, 1_048_576]]
        ),
        last_value("starcite_event_store_entries",
          event_name: [:starcite, :event_store, :write],
          measurement: :total_entries,
          description: "Current entry count in ETS event store"
        ),
        counter("starcite_cursor_updates_total",
          event_name: [:starcite, :cursor, :update],
          measurement: :count,
          description: "Total payload-free cursor updates emitted"
        ),
        last_value("starcite_cursor_lag",
          event_name: [:starcite, :cursor, :update],
          measurement: :lag,
          description: "Cursor update lag (last_seq - seq)"
        )
      ]
    )
  end
end
