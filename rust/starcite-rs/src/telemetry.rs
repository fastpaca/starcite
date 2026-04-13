use std::{
    collections::BTreeMap,
    fmt::Write,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::State,
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::{
    AppState,
    archive_queue::ArchiveQueueSnapshot,
    config::AuthMode,
    fanout::{LifecycleFanoutSnapshot, SessionFanoutSnapshot},
    ops::OpsSnapshot,
    runtime::{RuntimeSnapshot, RuntimeTouchReason},
};

const DEFAULT_MS_BUCKETS: &[u64] = &[
    1, 2, 5, 10, 20, 30, 40, 50, 75, 100, 125, 150, 200, 300, 500, 1_000, 2_000,
];

#[derive(Debug, Clone)]
pub struct Telemetry {
    enabled: bool,
    node_name: Arc<str>,
    started_at: Instant,
    inner: Arc<Mutex<Registry>>,
}

#[derive(Debug, Default)]
struct Registry {
    counters: BTreeMap<&'static str, BTreeMap<LabelSet, u64>>,
    gauges: BTreeMap<&'static str, BTreeMap<LabelSet, i64>>,
    histograms: BTreeMap<&'static str, BTreeMap<LabelSet, HistogramValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LabelSet(Vec<(&'static str, String)>);

#[derive(Debug, Clone)]
struct HistogramValue {
    buckets: Vec<u64>,
    count: u64,
    sum: u64,
}

struct CounterSpec {
    name: &'static str,
    help: &'static str,
}

struct GaugeSpec {
    name: &'static str,
    help: &'static str,
}

struct HistogramSpec {
    name: &'static str,
    help: &'static str,
    buckets: &'static [u64],
}

const COUNTERS: &[CounterSpec] = &[
    CounterSpec {
        name: "starcite_edge_http_total",
        help: "HTTP requests observed at the Rust endpoint boundary",
    },
    CounterSpec {
        name: "starcite_edge_stage_total",
        help: "Explicit edge-stage observations before controller timing",
    },
    CounterSpec {
        name: "starcite_auth_total",
        help: "Authentication-stage outcomes",
    },
    CounterSpec {
        name: "starcite_ingest_edge_total",
        help: "Total ingestion-edge requests by operation, outcome, and error reason",
    },
    CounterSpec {
        name: "starcite_request_total",
        help: "Write request outcomes by node, operation, phase, outcome, and error reason",
    },
    CounterSpec {
        name: "starcite_read_total",
        help: "Stream delivery outcomes by operation",
    },
    CounterSpec {
        name: "starcite_session_create_total",
        help: "Total successfully created sessions",
    },
    CounterSpec {
        name: "starcite_session_freeze_total",
        help: "Session freeze outcomes",
    },
    CounterSpec {
        name: "starcite_session_hydrate_total",
        help: "Session hydrate outcomes",
    },
];

const GAUGES: &[GaugeSpec] = &[
    GaugeSpec {
        name: "starcite_socket_connections",
        help: "Active WebSocket connections by transport and surface",
    },
    GaugeSpec {
        name: "starcite_socket_subscriptions",
        help: "Active logical stream subscriptions by transport and surface",
    },
];

const HISTOGRAMS: &[HistogramSpec] = &[
    HistogramSpec {
        name: "starcite_edge_http_duration_ms",
        help: "HTTP request duration from endpoint entry until response send",
        buckets: DEFAULT_MS_BUCKETS,
    },
    HistogramSpec {
        name: "starcite_edge_stage_duration_ms",
        help: "Explicit edge-stage duration in milliseconds",
        buckets: DEFAULT_MS_BUCKETS,
    },
    HistogramSpec {
        name: "starcite_auth_duration_ms",
        help: "Authentication-stage duration in milliseconds",
        buckets: DEFAULT_MS_BUCKETS,
    },
    HistogramSpec {
        name: "starcite_request_duration_ms",
        help: "Write request duration in milliseconds by node, operation, phase, and outcome",
        buckets: DEFAULT_MS_BUCKETS,
    },
    HistogramSpec {
        name: "starcite_read_duration_ms",
        help: "Stream delivery duration in milliseconds by operation and phase",
        buckets: DEFAULT_MS_BUCKETS,
    },
];

#[derive(Debug, Clone, Copy)]
pub enum AuthStage {
    Plug,
}

#[derive(Debug, Clone, Copy)]
pub enum EdgeStage {
    ControllerEntry,
}

#[derive(Debug, Clone, Copy)]
pub enum AuthOutcome {
    Ok,
    Error,
}

#[derive(Debug, Clone, Copy)]
pub enum AuthSource {
    None,
}

#[derive(Debug, Clone, Copy)]
pub enum IngestOperation {
    CreateSession,
    UpdateSession,
    AppendEvent,
}

#[derive(Debug, Clone, Copy)]
pub enum IngestOutcome {
    Ok,
    Error,
}

#[derive(Debug, Clone, Copy)]
pub enum RequestOperation {
    AppendEvent,
}

#[derive(Debug, Clone, Copy)]
pub enum RequestPhase {
    Total,
    Ack,
}

#[derive(Debug, Clone, Copy)]
pub enum RequestOutcome {
    Ok,
    Error,
    Timeout,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadOperation {
    TailCatchup,
    TailLive,
    LifecycleCatchup,
    LifecycleLive,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadPhase {
    Deliver,
}

#[derive(Debug, Clone, Copy)]
pub enum ReadOutcome {
    Ok,
    Error,
    Timeout,
}

#[derive(Debug, Clone, Copy)]
pub enum SessionOutcome {
    Ok,
}

#[derive(Debug, Clone, Copy)]
pub enum SessionReason {
    IdleTimeout,
    Hydrate,
}

#[derive(Debug, Clone, Copy)]
pub enum SocketTransport {
    Raw,
    Phoenix,
}

#[derive(Debug, Clone, Copy)]
pub enum SocketSurface {
    Socket,
    Tail,
    TenantLifecycle,
    SessionLifecycle,
}

#[derive(Debug)]
pub struct GaugeGuard {
    telemetry: Telemetry,
    name: &'static str,
    labels: LabelSet,
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.telemetry
            .adjust_gauge(self.name, self.labels.clone(), -1);
    }
}

impl Telemetry {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            node_name: Arc::<str>::from(
                std::env::var("HOSTNAME").unwrap_or_else(|_| "starcite-rs".to_string()),
            ),
            started_at: Instant::now(),
            inner: Arc::new(Mutex::new(Registry::default())),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    pub fn record_edge_http(&self, method: &str, status: StatusCode, duration_ms: u64) {
        let status_class = format!("{}xx", status.as_u16() / 100);
        let labels = label_set(&[
            ("method", method.to_string()),
            ("status_class", status_class),
        ]);

        self.increment_counter("starcite_edge_http_total", labels.clone());
        self.observe_histogram("starcite_edge_http_duration_ms", labels, duration_ms);
    }

    pub fn record_edge_stage(&self, stage: EdgeStage, method: &str, duration_ms: u64) {
        let labels = label_set(&[
            ("stage", edge_stage_label(stage).to_string()),
            ("method", method.to_string()),
        ]);

        self.increment_counter("starcite_edge_stage_total", labels.clone());
        self.observe_histogram("starcite_edge_stage_duration_ms", labels, duration_ms);
    }

    pub fn record_auth(
        &self,
        stage: AuthStage,
        mode: AuthMode,
        outcome: AuthOutcome,
        duration_ms: u64,
        error_reason: &'static str,
        source: AuthSource,
    ) {
        let outcome_label = auth_outcome_label(outcome);
        let mode_label = auth_mode_label(mode);
        let source_label = auth_source_label(source);
        let labels = label_set(&[
            ("stage", auth_stage_label(stage).to_string()),
            ("mode", mode_label.to_string()),
            ("outcome", outcome_label.to_string()),
            ("error_reason", error_reason.to_string()),
            ("source", source_label.to_string()),
        ]);
        let histogram_labels = label_set(&[
            ("stage", auth_stage_label(stage).to_string()),
            ("mode", mode_label.to_string()),
            ("outcome", outcome_label.to_string()),
            ("source", source_label.to_string()),
        ]);

        self.increment_counter("starcite_auth_total", labels);
        self.observe_histogram("starcite_auth_duration_ms", histogram_labels, duration_ms);
    }

    pub fn record_ingest_edge(
        &self,
        operation: IngestOperation,
        tenant_id: &str,
        outcome: IngestOutcome,
        error_reason: &'static str,
    ) {
        self.increment_counter(
            "starcite_ingest_edge_total",
            label_set(&[
                ("operation", ingest_operation_label(operation).to_string()),
                ("outcome", ingest_outcome_label(outcome).to_string()),
                ("error_reason", error_reason.to_string()),
                ("tenant_id", tenant_id.to_string()),
            ]),
        );
    }

    pub fn record_request(
        &self,
        operation: RequestOperation,
        phase: RequestPhase,
        outcome: RequestOutcome,
        duration_ms: u64,
        error_reason: &'static str,
    ) {
        let outcome_label = request_outcome_label(outcome);
        let labels = label_set(&[
            ("node", self.node_name().to_string()),
            ("operation", request_operation_label(operation).to_string()),
            ("phase", request_phase_label(phase).to_string()),
            ("outcome", outcome_label.to_string()),
            ("error_reason", error_reason.to_string()),
        ]);
        let histogram_labels = label_set(&[
            ("node", self.node_name().to_string()),
            ("operation", request_operation_label(operation).to_string()),
            ("phase", request_phase_label(phase).to_string()),
            ("outcome", outcome_label.to_string()),
        ]);

        self.increment_counter("starcite_request_total", labels);
        self.observe_histogram(
            "starcite_request_duration_ms",
            histogram_labels,
            duration_ms,
        );
    }

    pub fn record_read(
        &self,
        operation: ReadOperation,
        phase: ReadPhase,
        outcome: ReadOutcome,
        duration_ms: u64,
    ) {
        let outcome_label = read_outcome_label(outcome);
        let labels = label_set(&[
            ("operation", read_operation_label(operation).to_string()),
            ("outcome", outcome_label.to_string()),
        ]);
        let histogram_labels = label_set(&[
            ("operation", read_operation_label(operation).to_string()),
            ("phase", read_phase_label(phase).to_string()),
        ]);

        self.increment_counter("starcite_read_total", labels);
        self.observe_histogram("starcite_read_duration_ms", histogram_labels, duration_ms);
    }

    pub fn track_socket_connection(
        &self,
        transport: SocketTransport,
        surface: SocketSurface,
    ) -> GaugeGuard {
        self.track_gauge(
            "starcite_socket_connections",
            label_set(&[
                ("transport", socket_transport_label(transport).to_string()),
                ("surface", socket_surface_label(surface).to_string()),
            ]),
        )
    }

    pub fn track_socket_subscription(
        &self,
        transport: SocketTransport,
        surface: SocketSurface,
    ) -> GaugeGuard {
        self.track_gauge(
            "starcite_socket_subscriptions",
            label_set(&[
                ("transport", socket_transport_label(transport).to_string()),
                ("surface", socket_surface_label(surface).to_string()),
            ]),
        )
    }

    pub fn record_session_create(&self, tenant_id: &str) {
        self.increment_counter(
            "starcite_session_create_total",
            label_set(&[("tenant_id", tenant_id.to_string())]),
        );
    }

    pub fn record_session_freeze(
        &self,
        tenant_id: &str,
        outcome: SessionOutcome,
        reason: SessionReason,
    ) {
        self.increment_counter(
            "starcite_session_freeze_total",
            label_set(&[
                ("tenant_id", tenant_id.to_string()),
                ("outcome", session_outcome_label(outcome).to_string()),
                ("reason", session_reason_label(reason).to_string()),
            ]),
        );
    }

    pub fn record_session_hydrate(
        &self,
        tenant_id: &str,
        outcome: SessionOutcome,
        reason: SessionReason,
    ) {
        self.increment_counter(
            "starcite_session_hydrate_total",
            label_set(&[
                ("tenant_id", tenant_id.to_string()),
                ("outcome", session_outcome_label(outcome).to_string()),
                ("reason", session_reason_label(reason).to_string()),
            ]),
        );
    }

    pub fn render(&self) -> String {
        let mut out = String::new();

        writeln!(
            out,
            "# HELP starcite_process_uptime_seconds Process uptime in seconds"
        )
        .expect("write help");
        writeln!(out, "# TYPE starcite_process_uptime_seconds gauge").expect("write type");
        writeln!(
            out,
            "starcite_process_uptime_seconds {}",
            self.started_at.elapsed().as_secs()
        )
        .expect("write gauge");

        if !self.enabled {
            return out;
        }

        let registry = self.inner.lock().expect("telemetry lock");

        for spec in COUNTERS {
            writeln!(out).expect("newline");
            writeln!(out, "# HELP {} {}", spec.name, spec.help).expect("write help");
            writeln!(out, "# TYPE {} counter", spec.name).expect("write type");

            if let Some(series) = registry.counters.get(spec.name) {
                for (labels, value) in series {
                    render_metric_line(&mut out, spec.name, labels, *value);
                }
            }
        }

        for spec in GAUGES {
            writeln!(out).expect("newline");
            writeln!(out, "# HELP {} {}", spec.name, spec.help).expect("write help");
            writeln!(out, "# TYPE {} gauge", spec.name).expect("write type");

            if let Some(series) = registry.gauges.get(spec.name) {
                for (labels, value) in series {
                    render_metric_line(&mut out, spec.name, labels, *value);
                }
            }
        }

        for spec in HISTOGRAMS {
            writeln!(out).expect("newline");
            writeln!(out, "# HELP {} {}", spec.name, spec.help).expect("write help");
            writeln!(out, "# TYPE {} histogram", spec.name).expect("write type");

            if let Some(series) = registry.histograms.get(spec.name) {
                for (labels, histogram) in series {
                    for (index, bucket) in spec.buckets.iter().enumerate() {
                        render_histogram_bucket(
                            &mut out,
                            spec.name,
                            labels,
                            &bucket.to_string(),
                            histogram.buckets[index],
                        );
                    }

                    render_histogram_bucket(&mut out, spec.name, labels, "+Inf", histogram.count);
                    render_metric_line(
                        &mut out,
                        &format!("{}_sum", spec.name),
                        labels,
                        histogram.sum,
                    );
                    render_metric_line(
                        &mut out,
                        &format!("{}_count", spec.name),
                        labels,
                        histogram.count,
                    );
                }
            }
        }

        out
    }

    pub fn render_with_state(
        &self,
        ops: &OpsSnapshot,
        runtime: &RuntimeSnapshot,
        archive_queue: &ArchiveQueueSnapshot,
        events: &SessionFanoutSnapshot,
        lifecycle: &LifecycleFanoutSnapshot,
    ) -> String {
        let mut out = self.render();

        if !self.enabled {
            return out;
        }

        let node_labels = label_set(&[("node", self.node_name().to_string())]);
        let draining_labels = label_set(&[
            (
                "drain_source",
                ops.drain_source.unwrap_or("none").to_string(),
            ),
            ("node", self.node_name().to_string()),
        ]);
        let event_fanout_labels = label_set(&[
            ("node", self.node_name().to_string()),
            ("scope", "events_session".to_string()),
        ]);
        let tenant_lifecycle_labels = label_set(&[
            ("node", self.node_name().to_string()),
            ("scope", "lifecycle_tenant".to_string()),
        ]);
        let session_lifecycle_labels = label_set(&[
            ("node", self.node_name().to_string()),
            ("scope", "lifecycle_session".to_string()),
        ]);

        render_scalar_gauge(
            &mut out,
            "starcite_node_draining",
            "Whether this process is currently draining",
            &draining_labels,
            i64::from(ops.draining),
        );
        render_scalar_gauge(
            &mut out,
            "starcite_runtime_active_sessions",
            "Active runtime sessions currently tracked by this process",
            &node_labels,
            runtime.active_session_count as i64,
        );
        render_scalar_gauge(
            &mut out,
            "starcite_archive_queue_pending_sessions",
            "Sessions currently pending archive progress flush on this process",
            &node_labels,
            archive_queue.pending_session_count as i64,
        );
        let runtime_reason_labels = runtime
            .sessions
            .iter()
            .fold(
                BTreeMap::<RuntimeTouchReason, i64>::new(),
                |mut counts, session| {
                    *counts.entry(session.last_touch_reason).or_insert(0) += 1;
                    counts
                },
            )
            .into_iter()
            .map(|(reason, value)| {
                (
                    label_set(&[
                        ("node", self.node_name().to_string()),
                        ("reason", runtime_touch_reason_label(reason).to_string()),
                    ]),
                    value,
                )
            })
            .collect::<Vec<_>>();
        let runtime_reason_series = runtime_reason_labels
            .iter()
            .map(|(labels, value)| (labels, *value))
            .collect::<Vec<_>>();
        render_scalar_gauge_family(
            &mut out,
            "starcite_runtime_active_sessions_by_reason",
            "Active runtime sessions grouped by last touch reason",
            &runtime_reason_series,
        );
        render_scalar_gauge_family(
            &mut out,
            "starcite_fanout_active_keys",
            "Active fanout channel keys with at least one subscriber",
            &[
                (&event_fanout_labels, events.active_session_count as i64),
                (
                    &tenant_lifecycle_labels,
                    lifecycle.active_tenant_count as i64,
                ),
                (
                    &session_lifecycle_labels,
                    lifecycle.active_session_count as i64,
                ),
            ],
        );
        render_scalar_gauge_family(
            &mut out,
            "starcite_fanout_subscribers",
            "Total subscribers attached to fanout keys for this process",
            &[
                (
                    &event_fanout_labels,
                    events
                        .sessions
                        .iter()
                        .map(|session| session.subscribers as i64)
                        .sum::<i64>(),
                ),
                (
                    &tenant_lifecycle_labels,
                    lifecycle
                        .tenants
                        .iter()
                        .map(|tenant| tenant.subscribers as i64)
                        .sum::<i64>(),
                ),
                (
                    &session_lifecycle_labels,
                    lifecycle
                        .sessions
                        .iter()
                        .map(|session| session.subscribers as i64)
                        .sum::<i64>(),
                ),
            ],
        );

        out
    }

    fn increment_counter(&self, name: &'static str, labels: LabelSet) {
        if !self.enabled {
            return;
        }

        let mut registry = self.inner.lock().expect("telemetry lock");
        let series = registry.counters.entry(name).or_default();
        *series.entry(labels).or_insert(0) += 1;
    }

    fn observe_histogram(&self, name: &'static str, labels: LabelSet, value: u64) {
        if !self.enabled {
            return;
        }

        let Some(spec) = HISTOGRAMS.iter().find(|spec| spec.name == name) else {
            return;
        };

        let mut registry = self.inner.lock().expect("telemetry lock");
        let series = registry.histograms.entry(name).or_default();
        let histogram = series.entry(labels).or_insert_with(|| HistogramValue {
            buckets: vec![0; spec.buckets.len()],
            count: 0,
            sum: 0,
        });

        histogram.count += 1;
        histogram.sum += value;

        for (index, bucket) in spec.buckets.iter().enumerate() {
            if value <= *bucket {
                histogram.buckets[index] += 1;
            }
        }
    }

    fn track_gauge(&self, name: &'static str, labels: LabelSet) -> GaugeGuard {
        self.adjust_gauge(name, labels.clone(), 1);

        GaugeGuard {
            telemetry: self.clone(),
            name,
            labels,
        }
    }

    fn adjust_gauge(&self, name: &'static str, labels: LabelSet, delta: i64) {
        if !self.enabled {
            return;
        }

        let mut registry = self.inner.lock().expect("telemetry lock");
        let series = registry.gauges.entry(name).or_default();
        let remove_series = {
            let value = series.entry(labels.clone()).or_insert(0);
            *value += delta;
            *value <= 0
        };

        if remove_series {
            series.remove(&labels);
        }
    }
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::new(true)
    }
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let ops = state.ops.snapshot();
    let (runtime, archive_queue, events, lifecycle) = tokio::join!(
        state.runtime.snapshot(),
        state.archive_queue.snapshot(),
        state.fanout.snapshot(),
        state.lifecycle.snapshot()
    );

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state
            .telemetry
            .render_with_state(&ops, &runtime, &archive_queue, &events, &lifecycle),
    )
}

pub async fn measure_http(
    State(telemetry): State<Telemetry>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_string();
    let started_at = Instant::now();
    let response = next.run(request).await;
    let duration_ms = started_at.elapsed().as_millis() as u64;
    telemetry.record_edge_http(&method, response.status(), duration_ms);
    response
}

pub async fn measure_edge_stage_entry(
    State(telemetry): State<Telemetry>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().as_str().to_string();
    telemetry.record_edge_stage(EdgeStage::ControllerEntry, &method, 0);
    next.run(request).await
}

fn render_metric_line<T: std::fmt::Display>(
    out: &mut String,
    name: &str,
    labels: &LabelSet,
    value: T,
) {
    if labels.0.is_empty() {
        writeln!(out, "{name} {value}").expect("write metric");
    } else {
        writeln!(out, "{name}{{{}}} {value}", render_labels(labels)).expect("write metric");
    }
}

fn render_histogram_bucket(out: &mut String, name: &str, labels: &LabelSet, le: &str, value: u64) {
    let mut bucket_labels = labels.0.clone();
    bucket_labels.push(("le", le.to_string()));
    writeln!(
        out,
        "{}_bucket{{{}}} {}",
        name,
        render_labels(&LabelSet(bucket_labels)),
        value
    )
    .expect("write bucket");
}

fn render_scalar_gauge(out: &mut String, name: &str, help: &str, labels: &LabelSet, value: i64) {
    writeln!(out).expect("newline");
    writeln!(out, "# HELP {name} {help}").expect("write help");
    writeln!(out, "# TYPE {name} gauge").expect("write type");
    render_metric_line(out, name, labels, value);
}

fn render_scalar_gauge_family(
    out: &mut String,
    name: &str,
    help: &str,
    series: &[(&LabelSet, i64)],
) {
    writeln!(out).expect("newline");
    writeln!(out, "# HELP {name} {help}").expect("write help");
    writeln!(out, "# TYPE {name} gauge").expect("write type");

    for (labels, value) in series {
        render_metric_line(out, name, labels, *value);
    }
}

fn render_labels(labels: &LabelSet) -> String {
    labels
        .0
        .iter()
        .map(|(key, value)| format!(r#"{key}="{}""#, escape_label_value(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn escape_label_value(value: &str) -> String {
    value
        .replace('\\', r#"\\"#)
        .replace('\n', r#"\n"#)
        .replace('"', r#"\""#)
}

fn label_set(labels: &[(&'static str, String)]) -> LabelSet {
    let mut labels = labels.to_vec();
    labels.sort_by(|left, right| left.0.cmp(right.0));
    LabelSet(labels)
}

fn auth_stage_label(stage: AuthStage) -> &'static str {
    match stage {
        AuthStage::Plug => "plug",
    }
}

fn edge_stage_label(stage: EdgeStage) -> &'static str {
    match stage {
        EdgeStage::ControllerEntry => "controller_entry",
    }
}

fn auth_mode_label(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::None => "none",
        AuthMode::UnsafeJwt => "jwt",
    }
}

fn auth_outcome_label(outcome: AuthOutcome) -> &'static str {
    match outcome {
        AuthOutcome::Ok => "ok",
        AuthOutcome::Error => "error",
    }
}

fn auth_source_label(source: AuthSource) -> &'static str {
    match source {
        AuthSource::None => "none",
    }
}

fn ingest_operation_label(operation: IngestOperation) -> &'static str {
    match operation {
        IngestOperation::CreateSession => "create_session",
        IngestOperation::UpdateSession => "update_session",
        IngestOperation::AppendEvent => "append_event",
    }
}

fn ingest_outcome_label(outcome: IngestOutcome) -> &'static str {
    match outcome {
        IngestOutcome::Ok => "ok",
        IngestOutcome::Error => "error",
    }
}

fn request_operation_label(operation: RequestOperation) -> &'static str {
    match operation {
        RequestOperation::AppendEvent => "append_event",
    }
}

fn request_phase_label(phase: RequestPhase) -> &'static str {
    match phase {
        RequestPhase::Total => "total",
        RequestPhase::Ack => "ack",
    }
}

fn request_outcome_label(outcome: RequestOutcome) -> &'static str {
    match outcome {
        RequestOutcome::Ok => "ok",
        RequestOutcome::Error => "error",
        RequestOutcome::Timeout => "timeout",
    }
}

fn read_operation_label(operation: ReadOperation) -> &'static str {
    match operation {
        ReadOperation::TailCatchup => "tail_catchup",
        ReadOperation::TailLive => "tail_live",
        ReadOperation::LifecycleCatchup => "lifecycle_catchup",
        ReadOperation::LifecycleLive => "lifecycle_live",
    }
}

fn read_phase_label(phase: ReadPhase) -> &'static str {
    match phase {
        ReadPhase::Deliver => "deliver",
    }
}

fn read_outcome_label(outcome: ReadOutcome) -> &'static str {
    match outcome {
        ReadOutcome::Ok => "ok",
        ReadOutcome::Error => "error",
        ReadOutcome::Timeout => "timeout",
    }
}

fn session_outcome_label(outcome: SessionOutcome) -> &'static str {
    match outcome {
        SessionOutcome::Ok => "ok",
    }
}

fn session_reason_label(reason: SessionReason) -> &'static str {
    match reason {
        SessionReason::IdleTimeout => "idle_timeout",
        SessionReason::Hydrate => "hydrate",
    }
}

fn runtime_touch_reason_label(reason: RuntimeTouchReason) -> &'static str {
    match reason {
        RuntimeTouchReason::Create => "create",
        RuntimeTouchReason::HttpRead => "http_read",
        RuntimeTouchReason::HttpWrite => "http_write",
        RuntimeTouchReason::HttpLifecycle => "http_lifecycle",
        RuntimeTouchReason::RawTail => "raw_tail",
        RuntimeTouchReason::RawLifecycle => "raw_lifecycle",
        RuntimeTouchReason::PhoenixTail => "phoenix_tail",
        RuntimeTouchReason::PhoenixLifecycle => "phoenix_lifecycle",
    }
}

fn socket_transport_label(transport: SocketTransport) -> &'static str {
    match transport {
        SocketTransport::Raw => "raw",
        SocketTransport::Phoenix => "phoenix",
    }
}

fn socket_surface_label(surface: SocketSurface) -> &'static str {
    match surface {
        SocketSurface::Socket => "socket",
        SocketSurface::Tail => "tail",
        SocketSurface::TenantLifecycle => "tenant_lifecycle",
        SocketSurface::SessionLifecycle => "session_lifecycle",
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::{
        archive_queue::ArchiveQueueSnapshot,
        config::AuthMode,
        fanout::{
            LifecycleFanoutSnapshot, SessionFanoutSnapshot, SessionSubscriptionSnapshot,
            TenantSubscriptionSnapshot,
        },
        ops::OpsSnapshot,
        runtime::{ActiveSessionSnapshot, RuntimeSnapshot, RuntimeTouchReason},
    };

    use super::{
        AuthOutcome, AuthSource, AuthStage, EdgeStage, IngestOperation, IngestOutcome,
        ReadOperation, ReadOutcome, ReadPhase, RequestOperation, RequestOutcome, RequestPhase,
        SessionOutcome, SessionReason, SocketSurface, SocketTransport, Telemetry,
    };

    #[test]
    fn renders_prometheus_metrics_for_recorded_series() {
        let telemetry = Telemetry::new(true);

        telemetry.record_edge_http("GET", StatusCode::CREATED, 12);
        telemetry.record_edge_stage(EdgeStage::ControllerEntry, "POST", 0);
        telemetry.record_auth(
            AuthStage::Plug,
            AuthMode::UnsafeJwt,
            AuthOutcome::Ok,
            3,
            "none",
            AuthSource::None,
        );
        telemetry.record_ingest_edge(
            IngestOperation::AppendEvent,
            "acme",
            IngestOutcome::Ok,
            "none",
        );
        telemetry.record_request(
            RequestOperation::AppendEvent,
            RequestPhase::Ack,
            RequestOutcome::Ok,
            4,
            "none",
        );
        telemetry.record_read(
            ReadOperation::LifecycleCatchup,
            ReadPhase::Deliver,
            ReadOutcome::Ok,
            2,
        );
        telemetry.record_session_create("acme");
        telemetry.record_session_freeze("acme", SessionOutcome::Ok, SessionReason::IdleTimeout);
        telemetry.record_session_hydrate("acme", SessionOutcome::Ok, SessionReason::Hydrate);
        let _connection =
            telemetry.track_socket_connection(SocketTransport::Raw, SocketSurface::Tail);
        let _subscription = telemetry
            .track_socket_subscription(SocketTransport::Phoenix, SocketSurface::SessionLifecycle);

        let rendered = telemetry.render();

        assert!(rendered.contains("starcite_edge_http_total"));
        assert!(rendered.contains("starcite_edge_stage_total"));
        assert!(rendered.contains(r#"stage="controller_entry""#));
        assert!(rendered.contains(r#"method="GET""#));
        assert!(rendered.contains(r#"status_class="2xx""#));
        assert!(rendered.contains("starcite_auth_total"));
        assert!(rendered.contains(r#"mode="jwt""#));
        assert!(rendered.contains("starcite_ingest_edge_total"));
        assert!(rendered.contains(r#"operation="append_event""#));
        assert!(rendered.contains("starcite_request_duration_ms_bucket"));
        assert!(rendered.contains("starcite_read_total"));
        assert!(rendered.contains(r#"operation="lifecycle_catchup""#));
        assert!(rendered.contains("starcite_session_create_total"));
        assert!(rendered.contains("starcite_session_freeze_total"));
        assert!(rendered.contains("starcite_session_hydrate_total"));
        assert!(rendered.contains("starcite_socket_connections"));
        assert!(rendered.contains("starcite_socket_subscriptions"));
        assert!(rendered.contains(r#"transport="raw""#));
        assert!(rendered.contains(r#"surface="session_lifecycle""#));
    }

    #[test]
    fn disabled_telemetry_keeps_only_uptime_gauge() {
        let telemetry = Telemetry::new(false);

        telemetry.record_edge_http("GET", StatusCode::OK, 1);
        telemetry.record_edge_stage(EdgeStage::ControllerEntry, "GET", 0);

        let rendered = telemetry.render();

        assert!(rendered.contains("starcite_process_uptime_seconds"));
        assert!(!rendered.contains("starcite_edge_http_total{"));
        assert!(!rendered.contains("starcite_edge_stage_total{"));
    }

    #[test]
    fn socket_gauges_drop_back_to_zero_when_guards_leave_scope() {
        let telemetry = Telemetry::new(true);

        {
            let _connection =
                telemetry.track_socket_connection(SocketTransport::Phoenix, SocketSurface::Socket);
            let _subscription = telemetry.track_socket_subscription(
                SocketTransport::Phoenix,
                SocketSurface::TenantLifecycle,
            );

            let rendered = telemetry.render();
            assert!(rendered.contains("starcite_socket_connections"));
            assert!(rendered.contains(
                r#"starcite_socket_connections{surface="socket",transport="phoenix"} 1"#
            ));
            assert!(rendered.contains(
                r#"starcite_socket_subscriptions{surface="tenant_lifecycle",transport="phoenix"} 1"#
            ));
        }

        let rendered = telemetry.render();
        assert!(
            !rendered
                .contains(r#"starcite_socket_connections{surface="socket",transport="phoenix"} 1"#)
        );
        assert!(!rendered.contains(
            r#"starcite_socket_subscriptions{surface="tenant_lifecycle",transport="phoenix"} 1"#
        ));
    }

    #[test]
    fn render_with_state_includes_ops_runtime_and_fanout_gauges() {
        let telemetry = Telemetry::new(true);
        let rendered = telemetry.render_with_state(
            &OpsSnapshot {
                mode: "draining",
                draining: true,
                drain_source: Some("manual"),
                retry_after_ms: None,
                shutdown_drain_timeout_ms: 4_000,
            },
            &RuntimeSnapshot {
                idle_timeout_ms: 30_000,
                active_session_count: 2,
                sessions: vec![
                    ActiveSessionSnapshot {
                        session_id: "ses_a".to_string(),
                        tenant_id: "acme".to_string(),
                        generation: 1,
                        last_touch_reason: RuntimeTouchReason::HttpLifecycle,
                        idle_expires_in_ms: 12_345,
                    },
                    ActiveSessionSnapshot {
                        session_id: "ses_b".to_string(),
                        tenant_id: "acme".to_string(),
                        generation: 2,
                        last_touch_reason: RuntimeTouchReason::PhoenixTail,
                        idle_expires_in_ms: 20_000,
                    },
                ],
            },
            &ArchiveQueueSnapshot {
                pending_session_count: 2,
                sessions: vec!["ses_a".to_string(), "ses_b".to_string()],
            },
            &SessionFanoutSnapshot {
                active_session_count: 1,
                sessions: vec![SessionSubscriptionSnapshot {
                    session_id: "ses_demo".to_string(),
                    subscribers: 3,
                }],
            },
            &LifecycleFanoutSnapshot {
                active_tenant_count: 1,
                active_session_count: 2,
                tenants: vec![TenantSubscriptionSnapshot {
                    tenant_id: "acme".to_string(),
                    subscribers: 4,
                }],
                sessions: vec![
                    SessionSubscriptionSnapshot {
                        session_id: "ses_a".to_string(),
                        subscribers: 2,
                    },
                    SessionSubscriptionSnapshot {
                        session_id: "ses_b".to_string(),
                        subscribers: 1,
                    },
                ],
            },
        );

        assert!(rendered.contains("starcite_node_draining"));
        assert!(rendered.contains(r#"drain_source="manual""#));
        assert!(rendered.contains("starcite_runtime_active_sessions"));
        assert!(rendered.contains(r#"starcite_runtime_active_sessions{node="starcite-rs"} 2"#));
        assert!(rendered.contains("starcite_archive_queue_pending_sessions"));
        assert!(
            rendered.contains(r#"starcite_archive_queue_pending_sessions{node="starcite-rs"} 2"#)
        );
        assert!(rendered.contains("starcite_runtime_active_sessions_by_reason"));
        assert!(rendered.contains(
            r#"starcite_runtime_active_sessions_by_reason{node="starcite-rs",reason="http_lifecycle"} 1"#
        ));
        assert!(rendered.contains(
            r#"starcite_runtime_active_sessions_by_reason{node="starcite-rs",reason="phoenix_tail"} 1"#
        ));
        assert!(rendered.contains("starcite_fanout_active_keys"));
        assert!(rendered.contains(
            r#"starcite_fanout_active_keys{node="starcite-rs",scope="events_session"} 1"#
        ));
        assert!(rendered.contains(
            r#"starcite_fanout_active_keys{node="starcite-rs",scope="lifecycle_tenant"} 1"#
        ));
        assert!(rendered.contains(
            r#"starcite_fanout_active_keys{node="starcite-rs",scope="lifecycle_session"} 2"#
        ));
        assert!(rendered.contains("starcite_fanout_subscribers"));
        assert!(rendered.contains(
            r#"starcite_fanout_subscribers{node="starcite-rs",scope="events_session"} 3"#
        ));
        assert!(rendered.contains(
            r#"starcite_fanout_subscribers{node="starcite-rs",scope="lifecycle_tenant"} 4"#
        ));
        assert!(rendered.contains(
            r#"starcite_fanout_subscribers{node="starcite-rs",scope="lifecycle_session"} 3"#
        ));
    }
}
