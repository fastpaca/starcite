mod archive;
mod archive_queue;
mod auth;
mod config;
mod control_plane;
mod edge_routing;
mod error;
mod fanout;
mod flush_queue;
mod flusher;
mod hot_store;
mod lifecycle_scope;
mod model;
mod ops;
mod ops_http;
mod owner_proxy;
mod ownership;
mod phoenix;
mod phoenix_context;
mod phoenix_protocol;
mod phoenix_topics;
mod query_options;
mod raw_socket;
mod read_path;
mod relay;
mod replication;
mod repository;
mod request_metrics;
mod request_validation;
mod runtime;
mod session_manager;
mod session_store;
mod socket_runtime;
mod socket_support;
mod telemetry;
mod web;

use archive::ArchiveWorker;
use archive_queue::ArchiveQueue;
use axum::{
    Router, middleware,
    routing::{get, post},
};
use config::Config;
use control_plane::ControlPlaneState;
use fanout::{LifecycleFanout, SessionFanout};
use flush_queue::PendingFlushQueue;
use flusher::FlushWorker;
use hot_store::HotEventStore;
use ops::OpsState;
use owner_proxy::OwnerProxy;
use ownership::OwnershipManager;
use replication::ReplicationCoordinator;
use runtime::SessionRuntime;
use session_manager::{SessionManager, SessionManagerDeps};
use session_store::HotSessionStore;
use sqlx::postgres::PgPoolOptions;
use std::{sync::Arc, time::Duration};
use telemetry::Telemetry;
use tokio::sync::watch;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{EnvFilter, fmt};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub fanout: SessionFanout,
    pub lifecycle: LifecycleFanout,
    pub hot_store: HotEventStore,
    pub archive_queue: ArchiveQueue,
    pub pending_flush: PendingFlushQueue,
    pub session_store: HotSessionStore,
    pub session_manager: SessionManager,
    pub ownership: OwnershipManager,
    pub control_plane: ControlPlaneState,
    pub owner_proxy: OwnerProxy,
    pub replication: ReplicationCoordinator,
    pub runtime: SessionRuntime,
    pub ops: OpsState,
    pub auth_mode: config::AuthMode,
    pub commit_mode: config::CommitMode,
    pub telemetry: Telemetry,
    pub instance_id: Arc<str>,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    init_tracing();

    let config = Config::from_env()?;

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .map_err(|error| format!("failed to connect to postgres: {error}"))?;

    if config.migrate_on_boot {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|error| format!("failed to run migrations: {error}"))?;
    }

    let lifecycle = LifecycleFanout::default();
    let fanout = SessionFanout::default();
    let hot_store = HotEventStore::new();
    let archive_queue = ArchiveQueue::new();
    let pending_flush = PendingFlushQueue::new();
    let session_store = HotSessionStore::new();
    let telemetry = Telemetry::new(config.telemetry_enabled);
    let ops_state = OpsState::new(config.shutdown_drain_timeout_ms);
    let instance_id: Arc<str> = Arc::from(Uuid::now_v7().simple().to_string());
    let control_plane = ControlPlaneState::new(
        config.local_async_node_public_url.clone(),
        config.local_async_node_ops_url.clone(),
        Duration::from_millis(config.local_async_node_ttl_ms),
    );
    let owner_proxy = OwnerProxy::new(
        Duration::from_millis(config.local_async_owner_proxy_timeout_ms),
        config.local_async_node_public_url.clone(),
    );
    let ownership = OwnershipManager::new(
        pool.clone(),
        instance_id.clone(),
        Duration::from_millis(config.local_async_lease_ttl_ms),
    );
    let replication = ReplicationCoordinator::new(
        instance_id.clone(),
        control_plane.enabled(),
        config.local_async_standby_url.clone(),
        Duration::from_millis(config.local_async_replication_timeout_ms),
    )?;
    let session_manager = SessionManager::new(SessionManagerDeps {
        pool: pool.clone(),
        fanout: fanout.clone(),
        hot_store: hot_store.clone(),
        archive_queue: archive_queue.clone(),
        pending_flush: pending_flush.clone(),
        session_store: session_store.clone(),
        ownership: ownership.clone(),
        replication: replication.clone(),
        ops: ops_state.clone(),
        commit_mode: config.commit_mode,
        instance_id: instance_id.clone(),
        idle_timeout: Duration::from_millis(config.session_runtime_idle_timeout_ms),
    });
    let runtime = SessionRuntime::new(
        Some(pool.clone()),
        lifecycle.clone(),
        telemetry.clone(),
        instance_id.clone(),
        Duration::from_millis(config.session_runtime_idle_timeout_ms),
    );

    let state = AppState {
        pool: pool.clone(),
        fanout: fanout.clone(),
        lifecycle,
        hot_store: hot_store.clone(),
        archive_queue: archive_queue.clone(),
        pending_flush: pending_flush.clone(),
        session_store: session_store.clone(),
        session_manager,
        ownership,
        control_plane: control_plane.clone(),
        owner_proxy,
        replication,
        runtime,
        ops: ops_state.clone(),
        auth_mode: config.auth_mode,
        commit_mode: config.commit_mode,
        telemetry: telemetry.clone(),
        instance_id: instance_id.clone(),
    };

    FlushWorker::new(
        pool.clone(),
        pending_flush,
        archive_queue.clone(),
        instance_id.clone(),
        Duration::from_millis(config.commit_flush_interval_ms),
    )
    .spawn();

    ArchiveWorker::new(
        pool.clone(),
        state.hot_store.clone(),
        state.session_store.clone(),
        state.archive_queue.clone(),
        Duration::from_millis(config.archive_flush_interval_ms),
        state.instance_id.clone(),
    )
    .spawn();

    control_plane.spawn(pool.clone(), state.instance_id.clone(), ops_state.clone());

    relay::spawn(
        pool,
        fanout,
        state.lifecycle.clone(),
        hot_store,
        archive_queue,
        session_store,
        instance_id,
    );

    let app = build_public_router(telemetry.clone(), ops_state.clone()).with_state(state.clone());
    let ops_router = build_ops_router(telemetry).with_state(state);

    tracing::info!(
        public_listen_addr = %config.listen_addr,
        ops_listen_addr = %config.ops_listen_addr,
        "starting starcite-rs"
    );

    let app_listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|error| format!("failed to bind public listener: {error}"))?;
    let ops_listener = tokio::net::TcpListener::bind(config.ops_listen_addr)
        .await
        .map_err(|error| format!("failed to bind ops listener: {error}"))?;

    let shutdown = shutdown_watch(
        ops_state,
        Duration::from_millis(config.shutdown_drain_timeout_ms),
    );

    let app_server =
        axum::serve(app_listener, app).with_graceful_shutdown(wait_for_shutdown(shutdown.clone()));
    let ops_server =
        axum::serve(ops_listener, ops_router).with_graceful_shutdown(wait_for_shutdown(shutdown));

    tokio::try_join!(app_server, ops_server)
        .map(|_| ())
        .map_err(|error| format!("server error: {error}"))
}

fn build_public_router(telemetry: Telemetry, ops: OpsState) -> Router<AppState> {
    Router::new()
        .route("/v1/socket/websocket", get(phoenix::socket))
        .route("/v1/lifecycle", get(web::lifecycle_events))
        .route("/v1/lifecycle/events", get(web::read_lifecycle))
        .route(
            "/v1/sessions",
            post(web::create_session).get(web::list_sessions),
        )
        .route(
            "/v1/sessions/{id}",
            get(web::show_session).patch(web::update_session),
        )
        .route("/v1/sessions/{id}/append", post(web::append_event))
        .route("/v1/sessions/{id}/events", get(web::read_events))
        .route(
            "/v1/sessions/{id}/lifecycle",
            get(web::session_lifecycle_events),
        )
        .route(
            "/v1/sessions/{id}/lifecycle/events",
            get(web::read_session_lifecycle),
        )
        .route("/v1/sessions/{id}/tail", get(web::tail_events))
        .route("/v1/sessions/{id}/archive", post(web::archive_session))
        .route("/v1/sessions/{id}/unarchive", post(web::unarchive_session))
        .layer(middleware::from_fn_with_state(
            ops,
            ops_http::reject_when_draining,
        ))
        .layer(middleware::from_fn_with_state(
            telemetry.clone(),
            telemetry::measure_edge_stage_entry,
        ))
        .layer(middleware::from_fn_with_state(
            telemetry,
            telemetry::measure_http,
        ))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

fn build_ops_router(telemetry: Telemetry) -> Router<AppState> {
    Router::new()
        .route("/health/live", get(ops_http::live))
        .route("/health/ready", get(ops_http::ready))
        .route("/metrics", get(telemetry::metrics))
        .route("/debug/state", get(ops_http::debug_state))
        .route(
            "/debug/drain",
            post(ops_http::begin_drain).delete(ops_http::clear_drain),
        )
        .route(
            "/internal/replication/append",
            post(ops_http::append_replica),
        )
        .layer(middleware::from_fn_with_state(
            telemetry,
            telemetry::measure_http,
        ))
        .layer(TraceLayer::new_for_http())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("starcite_rs=info,tower_http=info"));

    fmt().with_env_filter(filter).with_target(false).init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        let mut signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

        if let Some(signal) = signal.as_mut() {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn shutdown_watch(ops: OpsState, shutdown_drain_timeout: Duration) -> watch::Receiver<bool> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::spawn(async move {
        shutdown_signal().await;
        ops.begin_shutdown_drain();
        tracing::info!(
            shutdown_drain_timeout_ms = shutdown_drain_timeout.as_millis() as u64,
            "starting local shutdown drain"
        );

        if !shutdown_drain_timeout.is_zero() {
            tokio::time::sleep(shutdown_drain_timeout).await;
        }

        tracing::info!("completing local shutdown drain");
        let _ = shutdown_tx.send(true);
    });

    shutdown_rx
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }

    let _ = shutdown.changed().await;
}
