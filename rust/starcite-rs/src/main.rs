mod auth;
mod config;
mod error;
mod fanout;
mod model;
mod ops;
mod phoenix;
mod repository;
mod runtime;
mod telemetry;
mod web;

use axum::{
    Router, middleware,
    routing::{get, post},
};
use config::Config;
use fanout::{LifecycleFanout, SessionFanout};
use ops::OpsState;
use runtime::SessionRuntime;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use telemetry::Telemetry;
use tokio::sync::watch;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Clone)]
pub struct AppState {
    pub pool: sqlx::PgPool,
    pub fanout: SessionFanout,
    pub lifecycle: LifecycleFanout,
    pub runtime: SessionRuntime,
    pub ops: OpsState,
    pub auth_mode: config::AuthMode,
    pub telemetry: Telemetry,
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
    let telemetry = Telemetry::new(config.telemetry_enabled);
    let ops_state = OpsState::new(config.shutdown_drain_timeout_ms);
    let runtime = SessionRuntime::new(
        Some(pool.clone()),
        lifecycle.clone(),
        telemetry.clone(),
        Duration::from_millis(config.session_runtime_idle_timeout_ms),
    );

    let state = AppState {
        pool,
        fanout: SessionFanout::default(),
        lifecycle,
        runtime,
        ops: ops_state.clone(),
        auth_mode: config.auth_mode,
        telemetry: telemetry.clone(),
    };

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
            web::reject_when_draining,
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
        .route("/health/live", get(web::live))
        .route("/health/ready", get(web::ready))
        .route("/metrics", get(telemetry::metrics))
        .route("/debug/state", get(web::debug_state))
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

        if !shutdown_drain_timeout.is_zero() {
            tokio::time::sleep(shutdown_drain_timeout).await;
        }

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
