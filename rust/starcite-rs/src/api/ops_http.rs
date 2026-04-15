use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{
    AppState, cluster,
    cluster::replication::{AppendReplicaRequest, ReplicationAck, ReplicationSnapshot},
    data_plane,
    error::{self, AppError},
    runtime::{
        LifecycleFanoutSnapshot, OpsSnapshot, RuntimeSnapshot, SessionFanoutSnapshot,
        SessionManagerSnapshot,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicaAppendDisposition {
    AlreadyCommitted,
    CommitNext,
    SeqGap { expected_seq: i64 },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct LiveResponse {
    status: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ReadyResponse {
    status: &'static str,
    mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<cluster::ControlPlaneReadinessDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain_source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DebugStateResponse {
    ops: OpsSnapshot,
    auth_mode: &'static str,
    write_model: &'static str,
    telemetry_enabled: bool,
    control_plane: cluster::ControlPlaneSnapshot,
    runtime: RuntimeSnapshot,
    hot_store: data_plane::HotEventStoreSnapshot,
    session_store: data_plane::HotSessionStoreSnapshot,
    session_manager: SessionManagerSnapshot,
    ownership: cluster::OwnershipSnapshot,
    replication: ReplicationSnapshot,
    pending_flush: data_plane::PendingFlushSnapshot,
    archive_queue: data_plane::ArchiveQueueSnapshot,
    fanout: DebugFanoutState,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DebugFanoutState {
    events: SessionFanoutSnapshot,
    lifecycle: LifecycleFanoutSnapshot,
}

pub async fn live() -> impl IntoResponse {
    (StatusCode::OK, Json(LiveResponse { status: "ok" }))
}

pub async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let ops = state.ops.snapshot();

    if ops.draining {
        let mut response = ready_response(
            &ops,
            Some("draining"),
            Some(cluster::ControlPlaneReadinessDetail::draining()),
        )
        .into_response();
        error::apply_drain_headers(response.headers_mut(), ops.drain_source, ops.retry_after_ms);
        return response;
    }

    match state
        .control_plane
        .readiness(&state.pool, state.instance_id.as_ref())
        .await
    {
        Ok(readiness) => ready_response(&ops, readiness.reason, readiness.detail).into_response(),
        Err(error) => {
            tracing::error!(error = ?error, "readiness control-plane lookup failed");
            ready_response(&ops, Some("database_unavailable"), None).into_response()
        }
    }
}

pub async fn debug_state(State(state): State<AppState>) -> impl IntoResponse {
    let ops = state.ops.snapshot();
    let control_plane = state.control_plane.snapshot();
    let runtime = state.runtime.snapshot().await;
    let hot_store = state.hot_store.snapshot().await;
    let session_store = state.session_store.snapshot().await;
    let session_manager = state.session_manager.snapshot().await;
    let ownership = state.ownership.snapshot().await;
    let replication = state.replication.snapshot();
    let pending_flush = state.pending_flush.snapshot().await;
    let archive_queue = state.archive_queue.snapshot().await;
    let events = state.fanout.snapshot().await;
    let lifecycle = state.lifecycle.snapshot().await;

    Json(DebugStateResponse {
        ops,
        auth_mode: auth_mode_name(state.auth_mode),
        write_model: "local_async",
        telemetry_enabled: state.telemetry.enabled(),
        control_plane,
        runtime,
        hot_store,
        session_store,
        session_manager,
        ownership,
        replication,
        pending_flush,
        archive_queue,
        fanout: DebugFanoutState { events, lifecycle },
    })
}

pub async fn begin_drain(State(state): State<AppState>) -> impl IntoResponse {
    state.ops.begin_manual_drain();
    tracing::info!("triggered local drain from ops endpoint");
    (StatusCode::ACCEPTED, Json(state.ops.snapshot()))
}

pub async fn clear_drain(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    match state.ops.snapshot().drain_source {
        None => Ok((StatusCode::OK, Json(state.ops.snapshot()))),
        Some("manual") => {
            state.ops.clear_drain();
            tracing::info!("cleared local manual drain from ops endpoint");
            Ok((StatusCode::OK, Json(state.ops.snapshot())))
        }
        Some(_) => Err(AppError::DrainResetForbidden),
    }
}

pub async fn append_replica(
    State(state): State<AppState>,
    body: Result<Json<AppendReplicaRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    reject_internal_replication_when_unavailable(&state)?;
    let Json(request) = body.map_err(|_| AppError::InvalidEvent)?;
    reject_stale_replica_epoch(&state, &request.event.session_id, request.epoch).await?;

    let last_seq = state
        .session_store
        .get_last_seq(&request.event.session_id)
        .await
        .unwrap_or(0);

    match classify_replica_append(last_seq, request.event.seq) {
        ReplicaAppendDisposition::AlreadyCommitted => {
            return Ok((
                StatusCode::OK,
                Json(ReplicationAck {
                    status: "already_committed".to_string(),
                }),
            ));
        }
        ReplicaAppendDisposition::CommitNext => {}
        ReplicaAppendDisposition::SeqGap { expected_seq } => {
            tracing::warn!(
                owner_id = request.owner_id,
                epoch = request.epoch,
                session_id = request.event.session_id,
                last_seq,
                expected_seq,
                incoming_seq = request.event.seq,
                "replica append rejected due to seq gap"
            );

            return Ok((
                StatusCode::CONFLICT,
                Json(ReplicationAck {
                    status: "seq_gap".to_string(),
                }),
            ));
        }
    }

    state
        .session_manager
        .apply_replica_commit(request.event.with_epoch(request.epoch))
        .await;

    Ok((
        StatusCode::ACCEPTED,
        Json(ReplicationAck {
            status: "committed".to_string(),
        }),
    ))
}

pub async fn reject_when_draining(
    State(ops): State<crate::runtime::OpsState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if ops.is_draining() {
        AppError::node_draining(&ops.snapshot()).into_response()
    } else {
        next.run(request).await
    }
}

fn classify_replica_append(last_seq: i64, incoming_seq: i64) -> ReplicaAppendDisposition {
    if last_seq >= incoming_seq {
        ReplicaAppendDisposition::AlreadyCommitted
    } else {
        let expected_seq = last_seq + 1;

        if incoming_seq == expected_seq {
            ReplicaAppendDisposition::CommitNext
        } else {
            ReplicaAppendDisposition::SeqGap { expected_seq }
        }
    }
}

fn ready_response(
    ops: &OpsSnapshot,
    reason: Option<&'static str>,
    detail: Option<cluster::ControlPlaneReadinessDetail>,
) -> (StatusCode, Json<ReadyResponse>) {
    let status = if reason.is_some() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        status,
        Json(ReadyResponse {
            status: if reason.is_some() { "starting" } else { "ok" },
            mode: ops.mode,
            reason,
            detail,
            drain_source: ops.drain_source,
            retry_after_ms: ops.retry_after_ms,
        }),
    )
}

fn auth_mode_name(auth_mode: crate::config::AuthMode) -> &'static str {
    match auth_mode {
        crate::config::AuthMode::None => "none",
        crate::config::AuthMode::Jwt => "jwt",
    }
}

fn reject_internal_replication_when_unavailable(state: &AppState) -> Result<(), AppError> {
    if state.ops.is_draining() {
        return Err(AppError::node_draining(&state.ops.snapshot()));
    }

    Ok(())
}

async fn reject_stale_replica_epoch(
    state: &AppState,
    session_id: &str,
    epoch: i64,
) -> Result<(), AppError> {
    let Some(owned_epoch) = state.ownership.owned_epoch(session_id).await else {
        return Ok(());
    };

    if owned_epoch <= epoch {
        return Ok(());
    }

    let owner_public_url = state.control_plane.snapshot().public_url;

    Err(AppError::SessionNotOwned {
        owner_id: state.instance_id.to_string(),
        owner_public_url,
        epoch: owned_epoch,
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{Json, http::StatusCode};
    use sqlx::postgres::PgPoolOptions;

    use super::{
        ReplicaAppendDisposition, classify_replica_append, ready_response,
        reject_stale_replica_epoch,
    };
    use crate::{
        AppState,
        auth::AuthService,
        cluster::{
            ControlPlaneReadinessDetail, ControlPlaneState, OwnerProxy, OwnershipManager,
            ReplicationCoordinator,
        },
        config::{AuthMode, Config},
        data_plane::{ArchiveQueue, HotEventStore, HotSessionStore, PendingFlushQueue},
        error::AppError,
        runtime::{
            LifecycleFanout, OpsState, SessionFanout, SessionManager, SessionManagerDeps,
            SessionRuntime,
        },
        telemetry::Telemetry,
    };

    fn test_state(public_url: Option<&str>) -> AppState {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");
        let fanout = SessionFanout::default();
        let lifecycle = LifecycleFanout::default();
        let hot_store = HotEventStore::new();
        let archive_queue = ArchiveQueue::new();
        let pending_flush = PendingFlushQueue::new();
        let session_store = HotSessionStore::new();
        let telemetry = Telemetry::new(true);
        let ops = OpsState::new(30_000);
        let instance_id = Arc::<str>::from("node-a");
        let ownership =
            OwnershipManager::new(pool.clone(), instance_id.clone(), Duration::from_secs(5));
        let control_plane =
            ControlPlaneState::new(public_url.map(str::to_string), None, Duration::from_secs(5));
        let owner_proxy = OwnerProxy::new(Duration::from_millis(100), None);
        let replication =
            ReplicationCoordinator::new(instance_id.clone(), false, Duration::from_millis(100))
                .expect("replication");
        let session_manager = SessionManager::new(SessionManagerDeps {
            pool: pool.clone(),
            fanout: fanout.clone(),
            hot_store: hot_store.clone(),
            pending_flush: pending_flush.clone(),
            session_store: session_store.clone(),
            ownership: ownership.clone(),
            replication: replication.clone(),
            ops: ops.clone(),
            telemetry: telemetry.clone(),
            idle_timeout: Duration::from_secs(30),
        });
        let runtime = SessionRuntime::new(
            None,
            lifecycle.clone(),
            telemetry.clone(),
            instance_id.clone(),
            Duration::from_secs(30),
        );
        let auth = AuthService::new(&test_config()).expect("auth");

        AppState {
            pool,
            fanout,
            lifecycle,
            hot_store,
            archive_queue,
            pending_flush,
            session_store,
            session_manager,
            ownership,
            control_plane,
            owner_proxy,
            replication,
            runtime,
            ops,
            auth_mode: AuthMode::None,
            auth,
            telemetry,
            instance_id,
        }
    }

    fn test_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:4001".parse().expect("listen addr"),
            ops_listen_addr: "127.0.0.1:4101".parse().expect("ops listen addr"),
            database_url: "postgres://postgres:postgres@localhost/starcite_test".to_string(),
            max_connections: 1,
            archive_flush_interval_ms: 5_000,
            migrate_on_boot: false,
            auth_mode: AuthMode::None,
            auth_issuer: None,
            auth_audience: None,
            auth_jwks_url: None,
            auth_jwt_leeway_seconds: 1,
            auth_jwks_refresh_ms: 60_000,
            auth_jwks_hard_expiry_ms: 60_000,
            telemetry_enabled: true,
            shutdown_drain_timeout_ms: 30_000,
            session_runtime_idle_timeout_ms: 30_000,
            commit_flush_interval_ms: 100,
            local_async_lease_ttl_ms: 5_000,
            local_async_node_public_url: None,
            local_async_node_ops_url: None,
            local_async_node_ttl_ms: 2_000,
            local_async_owner_proxy_timeout_ms: 100,
            local_async_replication_timeout_ms: 100,
        }
    }

    #[test]
    fn ready_response_reports_ready_mode() {
        let ops = OpsState::new(30_000).snapshot();
        let (status, Json(body)) = ready_response(&ops, None, None);

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.status, "ok");
        assert_eq!(body.mode, "ready");
        assert_eq!(body.reason, None);
        assert_eq!(body.detail, None);
        assert_eq!(body.drain_source, None);
        assert_eq!(body.retry_after_ms, None);
    }

    #[test]
    fn ready_response_reports_draining_mode() {
        let ops_state = OpsState::new(30_000);
        ops_state.begin_shutdown_drain();
        let ops = ops_state.snapshot();
        let (status, Json(body)) = ready_response(
            &ops,
            Some("draining"),
            Some(ControlPlaneReadinessDetail::draining()),
        );

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.status, "starting");
        assert_eq!(body.mode, "draining");
        assert_eq!(body.reason, Some("draining"));
        assert_eq!(body.detail, Some(ControlPlaneReadinessDetail::draining()));
        assert_eq!(body.drain_source, Some("shutdown"));
        assert!(body.retry_after_ms.is_some());
    }

    #[test]
    fn replica_append_classifies_replayed_and_next_seq() {
        assert_eq!(
            classify_replica_append(7, 7),
            ReplicaAppendDisposition::AlreadyCommitted
        );
        assert_eq!(
            classify_replica_append(7, 8),
            ReplicaAppendDisposition::CommitNext
        );
    }

    #[test]
    fn replica_append_rejects_seq_gaps() {
        assert_eq!(
            classify_replica_append(7, 9),
            ReplicaAppendDisposition::SeqGap { expected_seq: 8 }
        );
    }

    #[tokio::test]
    async fn stale_replica_epoch_is_rejected_with_local_owner_hint() {
        let state = test_state(Some("http://node-a:4001"));
        state.ownership.insert_test_lease("ses_demo", 9, None).await;

        let error = reject_stale_replica_epoch(&state, "ses_demo", 7)
            .await
            .expect_err("older replica epoch should be rejected");

        assert!(matches!(
            error,
            AppError::SessionNotOwned {
                owner_id,
                owner_public_url,
                epoch
            } if owner_id == "node-a"
                && owner_public_url.as_deref() == Some("http://node-a:4001")
                && epoch == 9
        ));
    }

    #[tokio::test]
    async fn same_or_newer_replica_epoch_is_allowed() {
        let state = test_state(None);
        state.ownership.insert_test_lease("ses_demo", 9, None).await;

        reject_stale_replica_epoch(&state, "ses_demo", 9)
            .await
            .expect("same epoch should pass");
        reject_stale_replica_epoch(&state, "ses_demo", 10)
            .await
            .expect("newer epoch should pass");
        reject_stale_replica_epoch(&state, "ses_other", 1)
            .await
            .expect("missing local ownership should pass");
    }
}
