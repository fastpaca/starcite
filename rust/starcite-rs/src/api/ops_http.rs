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
        let mut response = ready_response(&ops, Some("draining")).into_response();
        error::apply_drain_headers(response.headers_mut(), ops.drain_source, ops.retry_after_ms);
        return response;
    }

    match sqlx::query_scalar::<_, i64>("SELECT 1::bigint")
        .fetch_one(&state.pool)
        .await
    {
        Ok(_) => ready_response(&ops, None).into_response(),
        Err(error) => {
            tracing::error!(error = ?error, "readiness query failed");
            ready_response(&ops, Some("database_unavailable")).into_response()
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
        .apply_replica_commit(request.event)
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
    use axum::{Json, http::StatusCode};

    use super::{ReplicaAppendDisposition, classify_replica_append, ready_response};
    use crate::runtime::OpsState;

    #[test]
    fn ready_response_reports_ready_mode() {
        let ops = OpsState::new(30_000).snapshot();
        let (status, Json(body)) = ready_response(&ops, None);

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.status, "ok");
        assert_eq!(body.mode, "ready");
        assert_eq!(body.reason, None);
        assert_eq!(body.drain_source, None);
        assert_eq!(body.retry_after_ms, None);
    }

    #[test]
    fn ready_response_reports_draining_mode() {
        let ops_state = OpsState::new(30_000);
        ops_state.begin_shutdown_drain();
        let ops = ops_state.snapshot();
        let (status, Json(body)) = ready_response(&ops, Some("draining"));

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body.status, "starting");
        assert_eq!(body.mode, "draining");
        assert_eq!(body.reason, Some("draining"));
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
}
