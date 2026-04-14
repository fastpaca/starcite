use std::collections::HashMap;
use std::time::Instant;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection, ws::WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{
    AppState,
    archive_queue::ArchiveQueueSnapshot,
    auth,
    config::CommitMode,
    control_plane::ControlPlaneSnapshot,
    edge_routing::{self, EventPathRequest},
    error::{self, AppError},
    fanout::{LifecycleFanoutSnapshot, SessionFanoutSnapshot},
    flush_queue::PendingFlushSnapshot,
    hot_store::HotEventStoreSnapshot,
    lifecycle_scope::{resolve_lifecycle_options, resolve_session_lifecycle},
    model::{
        CreateSessionRequest, EventsOptions, LifecycleEvent, LifecyclePage, UpdateSessionRequest,
    },
    ops::OpsSnapshot,
    owner_proxy::build_tail_ws_url,
    ownership::OwnershipSnapshot,
    query_options::{parse_events_options, parse_list_options, parse_tail_options},
    read_path,
    replication::{AppendReplicaRequest, ReplicationAck, ReplicationSnapshot},
    repository,
    request_validation::{parse_append_request, read_append_body, validate_session_id},
    runtime::{RuntimeSnapshot, RuntimeTouchReason},
    session_manager::SessionManagerSnapshot,
    session_store::{HotSessionStoreSnapshot, resolve_session, resolve_session_tenant_id},
    socket_runtime::{require_local_owner_for_event_path, run_lifecycle_session, run_tail_session},
    telemetry::{
        AuthOutcome, AuthSource, AuthStage, IngestOperation, IngestOutcome, RequestOperation,
        RequestOutcome, RequestPhase,
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
    commit_mode: &'static str,
    telemetry_enabled: bool,
    control_plane: ControlPlaneSnapshot,
    runtime: RuntimeSnapshot,
    hot_store: HotEventStoreSnapshot,
    session_store: HotSessionStoreSnapshot,
    session_manager: SessionManagerSnapshot,
    ownership: OwnershipSnapshot,
    replication: ReplicationSnapshot,
    pending_flush: PendingFlushSnapshot,
    archive_queue: ArchiveQueueSnapshot,
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
        commit_mode: commit_mode_name(state.commit_mode),
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
        .apply_local_async_replica_commit(request.event)
        .await;

    Ok((
        StatusCode::ACCEPTED,
        Json(ReplicationAck {
            status: "committed".to_string(),
        }),
    ))
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

pub async fn reject_when_draining(
    State(ops): State<crate::ops::OpsState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if ops.is_draining() {
        AppError::node_draining(&ops.snapshot()).into_response()
    } else {
        next.run(request).await
    }
}

pub async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let mut tenant_id = "unknown".to_string();
    let result: Result<(StatusCode, Json<crate::model::SessionResponse>), AppError> = async {
        let auth = authenticate_http(&state, &headers)?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        let validated = auth::validate_create_request(request, &auth)?;
        tenant_id = validated.tenant_id.clone();
        let session = repository::create_session(&state.pool, validated.clone()).await?;
        state
            .session_store
            .put_session(&validated.tenant_id, session.clone(), Some(0))
            .await;

        state
            .runtime
            .session_created(&session.id, &validated.tenant_id)
            .await;
        publish_lifecycle(
            &state,
            LifecycleEvent::created(validated.tenant_id, &session),
        )
        .await;

        Ok((StatusCode::CREATED, Json(session)))
    }
    .await;

    record_ingest_result(&state, IngestOperation::CreateSession, &tenant_id, &result);
    result
}

pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<crate::model::SessionsPage>, AppError> {
    let auth = authenticate_http(&state, &headers)?;
    let options = auth::apply_list_scope(&auth, parse_list_options(params)?)?;
    let page = repository::list_sessions(&state.pool, options).await?;
    Ok(Json(page))
}

pub async fn show_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    validate_session_id(&session_id)?;
    let auth = authenticate_http(&state, &headers)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    let session = resolve_session(&state.session_store, &state.pool, &session_id).await?;

    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::HttpRead)
        .await;

    Ok(Json(session))
}

pub async fn update_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Result<Json<UpdateSessionRequest>, JsonRejection>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    validate_session_id(&session_id)?;
    let mut tenant_id = "unknown".to_string();
    let result: Result<Json<crate::model::SessionResponse>, AppError> = async {
        let auth = authenticate_http(&state, &headers)?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        tenant_id =
            resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
        auth::allow_manage_session(&auth, &session_id, &tenant_id)?;
        state
            .runtime
            .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::HttpWrite)
            .await;

        let session =
            repository::update_session(&state.pool, &session_id, request.validate()?).await?;
        state
            .session_store
            .put_session(&tenant_id, session.clone(), None)
            .await;

        publish_lifecycle(&state, LifecycleEvent::updated(tenant_id.clone(), &session)).await;

        Ok(Json(session))
    }
    .await;

    record_ingest_result(&state, IngestOperation::UpdateSession, &tenant_id, &result);
    result
}

pub async fn archive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    validate_session_id(&session_id)?;
    let auth = authenticate_http(&state, &headers)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_manage_session(&auth, &session_id, &tenant_id)?;
    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::HttpWrite)
        .await;
    let outcome = repository::set_archive_state(&state.pool, &session_id, true).await?;
    state
        .session_store
        .put_session(&outcome.tenant_id, outcome.session.clone(), None)
        .await;

    if outcome.changed {
        publish_lifecycle(
            &state,
            LifecycleEvent::archived(outcome.tenant_id.clone(), &outcome.session),
        )
        .await;
    }

    Ok(Json(outcome.session))
}

pub async fn unarchive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    validate_session_id(&session_id)?;
    let auth = authenticate_http(&state, &headers)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_manage_session(&auth, &session_id, &tenant_id)?;
    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::HttpWrite)
        .await;
    let outcome = repository::set_archive_state(&state.pool, &session_id, false).await?;
    state
        .session_store
        .put_session(&outcome.tenant_id, outcome.session.clone(), None)
        .await;

    if outcome.changed {
        publish_lifecycle(
            &state,
            LifecycleEvent::unarchived(outcome.tenant_id.clone(), &outcome.session),
        )
        .await;
    }

    Ok(Json(outcome.session))
}

pub async fn append_event(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    request: Request<axum::body::Body>,
) -> Result<Response, AppError> {
    validate_session_id(&session_id)?;
    let started_at = Instant::now();
    let mut tenant_id = "unknown".to_string();
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let body = read_append_body(&headers, body).await?;
    let authorization = headers.get(axum::http::header::AUTHORIZATION);
    let result: Result<Response, AppError> = async {
        if let Some(proxied) = edge_routing::forward_cached_event_path(
            &state,
            &session_id,
            EventPathRequest::Append { body: &body },
            authorization,
        )
        .await?
        {
            return Ok(proxied);
        }

        let auth = authenticate_http(&state, &headers)?;
        let request = parse_append_request(&body)?;
        tenant_id =
            resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
        auth::allow_append_session(&auth, &session_id, &tenant_id)?;
        let validated = auth::validate_append_request(request, &auth)?;
        let ack_started_at = Instant::now();
        let outcome = state
            .session_manager
            .append(&session_id, &tenant_id, validated)
            .await;
        let response = match outcome {
            Ok(outcome) => {
                state
                    .runtime
                    .touch_existing(
                        &session_id,
                        &outcome.tenant_id,
                        RuntimeTouchReason::HttpWrite,
                    )
                    .await;

                Ok((StatusCode::CREATED, Json(outcome.reply)).into_response())
            }
            Err(AppError::SessionNotOwned {
                owner_public_url: Some(owner_public_url),
                ..
            }) => {
                state.session_manager.drop_worker_handle(&session_id).await;
                edge_routing::forward_known_event_path(
                    &state,
                    &session_id,
                    &owner_public_url,
                    EventPathRequest::Append { body: &body },
                    authorization,
                )
                .await
            }
            Err(error) => Err(error),
        };
        record_request_result(
            &state,
            RequestPhase::Ack,
            ack_started_at,
            response.as_ref().map(|_| ()),
        );

        response
    }
    .await;

    record_request_result(
        &state,
        RequestPhase::Total,
        started_at,
        result.as_ref().map(|_| ()),
    );
    record_ingest_result(&state, IngestOperation::AppendEvent, &tenant_id, &result);
    result
}

pub async fn lifecycle_events(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    let auth = authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let lifecycle = resolve_lifecycle_options(&state, &auth, &params).await?;
    if let Some(session_id) = lifecycle.session_id.as_deref() {
        state
            .runtime
            .touch_existing(
                session_id,
                &lifecycle.tenant_id,
                RuntimeTouchReason::RawLifecycle,
            )
            .await;
    }
    let receiver = state.lifecycle.subscribe_tenant(&lifecycle.tenant_id).await;

    Ok(websocket.on_upgrade(move |socket| async move {
        run_lifecycle_session(socket, state, lifecycle, receiver, expiry).await;
    }))
}

pub async fn read_lifecycle(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LifecyclePage>, AppError> {
    let auth = authenticate_http(&state, &headers)?;
    let lifecycle = resolve_lifecycle_options(&state, &auth, &params).await?;
    let page = repository::read_lifecycle_events(
        &state.pool,
        &lifecycle.tenant_id,
        lifecycle.session_id.as_deref(),
        EventsOptions {
            cursor: lifecycle.cursor,
            limit: parse_events_options(params)?.limit,
        },
    )
    .await?;

    if let Some(session_id) = lifecycle.session_id.as_deref() {
        state
            .runtime
            .touch_existing(
                session_id,
                &lifecycle.tenant_id,
                RuntimeTouchReason::HttpLifecycle,
            )
            .await;
    }

    Ok(Json(page))
}

pub async fn session_lifecycle_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    validate_session_id(&session_id)?;

    let auth = authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let cursor = parse_events_options(params)?.cursor;
    let lifecycle = resolve_session_lifecycle(&state, &auth, &session_id, cursor).await?;
    state
        .runtime
        .touch_existing(
            &session_id,
            &lifecycle.tenant_id,
            RuntimeTouchReason::RawLifecycle,
        )
        .await;
    let receiver = state.lifecycle.subscribe_session(&session_id).await;

    Ok(websocket.on_upgrade(move |socket| async move {
        run_lifecycle_session(socket, state, lifecycle, receiver, expiry).await;
    }))
}

pub async fn read_session_lifecycle(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LifecyclePage>, AppError> {
    validate_session_id(&session_id)?;

    let auth = authenticate_http(&state, &headers)?;
    let options = parse_events_options(params)?;
    let lifecycle = resolve_session_lifecycle(&state, &auth, &session_id, options.cursor).await?;
    let page = repository::read_lifecycle_events(
        &state.pool,
        &lifecycle.tenant_id,
        Some(&session_id),
        options,
    )
    .await?;

    state
        .runtime
        .touch_existing(
            &session_id,
            &lifecycle.tenant_id,
            RuntimeTouchReason::HttpLifecycle,
        )
        .await;

    Ok(Json(page))
}

pub async fn read_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    validate_session_id(&session_id)?;
    let authorization = headers.get(axum::http::header::AUTHORIZATION);

    if let Some(proxied) = edge_routing::forward_cached_event_path(
        &state,
        &session_id,
        EventPathRequest::ReadEvents { params: &params },
        authorization,
    )
    .await?
    {
        return Ok(proxied);
    }

    let auth = authenticate_http(&state, &headers)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    match require_local_owner_for_event_path(&state, &session_id).await {
        Ok(()) => {}
        Err(AppError::SessionNotOwned {
            owner_public_url: Some(owner_public_url),
            ..
        }) => {
            return edge_routing::forward_known_event_path(
                &state,
                &session_id,
                &owner_public_url,
                EventPathRequest::ReadEvents { params: &params },
                authorization,
            )
            .await;
        }
        Err(error) => return Err(error),
    }
    let page = read_path::read_events(
        &state.hot_store,
        &state.pool,
        &session_id,
        parse_events_options(params)?,
    )
    .await?;

    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::HttpRead)
        .await;

    Ok(Json(page).into_response())
}

pub async fn tail_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<Response, AppError> {
    validate_session_id(&session_id)?;

    let auth = authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let tail = parse_tail_options(params.clone())?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    match require_local_owner_for_event_path(&state, &session_id).await {
        Ok(()) => {}
        Err(AppError::SessionNotOwned {
            owner_id,
            owner_public_url: Some(owner_public_url),
            epoch,
        }) => {
            if let Some(owner_ws_url) = build_tail_ws_url(&owner_public_url, &session_id, &params) {
                return Ok(tail_owner_redirect_response(
                    &owner_id,
                    &owner_public_url,
                    epoch,
                    &owner_ws_url,
                ));
            }

            return Err(AppError::SessionNotOwned {
                owner_id,
                owner_public_url: Some(owner_public_url),
                epoch,
            });
        }
        Err(error) => return Err(error),
    }
    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::RawTail)
        .await;
    let receiver = state.fanout.subscribe(&session_id).await;

    Ok(websocket
        .on_upgrade(move |socket| async move {
            run_tail_session(socket, state, session_id, tail, receiver, expiry).await;
        })
        .into_response())
}

fn tail_owner_redirect_response(
    owner_id: &str,
    owner_public_url: &str,
    epoch: i64,
    owner_ws_url: &str,
) -> Response {
    let mut response = (
        StatusCode::TEMPORARY_REDIRECT,
        Json(serde_json::json!({
            "error": "session_not_owned",
            "message": "Session is not owned by this node",
            "owner_id": owner_id,
            "owner_url": owner_public_url,
            "owner_ws_url": owner_ws_url,
            "epoch": epoch
        })),
    )
        .into_response();

    if let Ok(value) = HeaderValue::from_str(owner_public_url) {
        response
            .headers_mut()
            .insert(error::OWNER_URL_HEADER.clone(), value);
    }

    if let Ok(value) = HeaderValue::from_str(owner_ws_url) {
        response
            .headers_mut()
            .insert(error::OWNER_WEBSOCKET_URL_HEADER.clone(), value.clone());
        response.headers_mut().insert(header::LOCATION, value);
    }

    response
}

fn authenticate_http(state: &AppState, headers: &HeaderMap) -> Result<auth::AuthContext, AppError> {
    let started_at = Instant::now();
    let result = auth::authenticate_http(headers, state.auth_mode);
    record_auth_result(&state.telemetry, state.auth_mode, started_at, &result);
    result
}

fn authenticate_socket(
    state: &AppState,
    params: &HashMap<String, String>,
) -> Result<auth::AuthContext, AppError> {
    let started_at = Instant::now();
    let result = auth::authenticate_socket(params, state.auth_mode);
    record_auth_result(&state.telemetry, state.auth_mode, started_at, &result);
    result
}

fn record_auth_result(
    telemetry: &crate::telemetry::Telemetry,
    auth_mode: crate::config::AuthMode,
    started_at: Instant,
    result: &Result<auth::AuthContext, AppError>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(_) => telemetry.record_auth(
            AuthStage::Plug,
            auth_mode,
            AuthOutcome::Ok,
            duration_ms,
            "none",
            AuthSource::None,
        ),
        Err(error) => telemetry.record_auth(
            AuthStage::Plug,
            auth_mode,
            AuthOutcome::Error,
            duration_ms,
            error.error_code(),
            AuthSource::None,
        ),
    }
}

fn record_ingest_result<T>(
    state: &AppState,
    operation: IngestOperation,
    tenant_id: &str,
    result: &Result<T, AppError>,
) {
    match result {
        Ok(_) => {
            state
                .telemetry
                .record_ingest_edge(operation, tenant_id, IngestOutcome::Ok, "none")
        }
        Err(error) => state.telemetry.record_ingest_edge(
            operation,
            tenant_id,
            IngestOutcome::Error,
            error.error_code(),
        ),
    }
}

fn record_request_result<T>(
    state: &AppState,
    phase: RequestPhase,
    started_at: Instant,
    result: Result<T, &AppError>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(_) => state.telemetry.record_request(
            RequestOperation::AppendEvent,
            phase,
            RequestOutcome::Ok,
            duration_ms,
            "none",
        ),
        Err(error) => state.telemetry.record_request(
            RequestOperation::AppendEvent,
            phase,
            request_outcome(error),
            duration_ms,
            error.error_code(),
        ),
    }
}

fn request_outcome(_error: &AppError) -> RequestOutcome {
    RequestOutcome::Error
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
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
        crate::config::AuthMode::UnsafeJwt => "unsafe_jwt",
    }
}

fn commit_mode_name(commit_mode: crate::config::CommitMode) -> &'static str {
    match commit_mode {
        CommitMode::SyncPostgres => "sync_postgres",
        CommitMode::LocalAsync => "local_async",
    }
}

fn reject_internal_replication_when_unavailable(state: &AppState) -> Result<(), AppError> {
    if state.ops.is_draining() {
        return Err(AppError::node_draining(&state.ops.snapshot()));
    }

    if state.commit_mode != CommitMode::LocalAsync {
        return Err(AppError::Internal);
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

async fn publish_lifecycle(state: &AppState, event: LifecycleEvent) {
    match repository::append_lifecycle_event(&state.pool, event, &state.instance_id).await {
        Ok(event) => {
            state.session_store.apply_lifecycle_hint(&event.event).await;
            state.lifecycle.broadcast(event).await;
        }
        Err(error) => {
            tracing::error!(error = ?error, "failed to persist lifecycle event");
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        Json, body,
        http::{StatusCode, header},
    };

    use super::{
        ReplicaAppendDisposition, classify_replica_append, ready_response,
        tail_owner_redirect_response,
    };
    use crate::{
        error::{OWNER_URL_HEADER, OWNER_WEBSOCKET_URL_HEADER},
        ops::OpsState,
    };

    #[tokio::test]
    async fn tail_owner_redirect_sets_headers_and_body() {
        let response = tail_owner_redirect_response(
            "node-a",
            "http://127.0.0.1:4191",
            7,
            "ws://127.0.0.1:4191/v1/sessions/ses_demo/tail?cursor=2&batch_size=8",
        );

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response
                .headers()
                .get(&OWNER_URL_HEADER)
                .expect("owner URL"),
            "http://127.0.0.1:4191"
        );
        assert_eq!(
            response
                .headers()
                .get(&OWNER_WEBSOCKET_URL_HEADER)
                .expect("owner websocket URL"),
            "ws://127.0.0.1:4191/v1/sessions/ses_demo/tail?cursor=2&batch_size=8"
        );
        assert_eq!(
            response.headers().get(header::LOCATION).expect("location"),
            "ws://127.0.0.1:4191/v1/sessions/ses_demo/tail?cursor=2&batch_size=8"
        );

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let json_body: serde_json::Value = serde_json::from_slice(&body).expect("body json");

        assert_eq!(json_body["error"], "session_not_owned");
        assert_eq!(json_body["owner_url"], "http://127.0.0.1:4191");
        assert_eq!(
            json_body["owner_ws_url"],
            "ws://127.0.0.1:4191/v1/sessions/ses_demo/tail?cursor=2&batch_size=8"
        );
        assert_eq!(json_body["epoch"], 7);
    }

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
