use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::{
    Json,
    extract::{
        Path, Query, State,
        rejection::JsonRejection,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    http::{HeaderMap, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use tokio::{sync::broadcast, time::sleep};

use crate::{
    AppState,
    archive_queue::ArchiveQueueSnapshot,
    auth,
    config::{DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT},
    error::{self, AppError},
    fanout::{LifecycleFanoutSnapshot, SessionFanoutSnapshot},
    hot_store::HotEventStoreSnapshot,
    model::{
        AppendEventRequest, ArchivedFilter, CreateSessionRequest, EventResponse, EventsOptions,
        LifecycleEvent, LifecyclePage, LifecycleResponse, ListOptions, UpdateSessionRequest,
        parse_query_scalar,
    },
    ops::OpsSnapshot,
    read_path, repository,
    runtime::{RuntimeSnapshot, RuntimeTouchReason},
    session_store::{
        HotSessionStoreSnapshot, resolve_session, resolve_session_last_seq,
        resolve_session_tenant_id,
    },
    telemetry::{
        AuthOutcome, AuthSource, AuthStage, IngestOperation, IngestOutcome, ReadOperation,
        ReadOutcome, ReadPhase, RequestOperation, RequestOutcome, RequestPhase, SocketSurface,
        SocketTransport,
    },
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TailOptions {
    cursor: i64,
    batch_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LifecycleOptions {
    tenant_id: String,
    cursor: i64,
    session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
struct TailEventsFrame {
    events: Vec<EventResponse>,
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
struct TailGapFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
    from_cursor: i64,
    next_cursor: i64,
    committed_cursor: i64,
    earliest_available_cursor: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TokenExpiredFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct NodeDrainingFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain_source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct DebugStateResponse {
    ops: OpsSnapshot,
    auth_mode: &'static str,
    telemetry_enabled: bool,
    runtime: RuntimeSnapshot,
    hot_store: HotEventStoreSnapshot,
    session_store: HotSessionStoreSnapshot,
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
    let runtime = state.runtime.snapshot().await;
    let hot_store = state.hot_store.snapshot().await;
    let session_store = state.session_store.snapshot().await;
    let archive_queue = state.archive_queue.snapshot().await;
    let events = state.fanout.snapshot().await;
    let lifecycle = state.lifecycle.snapshot().await;

    Json(DebugStateResponse {
        ops,
        auth_mode: auth_mode_name(state.auth_mode),
        telemetry_enabled: state.telemetry.enabled(),
        runtime,
        hot_store,
        session_store,
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
            .put_session(&validated.tenant_id, session.clone())
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
            .put_session(&tenant_id, session.clone())
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
        .put_session(&outcome.tenant_id, outcome.session.clone())
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
        .put_session(&outcome.tenant_id, outcome.session.clone())
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
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Result<Json<AppendEventRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    validate_session_id(&session_id)?;
    let started_at = Instant::now();
    let mut tenant_id = "unknown".to_string();
    let result: Result<(StatusCode, Json<crate::model::AppendReply>), AppError> = async {
        let auth = authenticate_http(&state, &headers)?;
        let Json(request) = body.map_err(|_| AppError::InvalidEvent)?;
        tenant_id =
            resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
        auth::allow_append_session(&auth, &session_id, &tenant_id)?;
        let validated = auth::validate_append_request(request, &auth)?;
        let ack_started_at = Instant::now();
        let outcome =
            repository::append_event(&state.pool, &session_id, validated, &state.instance_id).await;
        record_request_result(
            &state,
            RequestPhase::Ack,
            ack_started_at,
            outcome.as_ref().map(|_| ()),
        );
        let outcome = outcome?;

        state
            .runtime
            .touch_existing(
                &session_id,
                &outcome.tenant_id,
                RuntimeTouchReason::HttpWrite,
            )
            .await;

        if let Some(event) = outcome.event.clone() {
            state.hot_store.put_event(event.clone()).await;
            state.archive_queue.enqueue(&session_id).await;
            state
                .session_store
                .bump_last_seq(&session_id, &outcome.tenant_id, event.seq)
                .await;
            state.fanout.broadcast(event).await;
        }

        Ok((StatusCode::CREATED, Json(outcome.reply)))
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
) -> Result<Json<crate::model::EventsPage>, AppError> {
    validate_session_id(&session_id)?;
    let auth = authenticate_http(&state, &headers)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
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

    Ok(Json(page))
}

pub async fn tail_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    validate_session_id(&session_id)?;

    let auth = authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let tail = parse_tail_options(params.clone())?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::RawTail)
        .await;
    let receiver = state.fanout.subscribe(&session_id).await;

    Ok(websocket.on_upgrade(move |socket| async move {
        run_tail_session(socket, state, session_id, tail, receiver, expiry).await;
    }))
}

fn validate_session_id(session_id: &str) -> Result<(), AppError> {
    if session_id.is_empty() {
        Err(AppError::InvalidSessionId)
    } else {
        Ok(())
    }
}

fn parse_list_options(params: HashMap<String, String>) -> Result<ListOptions, AppError> {
    let mut metadata = serde_json::Map::new();
    let mut tenant_id = None;
    let mut limit = DEFAULT_LIST_LIMIT;
    let mut cursor = None;
    let mut archived = ArchivedFilter::Active;

    for (key, value) in params {
        match key.as_str() {
            "limit" => limit = parse_limit(&value)?,
            "cursor" => {
                if value.is_empty() {
                    return Err(AppError::InvalidCursor);
                }

                cursor = Some(value);
            }
            "archived" => archived = parse_archived_filter(&value)?,
            "tenant_id" => {
                if value.is_empty() {
                    return Err(AppError::InvalidListQuery);
                }

                tenant_id = Some(value);
            }
            _ => {
                if let Some(key) = metadata_key(&key) {
                    metadata.insert(key.to_string(), parse_query_scalar(&value));
                }
            }
        }
    }

    Ok(ListOptions {
        limit,
        cursor,
        archived,
        metadata,
        tenant_id,
        session_id: None,
    })
}

fn parse_events_options(params: HashMap<String, String>) -> Result<EventsOptions, AppError> {
    let mut cursor = 0_i64;
    let mut limit = DEFAULT_LIST_LIMIT;

    for (key, value) in params {
        match key.as_str() {
            "cursor" => {
                cursor = value.parse::<i64>().map_err(|_| AppError::InvalidCursor)?;

                if cursor < 0 {
                    return Err(AppError::InvalidCursor);
                }
            }
            "limit" => limit = parse_limit(&value)?,
            _ => {}
        }
    }

    Ok(EventsOptions { cursor, limit })
}

fn parse_tail_options(params: HashMap<String, String>) -> Result<TailOptions, AppError> {
    let mut cursor = 0_i64;
    let mut batch_size = 1_u32;

    for (key, value) in params {
        match key.as_str() {
            "cursor" => {
                cursor = value.parse::<i64>().map_err(|_| AppError::InvalidCursor)?;

                if cursor < 0 {
                    return Err(AppError::InvalidCursor);
                }
            }
            "batch_size" => {
                batch_size = value
                    .parse::<u32>()
                    .map_err(|_| AppError::InvalidTailBatchSize)?;

                if !(1..=MAX_LIST_LIMIT).contains(&batch_size) {
                    return Err(AppError::InvalidTailBatchSize);
                }
            }
            _ => {}
        }
    }

    Ok(TailOptions { cursor, batch_size })
}

fn parse_lifecycle_options(params: HashMap<String, String>) -> Result<LifecycleOptions, AppError> {
    let cursor = parse_events_options(params.clone())?.cursor;
    let session_id = parse_optional_session_id(&params)?;

    match params
        .get("tenant_id")
        .filter(|tenant_id| !tenant_id.is_empty())
    {
        Some(tenant_id) => Ok(LifecycleOptions {
            tenant_id: tenant_id.clone(),
            cursor,
            session_id,
        }),
        None => Err(AppError::InvalidTenantId),
    }
}

async fn resolve_lifecycle_options(
    state: &AppState,
    auth: &auth::AuthContext,
    params: &HashMap<String, String>,
) -> Result<LifecycleOptions, AppError> {
    let mut lifecycle = match auth.kind {
        crate::config::AuthMode::None => parse_lifecycle_options(params.clone())?,
        crate::config::AuthMode::UnsafeJwt => {
            auth::can_subscribe_lifecycle(auth)?;
            LifecycleOptions {
                tenant_id: auth.principal.tenant_id.clone(),
                cursor: parse_events_options(params.clone())?.cursor,
                session_id: parse_optional_session_id(params)?,
            }
        }
    };

    validate_lifecycle_scope(state, auth, &mut lifecycle).await?;
    Ok(lifecycle)
}

async fn resolve_session_lifecycle(
    state: &AppState,
    auth: &auth::AuthContext,
    session_id: &str,
    cursor: i64,
) -> Result<LifecycleOptions, AppError> {
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, session_id).await?;
    auth::allow_read_session(auth, session_id, &tenant_id)?;

    Ok(LifecycleOptions {
        tenant_id,
        cursor,
        session_id: Some(session_id.to_string()),
    })
}

fn parse_archived_filter(raw: &str) -> Result<ArchivedFilter, AppError> {
    match raw {
        "false" => Ok(ArchivedFilter::Active),
        "true" => Ok(ArchivedFilter::Archived),
        "all" => Ok(ArchivedFilter::All),
        _ => Err(AppError::InvalidListQuery),
    }
}

fn parse_limit(raw: &str) -> Result<u32, AppError> {
    let parsed = raw.parse::<u32>().map_err(|_| AppError::InvalidLimit)?;

    if (1..=MAX_LIST_LIMIT).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(AppError::InvalidLimit)
    }
}

fn metadata_key(raw: &str) -> Option<&str> {
    raw.strip_prefix("metadata.")
        .or_else(|| raw.strip_prefix("metadata[")?.strip_suffix(']'))
        .filter(|key| !key.is_empty())
}

fn parse_optional_session_id(params: &HashMap<String, String>) -> Result<Option<String>, AppError> {
    match params.get("session_id") {
        None => Ok(None),
        Some(value) if value.is_empty() => Err(AppError::InvalidSessionId),
        Some(value) => Ok(Some(value.clone())),
    }
}

async fn validate_lifecycle_scope(
    state: &AppState,
    auth: &auth::AuthContext,
    lifecycle: &mut LifecycleOptions,
) -> Result<(), AppError> {
    let Some(session_id) = lifecycle.session_id.as_ref() else {
        return Ok(());
    };

    validate_session_id(session_id)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, session_id).await?;

    if tenant_id != lifecycle.tenant_id {
        return Err(AppError::ForbiddenTenant);
    }

    auth::allow_read_session(auth, session_id, &tenant_id)?;
    Ok(())
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

fn record_read_result(
    state: &AppState,
    operation: ReadOperation,
    started_at: Instant,
    result: Result<(), ()>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(()) => {
            state
                .telemetry
                .record_read(operation, ReadPhase::Deliver, ReadOutcome::Ok, duration_ms)
        }
        Err(()) => state.telemetry.record_read(
            operation,
            ReadPhase::Deliver,
            ReadOutcome::Error,
            duration_ms,
        ),
    }
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

async fn run_tail_session(
    mut socket: WebSocket,
    state: AppState,
    session_id: String,
    tail: TailOptions,
    mut receiver: broadcast::Receiver<EventResponse>,
    expiry_delay: Option<Duration>,
) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Raw, SocketSurface::Tail);
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Raw, SocketSurface::Tail);
    let _fanout_guard = state.fanout.session_guard(session_id.clone());
    let mut cursor = tail.cursor;
    let mut expiry = expiry_delay.map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining(&mut socket, &state.ops).await;
        return;
    }

    match sync_tail(&mut socket, &state, &session_id, cursor, tail.batch_size).await {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(error = ?error, session_id, "tail replay failed");
            return;
        }
    }

    loop {
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
                    let _ = send_token_expired(&mut socket).await;
                    return;
                }
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        session_id = %session_id,
                        "closing raw tail socket because node is draining"
                    );
                    let _ = send_node_draining(&mut socket, &state.ops).await;
                    return;
                }
                incoming = socket.recv() => {
                    if !handle_socket_message(&mut socket, incoming).await {
                        return;
                    }
                }
                received = receiver.recv() => match received {
                    Ok(event) if event.seq <= cursor => continue,
                    Ok(event) => {
                        cursor = event.seq;
                        let started_at = Instant::now();
                        let result = send_events(&mut socket, &[event]).await;
                        record_read_result(&state, ReadOperation::TailLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            session_id,
                            skipped,
                            cursor,
                            "tail broadcast lagged, replaying from store"
                        );

                        match sync_tail(&mut socket, &state, &session_id, cursor, tail.batch_size).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(error = ?error, session_id, "tail replay after lag failed");
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        } else {
            tokio::select! {
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        session_id = %session_id,
                        "closing raw tail socket because node is draining"
                    );
                    let _ = send_node_draining(&mut socket, &state.ops).await;
                    return;
                }
                incoming = socket.recv() => {
                    if !handle_socket_message(&mut socket, incoming).await {
                        return;
                    }
                }
                received = receiver.recv() => match received {
                    Ok(event) if event.seq <= cursor => continue,
                    Ok(event) => {
                        cursor = event.seq;
                        let started_at = Instant::now();
                        let result = send_events(&mut socket, &[event]).await;
                        record_read_result(&state, ReadOperation::TailLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            session_id,
                            skipped,
                            cursor,
                            "tail broadcast lagged, replaying from store"
                        );

                        match sync_tail(&mut socket, &state, &session_id, cursor, tail.batch_size).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(error = ?error, session_id, "tail replay after lag failed");
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

async fn sync_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    cursor: i64,
    batch_size: u32,
) -> Result<i64, AppError> {
    let next_cursor = replay_tail(socket, state, session_id, cursor, batch_size).await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let last_seq = resolve_session_last_seq(&state.session_store, &state.pool, session_id).await?;

    if cursor > last_seq {
        let gap = build_resume_invalidated_gap(cursor, last_seq);
        send_gap(socket, &gap)
            .await
            .map_err(|_| AppError::Internal)?;
        Ok(last_seq)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    mut cursor: i64,
    batch_size: u32,
) -> Result<i64, AppError> {
    loop {
        let page = read_path::read_events(
            &state.hot_store,
            &state.pool,
            session_id,
            EventsOptions {
                cursor,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for events in page.events.chunks(batch_size as usize) {
            cursor = events.last().map(|event| event.seq).unwrap_or(cursor);
            let started_at = Instant::now();
            let result = send_events(socket, events).await;
            record_read_result(state, ReadOperation::TailCatchup, started_at, result);
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

async fn run_lifecycle_session(
    mut socket: WebSocket,
    state: AppState,
    lifecycle: LifecycleOptions,
    mut receiver: broadcast::Receiver<LifecycleResponse>,
    expiry_delay: Option<Duration>,
) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Raw, lifecycle_socket_surface(&lifecycle));
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Raw, lifecycle_socket_surface(&lifecycle));
    let _fanout_guard = match lifecycle.session_id.as_ref() {
        Some(session_id) => state.lifecycle.session_guard(session_id.clone()),
        None => state.lifecycle.tenant_guard(lifecycle.tenant_id.clone()),
    };
    let mut cursor = lifecycle.cursor;
    let mut expiry = expiry_delay.map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining(&mut socket, &state.ops).await;
        return;
    }

    match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(
                error = ?error,
                tenant_id = lifecycle.tenant_id,
                "lifecycle replay failed"
            );
            return;
        }
    }

    loop {
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
                    let _ = send_token_expired(&mut socket).await;
                    return;
                }
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        tenant_id = %lifecycle.tenant_id,
                        session_id = ?lifecycle.session_id,
                        "closing raw lifecycle socket because node is draining"
                    );
                    let _ = send_node_draining(&mut socket, &state.ops).await;
                    return;
                }
                incoming = socket.recv() => {
                    if !handle_socket_message(&mut socket, incoming).await {
                        return;
                    }
                }
                received = receiver.recv() => match received {
                    Ok(event) if event.cursor <= cursor => continue,
                    Ok(event)
                        if lifecycle
                            .session_id
                            .as_ref()
                            .is_some_and(|session_id| event.event.session_id() != session_id) =>
                    {
                        continue;
                    }
                    Ok(event) => {
                        cursor = event.cursor;
                        let started_at = Instant::now();
                        let result = send_lifecycle(&mut socket, &event).await;
                        record_read_result(&state, ReadOperation::LifecycleLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            tenant_id = lifecycle.tenant_id,
                            cursor,
                            "lifecycle broadcast lagged, replaying from store"
                        );

                        match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(
                                    error = ?error,
                                    tenant_id = lifecycle.tenant_id,
                                    "lifecycle replay after lag failed"
                                );
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        } else {
            tokio::select! {
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        tenant_id = %lifecycle.tenant_id,
                        session_id = ?lifecycle.session_id,
                        "closing raw lifecycle socket because node is draining"
                    );
                    let _ = send_node_draining(&mut socket, &state.ops).await;
                    return;
                }
                incoming = socket.recv() => {
                    if !handle_socket_message(&mut socket, incoming).await {
                        return;
                    }
                }
                received = receiver.recv() => match received {
                    Ok(event) if event.cursor <= cursor => continue,
                    Ok(event)
                        if lifecycle
                            .session_id
                            .as_ref()
                            .is_some_and(|session_id| event.event.session_id() != session_id) =>
                    {
                        continue;
                    }
                    Ok(event) => {
                        cursor = event.cursor;
                        let started_at = Instant::now();
                        let result = send_lifecycle(&mut socket, &event).await;
                        record_read_result(&state, ReadOperation::LifecycleLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            tenant_id = lifecycle.tenant_id,
                            cursor,
                            "lifecycle broadcast lagged, replaying from store"
                        );

                        match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(
                                    error = ?error,
                                    tenant_id = lifecycle.tenant_id,
                                    "lifecycle replay after lag failed"
                                );
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

async fn sync_lifecycle(
    socket: &mut WebSocket,
    state: &AppState,
    lifecycle: &LifecycleOptions,
    cursor: i64,
) -> Result<i64, AppError> {
    let next_cursor = replay_lifecycle(socket, state, lifecycle, cursor).await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let head = repository::lifecycle_head_seq(
        &state.pool,
        &lifecycle.tenant_id,
        lifecycle.session_id.as_deref(),
    )
    .await?;

    if cursor > head {
        let earliest_available_cursor = if head == 0 { 0 } else { 1 };
        let gap =
            build_resume_invalidated_gap_with_earliest(cursor, head, earliest_available_cursor);
        send_gap(socket, &gap)
            .await
            .map_err(|_| AppError::Internal)?;
        Ok(head)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_lifecycle(
    socket: &mut WebSocket,
    state: &AppState,
    lifecycle: &LifecycleOptions,
    mut cursor: i64,
) -> Result<i64, AppError> {
    loop {
        let page = repository::read_lifecycle_events(
            &state.pool,
            &lifecycle.tenant_id,
            lifecycle.session_id.as_deref(),
            EventsOptions {
                cursor,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for event in page.events {
            cursor = event.cursor;
            let started_at = Instant::now();
            let result = send_lifecycle(socket, &event).await;
            record_read_result(state, ReadOperation::LifecycleCatchup, started_at, result);
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

fn lifecycle_socket_surface(lifecycle: &LifecycleOptions) -> SocketSurface {
    if lifecycle.session_id.is_some() {
        SocketSurface::SessionLifecycle
    } else {
        SocketSurface::TenantLifecycle
    }
}

async fn handle_socket_message(
    socket: &mut WebSocket,
    incoming: Option<Result<Message, axum::Error>>,
) -> bool {
    match incoming {
        Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await.is_ok(),
        Some(Ok(Message::Close(_))) | None => false,
        Some(Ok(_)) => true,
        Some(Err(error)) => {
            tracing::warn!(error = ?error, "raw websocket receive failed");
            false
        }
    }
}

fn build_resume_invalidated_gap(from_cursor: i64, last_seq: i64) -> TailGapFrame {
    build_resume_invalidated_gap_with_earliest(from_cursor, last_seq, 1)
}

fn build_token_expired_frame() -> TokenExpiredFrame {
    TokenExpiredFrame {
        frame_type: "token_expired",
        reason: "token_expired",
    }
}

fn build_node_draining_frame(ops: &crate::ops::OpsSnapshot) -> NodeDrainingFrame {
    NodeDrainingFrame {
        frame_type: "node_draining",
        reason: "node_draining",
        drain_source: ops.drain_source,
        retry_after_ms: ops.retry_after_ms,
    }
}

fn build_resume_invalidated_gap_with_earliest(
    from_cursor: i64,
    last_seq: i64,
    earliest_available_cursor: i64,
) -> TailGapFrame {
    TailGapFrame {
        frame_type: "gap",
        reason: "resume_invalidated",
        from_cursor,
        next_cursor: last_seq,
        committed_cursor: last_seq,
        earliest_available_cursor,
    }
}

async fn send_events(socket: &mut WebSocket, events: &[EventResponse]) -> Result<(), ()> {
    let message = serde_json::to_string(&TailEventsFrame {
        events: events.to_vec(),
    })
    .map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

async fn send_lifecycle(socket: &mut WebSocket, event: &LifecycleResponse) -> Result<(), ()> {
    let message = serde_json::to_string(event).map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

async fn send_gap(socket: &mut WebSocket, gap: &TailGapFrame) -> Result<(), ()> {
    let message = serde_json::to_string(gap).map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

async fn send_token_expired(socket: &mut WebSocket) -> Result<(), ()> {
    let message = serde_json::to_string(&build_token_expired_frame()).map_err(|_| ())?;
    send_terminal_message(socket, message, close_code::POLICY, "token_expired").await
}

async fn send_node_draining(socket: &mut WebSocket, ops: &crate::ops::OpsState) -> Result<(), ()> {
    let message =
        serde_json::to_string(&build_node_draining_frame(&ops.snapshot())).map_err(|_| ())?;
    send_terminal_message(socket, message, close_code::RESTART, "node_draining").await
}

async fn send_terminal_message(
    socket: &mut WebSocket,
    message: String,
    code: u16,
    reason: &'static str,
) -> Result<(), ()> {
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())?;
    socket
        .send(Message::Close(Some(CloseFrame {
            code: code.into(),
            reason: reason.into(),
        })))
        .await
        .map_err(|_| ())
}

async fn wait_for_drain(ops: &crate::ops::OpsState) {
    if ops.is_draining() {
        return;
    }

    loop {
        sleep(Duration::from_millis(100)).await;

        if ops.is_draining() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use axum::{Json, http::StatusCode};
    use serde_json::json;

    use super::{
        LifecycleOptions, TailOptions, build_node_draining_frame, build_resume_invalidated_gap,
        build_token_expired_frame, parse_archived_filter, parse_events_options,
        parse_lifecycle_options, parse_list_options, parse_tail_options, ready_response,
    };
    use crate::{model::ArchivedFilter, ops::OpsState};

    #[test]
    fn list_query_supports_bracket_metadata_filters() {
        let params = HashMap::from([
            ("metadata[marker]".to_string(), "hot".to_string()),
            ("limit".to_string(), "50".to_string()),
        ]);

        let opts = parse_list_options(params).expect("query should parse");

        assert_eq!(opts.limit, 50);
        assert_eq!(opts.metadata.get("marker"), Some(&json!("hot")));
    }

    #[test]
    fn events_query_defaults_cursor_to_zero() {
        let opts = parse_events_options(HashMap::new()).expect("query should parse");
        assert_eq!(opts.cursor, 0);
    }

    #[test]
    fn tail_cursor_uses_same_validation_as_events_query() {
        let params = HashMap::from([("cursor".to_string(), "12".to_string())]);

        let options = parse_tail_options(params).expect("tail query should parse");
        assert_eq!(
            options,
            TailOptions {
                cursor: 12,
                batch_size: 1,
            }
        );
    }

    #[test]
    fn tail_query_supports_batch_size() {
        let params = HashMap::from([
            ("cursor".to_string(), "7".to_string()),
            ("batch_size".to_string(), "64".to_string()),
        ]);

        let options = parse_tail_options(params).expect("tail query should parse");
        assert_eq!(
            options,
            TailOptions {
                cursor: 7,
                batch_size: 64,
            }
        );
    }

    #[test]
    fn tail_query_rejects_invalid_batch_size() {
        let params = HashMap::from([("batch_size".to_string(), "0".to_string())]);
        assert!(parse_tail_options(params).is_err());
    }

    #[test]
    fn lifecycle_query_requires_tenant_id() {
        assert!(parse_lifecycle_options(HashMap::new()).is_err());
    }

    #[test]
    fn lifecycle_query_parses_tenant_id() {
        let params = HashMap::from([("tenant_id".to_string(), "acme".to_string())]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(
            options,
            LifecycleOptions {
                tenant_id: "acme".to_string(),
                cursor: 0,
                session_id: None,
            }
        );
    }

    #[test]
    fn lifecycle_query_parses_cursor() {
        let params = HashMap::from([
            ("tenant_id".to_string(), "acme".to_string()),
            ("cursor".to_string(), "9".to_string()),
        ]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(options.cursor, 9);
    }

    #[test]
    fn lifecycle_query_parses_session_filter() {
        let params = HashMap::from([
            ("tenant_id".to_string(), "acme".to_string()),
            ("session_id".to_string(), "ses_demo".to_string()),
        ]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(options.session_id.as_deref(), Some("ses_demo"));
    }

    #[test]
    fn resume_invalidated_gap_uses_public_shape() {
        let gap = build_resume_invalidated_gap(10, 2);

        assert_eq!(gap.frame_type, "gap");
        assert_eq!(gap.reason, "resume_invalidated");
        assert_eq!(gap.from_cursor, 10);
        assert_eq!(gap.next_cursor, 2);
        assert_eq!(gap.committed_cursor, 2);
        assert_eq!(gap.earliest_available_cursor, 1);
    }

    #[test]
    fn token_expired_frame_uses_public_shape() {
        let frame = build_token_expired_frame();

        assert_eq!(frame.frame_type, "token_expired");
        assert_eq!(frame.reason, "token_expired");
    }

    #[test]
    fn node_draining_frame_uses_public_shape() {
        let ops_state = OpsState::new(5_000);
        ops_state.begin_shutdown_drain();
        let frame = build_node_draining_frame(&ops_state.snapshot());

        assert_eq!(frame.frame_type, "node_draining");
        assert_eq!(frame.reason, "node_draining");
        assert_eq!(frame.drain_source, Some("shutdown"));
        assert!(frame.retry_after_ms.is_some());
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
    fn archived_filter_matches_existing_api() {
        assert_eq!(
            parse_archived_filter("false").expect("false should parse"),
            ArchivedFilter::Active
        );
        assert_eq!(
            parse_archived_filter("true").expect("true should parse"),
            ArchivedFilter::Archived
        );
        assert_eq!(
            parse_archived_filter("all").expect("all should parse"),
            ArchivedFilter::All
        );
    }
}
