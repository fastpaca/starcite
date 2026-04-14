use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    response::IntoResponse,
};

use crate::{
    AppState,
    error::AppError,
    lifecycle_scope::{resolve_lifecycle_options, resolve_session_lifecycle},
    model::{EventsOptions, LifecycleEvent, LifecyclePage},
    query_options::parse_events_options,
    repository,
    request_metrics::{authenticate_http, authenticate_socket},
    request_validation::validate_session_id,
    runtime::RuntimeTouchReason,
    socket_runtime::run_lifecycle_session,
};

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
    headers: axum::http::HeaderMap,
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
    headers: axum::http::HeaderMap,
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

pub(crate) async fn publish_lifecycle(state: &AppState, event: LifecycleEvent) {
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
