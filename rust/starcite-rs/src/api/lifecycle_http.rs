use std::{collections::HashMap, time::Duration};

use super::socket_runtime;
use axum::{
    Json,
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    response::IntoResponse,
};
use tokio::sync::broadcast;

use crate::{
    AppState, api, data_plane,
    error::AppError,
    model::{EventsOptions, LifecycleEvent, LifecyclePage, LifecycleResponse},
    runtime::RuntimeTouchReason,
};

pub async fn lifecycle_events(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    let auth = api::request_metrics::authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let lifecycle = api::lifecycle_scope::resolve_lifecycle_options(&state, &auth, &params).await?;
    touch_lifecycle_session(
        &state,
        lifecycle.session_id.as_deref(),
        &lifecycle.tenant_id,
        RuntimeTouchReason::RawLifecycle,
    )
    .await;
    let receiver = state.lifecycle.subscribe_tenant(&lifecycle.tenant_id).await;

    Ok(upgrade_lifecycle_socket(
        websocket, state, lifecycle, receiver, expiry,
    ))
}

pub async fn read_lifecycle(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LifecyclePage>, AppError> {
    let auth = api::request_metrics::authenticate_http(&state, &headers)?;
    let lifecycle = api::lifecycle_scope::resolve_lifecycle_options(&state, &auth, &params).await?;
    let page = data_plane::repository::read_lifecycle_events(
        &state.pool,
        &lifecycle.tenant_id,
        lifecycle.session_id.as_deref(),
        EventsOptions {
            cursor: lifecycle.cursor,
            limit: api::query_options::parse_events_options(params)?.limit,
        },
    )
    .await?;

    touch_lifecycle_session(
        &state,
        lifecycle.session_id.as_deref(),
        &lifecycle.tenant_id,
        RuntimeTouchReason::HttpLifecycle,
    )
    .await;

    Ok(Json(page))
}

pub async fn session_lifecycle_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    api::request_validation::validate_session_id(&session_id)?;

    let auth = api::request_metrics::authenticate_socket(&state, &params)?;
    let expiry = auth.expiry_delay();
    let cursor = api::query_options::parse_events_options(params)?.cursor;
    let lifecycle =
        api::lifecycle_scope::resolve_session_lifecycle(&state, &auth, &session_id, cursor).await?;
    touch_lifecycle_session(
        &state,
        Some(&session_id),
        &lifecycle.tenant_id,
        RuntimeTouchReason::RawLifecycle,
    )
    .await;
    let receiver = state.lifecycle.subscribe_session(&session_id).await;

    Ok(upgrade_lifecycle_socket(
        websocket, state, lifecycle, receiver, expiry,
    ))
}

pub async fn read_session_lifecycle(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<LifecyclePage>, AppError> {
    api::request_validation::validate_session_id(&session_id)?;

    let auth = api::request_metrics::authenticate_http(&state, &headers)?;
    let options = api::query_options::parse_events_options(params)?;
    let lifecycle =
        api::lifecycle_scope::resolve_session_lifecycle(&state, &auth, &session_id, options.cursor)
            .await?;
    let page = data_plane::repository::read_lifecycle_events(
        &state.pool,
        &lifecycle.tenant_id,
        Some(&session_id),
        options,
    )
    .await?;

    touch_lifecycle_session(
        &state,
        Some(&session_id),
        &lifecycle.tenant_id,
        RuntimeTouchReason::HttpLifecycle,
    )
    .await;

    Ok(Json(page))
}

async fn touch_lifecycle_session(
    state: &AppState,
    session_id: Option<&str>,
    tenant_id: &str,
    reason: RuntimeTouchReason,
) {
    let Some(session_id) = session_id else {
        return;
    };

    state
        .runtime
        .touch_existing(session_id, tenant_id, reason)
        .await;
}

fn upgrade_lifecycle_socket(
    websocket: WebSocketUpgrade,
    state: AppState,
    lifecycle: api::query_options::LifecycleOptions,
    receiver: broadcast::Receiver<LifecycleResponse>,
    expiry: Option<Duration>,
) -> impl IntoResponse {
    websocket.on_upgrade(move |socket| async move {
        socket_runtime::run_lifecycle_session(socket, state, lifecycle, receiver, expiry).await;
    })
}

pub(crate) async fn publish_lifecycle(state: &AppState, event: LifecycleEvent) {
    match data_plane::repository::append_lifecycle_event(&state.pool, event, &state.instance_id)
        .await
    {
        Ok(event) => {
            state.session_store.apply_lifecycle_hint(&event.event).await;
            state.lifecycle.broadcast(event).await;
        }
        Err(error) => {
            tracing::error!(error = ?error, "failed to persist lifecycle event");
        }
    }
}
