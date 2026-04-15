use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::{
    AppState, api, auth, data_plane,
    data_plane::repository,
    error::AppError,
    model::{CreateSessionRequest, LifecycleEvent, SessionResponse, UpdateSessionRequest},
    runtime::RuntimeTouchReason,
    telemetry::IngestOperation,
};

pub async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let mut tenant_id = "unknown".to_string();
    let result: Result<(StatusCode, Json<crate::model::SessionResponse>), AppError> = async {
        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        let validated = auth::validate_create_request(request, &auth)?;
        tenant_id = validated.tenant_id.clone();
        let session = repository::create_session(&state.pool, validated.clone()).await?;
        cache_session(&state, &validated.tenant_id, &session, Some(0)).await;

        state
            .runtime
            .session_created(&session.id, &validated.tenant_id)
            .await;
        crate::runtime::publish_catalog_lifecycle(
            &state.pool,
            &state.session_store,
            &state.lifecycle,
            &state.instance_id,
            LifecycleEvent::created(validated.tenant_id, &session),
        )
        .await;

        Ok((StatusCode::CREATED, Json(session)))
    }
    .await;

    api::request_metrics::record_ingest_result(
        &state,
        IngestOperation::CreateSession,
        &tenant_id,
        &result,
    );
    result
}

pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<crate::model::SessionsPage>, AppError> {
    let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
    let options = auth::apply_list_scope(&auth, api::query_options::parse_list_options(params)?)?;
    let page = repository::list_sessions(&state.pool, options).await?;
    Ok(Json(page))
}

pub async fn show_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
    let tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    let session =
        data_plane::session_store::resolve_session(&state.session_store, &state.pool, &session_id)
            .await?;

    touch_existing_session(
        &state,
        &session_id,
        &tenant_id,
        RuntimeTouchReason::HttpRead,
    )
    .await;

    Ok(Json(session))
}

pub async fn update_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Result<Json<UpdateSessionRequest>, JsonRejection>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let mut tenant_id = "unknown".to_string();
    let result: Result<Json<crate::model::SessionResponse>, AppError> = async {
        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
        auth::allow_manage_session(&auth, &session_id, &tenant_id)?;
        touch_existing_session(
            &state,
            &session_id,
            &tenant_id,
            RuntimeTouchReason::HttpWrite,
        )
        .await;

        let session =
            repository::update_session(&state.pool, &session_id, request.validate()?).await?;
        cache_session(&state, &tenant_id, &session, None).await;

        crate::runtime::publish_catalog_lifecycle(
            &state.pool,
            &state.session_store,
            &state.lifecycle,
            &state.instance_id,
            LifecycleEvent::updated(tenant_id.clone(), &session),
        )
        .await;

        Ok(Json(session))
    }
    .await;

    api::request_metrics::record_ingest_result(
        &state,
        IngestOperation::UpdateSession,
        &tenant_id,
        &result,
    );
    result
}

pub async fn archive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    change_archive_state(&state, &headers, &session_id, true).await
}

pub async fn unarchive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    change_archive_state(&state, &headers, &session_id, false).await
}

async fn resolve_session_tenant_id(state: &AppState, session_id: &str) -> Result<String, AppError> {
    data_plane::session_store::resolve_session_tenant_id(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await
}

async fn touch_existing_session(
    state: &AppState,
    session_id: &str,
    tenant_id: &str,
    reason: RuntimeTouchReason,
) {
    state
        .runtime
        .touch_existing(session_id, tenant_id, reason)
        .await;
}

async fn cache_session(
    state: &AppState,
    tenant_id: &str,
    session: &SessionResponse,
    archived_seq: Option<i64>,
) {
    state
        .session_store
        .put_session(tenant_id, session.clone(), archived_seq)
        .await;
}

async fn change_archive_state(
    state: &AppState,
    headers: &HeaderMap,
    session_id: &str,
    archived: bool,
) -> Result<Json<SessionResponse>, AppError> {
    api::request_validation::validate_session_id(session_id)?;

    let auth = api::request_metrics::authenticate_http(state, headers).await?;
    let tenant_id = resolve_session_tenant_id(state, session_id).await?;
    auth::allow_manage_session(&auth, session_id, &tenant_id)?;
    touch_existing_session(state, session_id, &tenant_id, RuntimeTouchReason::HttpWrite).await;

    let outcome = repository::set_archive_state(&state.pool, session_id, archived).await?;
    cache_session(state, &outcome.tenant_id, &outcome.session, None).await;
    publish_archive_lifecycle(state, archived, &outcome).await;

    Ok(Json(outcome.session))
}

async fn publish_archive_lifecycle(
    state: &AppState,
    archived: bool,
    outcome: &repository::ArchiveStateOutcome,
) {
    if !outcome.changed {
        return;
    }

    let event = if archived {
        LifecycleEvent::archived(outcome.tenant_id.clone(), &outcome.session)
    } else {
        LifecycleEvent::unarchived(outcome.tenant_id.clone(), &outcome.session)
    };

    crate::runtime::publish_catalog_lifecycle(
        &state.pool,
        &state.session_store,
        &state.lifecycle,
        &state.instance_id,
        event,
    )
    .await;
}
