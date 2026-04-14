use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::{
    AppState, auth,
    error::AppError,
    lifecycle_http::publish_lifecycle,
    model::{CreateSessionRequest, LifecycleEvent, UpdateSessionRequest},
    query_options::parse_list_options,
    repository,
    request_metrics::{authenticate_http, record_ingest_result},
    request_validation::validate_session_id,
    runtime::RuntimeTouchReason,
    session_store::{resolve_session, resolve_session_tenant_id},
    telemetry::IngestOperation,
};

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
