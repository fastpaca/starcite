use std::time::Instant;

use super::edge_routing;
use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
};

use crate::{
    AppState, api, auth, data_plane,
    error::AppError,
    model::ValidatedAppendEvent,
    runtime::RuntimeTouchReason,
    telemetry::{IngestOperation, RequestPhase},
};

pub async fn append_event(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    request: Request<axum::body::Body>,
) -> Result<Response, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let started_at = Instant::now();
    let mut tenant_id = "unknown".to_string();
    let (parts, body) = request.into_parts();
    let headers = parts.headers;
    let body = api::request_validation::read_append_body(&headers, body).await?;
    let authorization = headers.get(axum::http::header::AUTHORIZATION);
    let result: Result<Response, AppError> = async {
        if let Some(proxied) =
            forward_cached_append_request(&state, &session_id, &body, authorization).await?
        {
            return Ok(proxied);
        }

        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let request = api::request_validation::parse_append_request(&body)?;
        tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
        auth::allow_append_session(&auth, &session_id, &tenant_id)?;
        let validated = auth::validate_append_request(request, &auth)?;
        let ack_started_at = Instant::now();
        let response = append_or_forward_request(
            &state,
            &session_id,
            &tenant_id,
            validated,
            &body,
            authorization,
        )
        .await;
        api::request_metrics::record_request_result(
            &state,
            RequestPhase::Ack,
            ack_started_at,
            response.as_ref().map(|_| ()),
        );

        response
    }
    .await;

    api::request_metrics::record_request_result(
        &state,
        RequestPhase::Total,
        started_at,
        result.as_ref().map(|_| ()),
    );
    api::request_metrics::record_ingest_result(
        &state,
        IngestOperation::AppendEvent,
        &tenant_id,
        &result,
    );
    result
}

async fn append_or_forward_request(
    state: &AppState,
    session_id: &str,
    tenant_id: &str,
    validated: ValidatedAppendEvent,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Response, AppError> {
    let mut last_proxy_unavailable = None;

    for attempt in 0..2 {
        match state
            .session_manager
            .append(session_id, tenant_id, validated.clone())
            .await
        {
            Ok(outcome) => {
                touch_existing_session(
                    state,
                    session_id,
                    &outcome.tenant_id,
                    RuntimeTouchReason::HttpWrite,
                )
                .await;

                return Ok((
                    StatusCode::CREATED,
                    Json(api::public_payload::append_reply(&outcome.reply)),
                )
                    .into_response());
            }
            Err(AppError::SessionNotOwned {
                owner_id,
                owner_public_url: Some(owner_public_url),
                ..
            }) => {
                state.session_manager.drop_worker_handle(session_id).await;

                if let Some(proxied) = forward_known_append_request(
                    state,
                    session_id,
                    Some(&owner_id),
                    &owner_public_url,
                    body,
                    authorization,
                )
                .await?
                {
                    return Ok(proxied);
                }

                last_proxy_unavailable = Some(AppError::OwnerProxyUnavailable {
                    owner_url: owner_public_url.clone(),
                });

                if attempt == 0 {
                    continue;
                }

                return Err(last_proxy_unavailable.take().expect("proxy unavailable"));
            }
            Err(error) => return Err(error),
        }
    }

    Err(last_proxy_unavailable.unwrap_or(AppError::Internal))
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

async fn forward_cached_append_request(
    state: &AppState,
    session_id: &str,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response>, AppError> {
    edge_routing::forward_cached_append_path(state, session_id, body, authorization).await
}

async fn forward_known_append_request(
    state: &AppState,
    session_id: &str,
    owner_id: Option<&str>,
    owner_public_url: &str,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response>, AppError> {
    edge_routing::forward_known_append_path(
        state,
        session_id,
        owner_id,
        owner_public_url,
        body,
        authorization,
    )
    .await
}
