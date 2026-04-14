use std::collections::HashMap;
use std::time::Instant;

use super::{
    edge_routing::{self, EventPathRequest},
    socket_runtime,
};
use axum::{
    Json,
    extract::{Path, Query, State, ws::WebSocketUpgrade},
    http::{HeaderMap, HeaderValue, Request, StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    AppState, api, auth, cluster, data_plane,
    error::{self, AppError},
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
        if let Some(proxied) = forward_cached_event_request(
            &state,
            &session_id,
            EventPathRequest::Append { body: &body },
            authorization,
        )
        .await?
        {
            return Ok(proxied);
        }

        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let request = api::request_validation::parse_append_request(&body)?;
        tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
        auth::allow_append_session(&auth, &session_id, &tenant_id)?;
        let validated = auth::validate_append_request(request, &auth)?;
        let ack_started_at = Instant::now();
        let outcome = state
            .session_manager
            .append(&session_id, &tenant_id, validated)
            .await;
        let response = match outcome {
            Ok(outcome) => {
                touch_existing_session(
                    &state,
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
                forward_known_event_request(
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

pub async fn read_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Response, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let authorization = headers.get(axum::http::header::AUTHORIZATION);

    if let Some(proxied) = forward_cached_event_request(
        &state,
        &session_id,
        EventPathRequest::ReadEvents { params: &params },
        authorization,
    )
    .await?
    {
        return Ok(proxied);
    }

    let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
    let tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    if let Some(proxied) =
        require_local_read_path_or_forward(&state, &session_id, &params, authorization).await?
    {
        return Ok(proxied);
    }

    let page = data_plane::read_path::read_events(
        &state.hot_store,
        &state.pool,
        &session_id,
        api::query_options::parse_events_options(params)?,
    )
    .await?;

    touch_existing_session(
        &state,
        &session_id,
        &tenant_id,
        RuntimeTouchReason::HttpRead,
    )
    .await;

    Ok(Json(page).into_response())
}

pub async fn tail_events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<Response, AppError> {
    api::request_validation::validate_session_id(&session_id)?;

    let auth = api::request_metrics::authenticate_socket(&state, &params).await?;
    let expiry = auth.expiry_delay();
    let tail = api::query_options::parse_tail_options(params.clone())?;
    let tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    match socket_runtime::require_local_owner_for_event_path(&state, &session_id).await {
        Ok(()) => {}
        Err(AppError::SessionNotOwned {
            owner_id,
            owner_public_url: Some(owner_public_url),
            epoch,
        }) => {
            if let Some(owner_ws_url) =
                cluster::owner_proxy::build_tail_ws_url(&owner_public_url, &session_id, &params)
            {
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
    touch_existing_session(&state, &session_id, &tenant_id, RuntimeTouchReason::RawTail).await;
    let receiver = state.fanout.subscribe(&session_id).await;

    Ok(websocket
        .on_upgrade(move |socket| async move {
            socket_runtime::run_tail_session(socket, state, session_id, tail, receiver, expiry)
                .await;
        })
        .into_response())
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

async fn forward_cached_event_request(
    state: &AppState,
    session_id: &str,
    request: EventPathRequest<'_>,
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response>, AppError> {
    edge_routing::forward_cached_event_path(state, session_id, request, authorization).await
}

async fn forward_known_event_request(
    state: &AppState,
    session_id: &str,
    owner_public_url: &str,
    request: EventPathRequest<'_>,
    authorization: Option<&HeaderValue>,
) -> Result<Response, AppError> {
    edge_routing::forward_known_event_path(
        state,
        session_id,
        owner_public_url,
        request,
        authorization,
    )
    .await
}

async fn require_local_read_path_or_forward(
    state: &AppState,
    session_id: &str,
    params: &HashMap<String, String>,
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response>, AppError> {
    match socket_runtime::require_local_owner_for_event_path(state, session_id).await {
        Ok(()) => Ok(None),
        Err(AppError::SessionNotOwned {
            owner_public_url: Some(owner_public_url),
            ..
        }) => forward_known_event_request(
            state,
            session_id,
            &owner_public_url,
            EventPathRequest::ReadEvents { params },
            authorization,
        )
        .await
        .map(Some),
        Err(error) => Err(error),
    }
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

#[cfg(test)]
mod tests {
    use axum::{
        body,
        http::{StatusCode, header},
    };

    use super::tail_owner_redirect_response;
    use crate::error::{OWNER_URL_HEADER, OWNER_WEBSOCKET_URL_HEADER};

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
}
