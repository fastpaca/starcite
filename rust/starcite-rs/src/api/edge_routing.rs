use axum::{
    body::Body,
    http::{HeaderValue, Response},
};

use crate::{AppState, error::AppError};

pub async fn forward_cached_append_path(
    state: &AppState,
    session_id: &str,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response<Body>>, AppError> {
    let Some(owner) = state.ownership.cached_remote_owner(session_id).await else {
        return Ok(None);
    };

    forward_known_append_path(
        state,
        session_id,
        &owner.owner_public_url,
        body,
        authorization,
    )
    .await
    .map(Some)
}

pub async fn forward_known_append_path(
    state: &AppState,
    session_id: &str,
    owner_public_url: &str,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Response<Body>, AppError> {
    let proxied = state
        .owner_proxy
        .forward_append_raw(owner_public_url, session_id, body, authorization)
        .await;

    if matches!(proxied, Err(AppError::OwnerProxyUnavailable { .. })) {
        state.ownership.forget_remote_owner_hint(session_id).await;
    }

    proxied
}
