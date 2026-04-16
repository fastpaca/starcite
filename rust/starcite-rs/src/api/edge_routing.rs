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
        Some(&owner.owner_id),
        &owner.owner_public_url,
        body,
        authorization,
    )
    .await
}

pub async fn forward_known_append_path(
    state: &AppState,
    session_id: &str,
    owner_id: Option<&str>,
    owner_public_url: &str,
    body: &[u8],
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response<Body>>, AppError> {
    let proxied = state
        .owner_proxy
        .forward_append_raw(owner_public_url, session_id, body, authorization)
        .await;

    match proxied {
        Ok(response) => Ok(Some(response)),
        Err(AppError::OwnerProxyUnavailable { .. }) => {
            state.ownership.forget_remote_owner_hint(session_id).await;
            if let Some(owner_id) = owner_id {
                state
                    .ownership
                    .mark_remote_owner_unavailable(session_id, owner_id)
                    .await;
            }
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
