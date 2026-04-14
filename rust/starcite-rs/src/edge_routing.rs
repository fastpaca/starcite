use std::collections::HashMap;

use axum::{
    body::Body,
    http::{HeaderValue, Response},
};

use crate::{AppState, error::AppError};

pub enum EventPathRequest<'a> {
    Append { body: &'a [u8] },
    ReadEvents { params: &'a HashMap<String, String> },
}

impl EventPathRequest<'_> {
    async fn forward(
        &self,
        state: &AppState,
        session_id: &str,
        owner_public_url: &str,
        authorization: Option<&HeaderValue>,
    ) -> Result<Response<Body>, AppError> {
        match self {
            Self::Append { body } => {
                state
                    .owner_proxy
                    .forward_append_raw(owner_public_url, session_id, body, authorization)
                    .await
            }
            Self::ReadEvents { params } => {
                state
                    .owner_proxy
                    .forward_read_events(owner_public_url, session_id, params, authorization)
                    .await
            }
        }
    }
}

pub async fn forward_cached_event_path(
    state: &AppState,
    session_id: &str,
    request: EventPathRequest<'_>,
    authorization: Option<&HeaderValue>,
) -> Result<Option<Response<Body>>, AppError> {
    let Some(owner) = state.ownership.cached_remote_owner(session_id).await else {
        return Ok(None);
    };

    forward_known_event_path(
        state,
        session_id,
        &owner.owner_public_url,
        request,
        authorization,
    )
    .await
    .map(Some)
}

pub async fn forward_known_event_path(
    state: &AppState,
    session_id: &str,
    owner_public_url: &str,
    request: EventPathRequest<'_>,
    authorization: Option<&HeaderValue>,
) -> Result<Response<Body>, AppError> {
    let proxied = request
        .forward(state, session_id, owner_public_url, authorization)
        .await;

    if matches!(proxied, Err(AppError::OwnerProxyUnavailable { .. })) {
        state.ownership.forget_remote_owner_hint(session_id).await;
    }

    proxied
}
