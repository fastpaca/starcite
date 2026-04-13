use axum::{
    Json,
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{self, HeaderName},
    },
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use thiserror::Error;

use crate::ops::OpsSnapshot;

pub const DRAIN_SOURCE_HEADER: HeaderName = HeaderName::from_static("x-starcite-drain-source");
pub const RETRY_AFTER_MS_HEADER: HeaderName = HeaderName::from_static("x-starcite-retry-after-ms");
pub const OWNER_URL_HEADER: HeaderName = HeaderName::from_static("x-starcite-owner-url");

#[derive(Debug, Error)]
pub enum AppError {
    #[error("missing bearer token")]
    MissingBearerToken,
    #[error("invalid bearer token")]
    InvalidBearerToken,
    #[error("token expired")]
    TokenExpired,
    #[error("forbidden")]
    Forbidden,
    #[error("forbidden scope")]
    ForbiddenScope,
    #[error("forbidden session")]
    ForbiddenSession,
    #[error("forbidden tenant")]
    ForbiddenTenant,
    #[error("invalid session payload")]
    InvalidSession,
    #[error("invalid session id")]
    InvalidSessionId,
    #[error("invalid event payload")]
    InvalidEvent,
    #[error("invalid metadata payload")]
    InvalidMetadata,
    #[error("invalid refs payload")]
    InvalidRefs,
    #[error("invalid list query")]
    InvalidListQuery,
    #[error("invalid tenant id")]
    InvalidTenantId,
    #[error("invalid limit value")]
    InvalidLimit,
    #[error("invalid tail batch size value")]
    InvalidTailBatchSize,
    #[error("invalid cursor value")]
    InvalidCursor,
    #[error("session was not found")]
    SessionNotFound,
    #[error("session already exists")]
    SessionExists,
    #[error("expected seq conflict")]
    ExpectedSeqConflict { expected: i64, current: i64 },
    #[error("expected version conflict")]
    ExpectedVersionConflict { expected: i64, current: i64 },
    #[error("producer replay conflict")]
    ProducerReplayConflict,
    #[error("producer sequence conflict")]
    ProducerSeqConflict {
        producer_id: String,
        expected: i64,
        current: i64,
    },
    #[error("database unavailable")]
    DatabaseUnavailable,
    #[error("session is not owned by this node")]
    SessionNotOwned {
        owner_id: String,
        owner_public_url: Option<String>,
        epoch: i64,
    },
    #[error("append quorum unavailable")]
    QuorumUnavailable { required: u32, acknowledged: u32 },
    #[error("node draining")]
    NodeDraining {
        drain_source: Option<&'static str>,
        retry_after_ms: Option<u64>,
    },
    #[error("manual drain cannot clear shutdown drain")]
    DrainResetForbidden,
    #[error("internal error")]
    Internal,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

impl AppError {
    pub fn node_draining(ops: &OpsSnapshot) -> Self {
        Self::NodeDraining {
            drain_source: ops.drain_source,
            retry_after_ms: ops.retry_after_ms,
        }
    }

    pub fn error_code(&self) -> &'static str {
        match self {
            Self::MissingBearerToken => "missing_bearer_token",
            Self::InvalidBearerToken => "invalid_bearer_token",
            Self::TokenExpired => "token_expired",
            Self::Forbidden => "forbidden",
            Self::ForbiddenScope => "forbidden_scope",
            Self::ForbiddenSession => "forbidden_session",
            Self::ForbiddenTenant => "forbidden_tenant",
            Self::InvalidSession => "invalid_session",
            Self::InvalidSessionId => "invalid_session_id",
            Self::InvalidEvent => "invalid_event",
            Self::InvalidMetadata => "invalid_metadata",
            Self::InvalidRefs => "invalid_refs",
            Self::InvalidListQuery => "invalid_list_query",
            Self::InvalidTenantId => "invalid_tenant_id",
            Self::InvalidLimit => "invalid_limit",
            Self::InvalidTailBatchSize => "invalid_tail_batch_size",
            Self::InvalidCursor => "invalid_cursor",
            Self::SessionNotFound => "session_not_found",
            Self::SessionExists => "session_exists",
            Self::ExpectedSeqConflict { .. } => "expected_seq_conflict",
            Self::ExpectedVersionConflict { .. } => "expected_version_conflict",
            Self::ProducerReplayConflict => "producer_replay_conflict",
            Self::ProducerSeqConflict { .. } => "producer_seq_conflict",
            Self::DatabaseUnavailable => "database_unavailable",
            Self::SessionNotOwned { .. } => "session_not_owned",
            Self::QuorumUnavailable { .. } => "quorum_unavailable",
            Self::NodeDraining { .. } => "node_draining",
            Self::DrainResetForbidden => "drain_reset_forbidden",
            Self::Internal | Self::Sqlx(_) => "internal_error",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingBearerToken | Self::InvalidBearerToken | Self::TokenExpired => {
                StatusCode::UNAUTHORIZED
            }
            Self::Forbidden
            | Self::ForbiddenScope
            | Self::ForbiddenSession
            | Self::ForbiddenTenant => StatusCode::FORBIDDEN,
            Self::InvalidSession
            | Self::InvalidSessionId
            | Self::InvalidEvent
            | Self::InvalidMetadata
            | Self::InvalidRefs
            | Self::InvalidListQuery
            | Self::InvalidTenantId
            | Self::InvalidLimit
            | Self::InvalidTailBatchSize
            | Self::InvalidCursor => StatusCode::BAD_REQUEST,
            Self::SessionNotFound => StatusCode::NOT_FOUND,
            Self::SessionExists
            | Self::ExpectedSeqConflict { .. }
            | Self::ExpectedVersionConflict { .. }
            | Self::ProducerReplayConflict
            | Self::ProducerSeqConflict { .. }
            | Self::SessionNotOwned { .. }
            | Self::DrainResetForbidden => StatusCode::CONFLICT,
            Self::DatabaseUnavailable
            | Self::QuorumUnavailable { .. }
            | Self::NodeDraining { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal | Self::Sqlx(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn body(&self) -> Value {
        match self {
            Self::MissingBearerToken => json!({
                "error": self.error_code(),
                "message": "Missing bearer token"
            }),
            Self::InvalidBearerToken => json!({
                "error": self.error_code(),
                "message": "Invalid bearer token"
            }),
            Self::TokenExpired => json!({
                "error": self.error_code(),
                "message": "Token expired"
            }),
            Self::Forbidden => json!({
                "error": self.error_code(),
                "message": "Forbidden"
            }),
            Self::ForbiddenScope => json!({
                "error": self.error_code(),
                "message": "Forbidden by scope policy"
            }),
            Self::ForbiddenSession => json!({
                "error": self.error_code(),
                "message": "Forbidden by session policy"
            }),
            Self::ForbiddenTenant => json!({
                "error": self.error_code(),
                "message": "Forbidden by tenant policy"
            }),
            Self::InvalidSession => json!({
                "error": self.error_code(),
                "message": "Invalid session payload"
            }),
            Self::InvalidSessionId => json!({
                "error": self.error_code(),
                "message": "Invalid session id"
            }),
            Self::InvalidEvent => json!({
                "error": self.error_code(),
                "message": "Invalid event payload"
            }),
            Self::InvalidMetadata => json!({
                "error": self.error_code(),
                "message": "Invalid metadata payload"
            }),
            Self::InvalidRefs => json!({
                "error": self.error_code(),
                "message": "Invalid refs payload"
            }),
            Self::InvalidListQuery => json!({
                "error": self.error_code(),
                "message": "Invalid list query"
            }),
            Self::InvalidTenantId => json!({
                "error": self.error_code(),
                "message": "Invalid tenant id"
            }),
            Self::InvalidLimit => json!({
                "error": self.error_code(),
                "message": "Invalid limit value"
            }),
            Self::InvalidTailBatchSize => json!({
                "error": self.error_code(),
                "message": "Invalid tail batch size value"
            }),
            Self::InvalidCursor => json!({
                "error": self.error_code(),
                "message": "Invalid cursor value"
            }),
            Self::SessionNotFound => json!({
                "error": self.error_code(),
                "message": "Session was not found"
            }),
            Self::SessionExists => json!({
                "error": self.error_code(),
                "message": "Session already exists"
            }),
            Self::ExpectedSeqConflict { expected, current } => json!({
                "error": self.error_code(),
                "message": format!("Expected seq {expected}, current seq is {current}")
            }),
            Self::ExpectedVersionConflict { expected, current } => json!({
                "error": self.error_code(),
                "message": format!("Expected version {expected}, current version is {current}"),
                "current_version": current
            }),
            Self::DrainResetForbidden => json!({
                "error": self.error_code(),
                "message": "Only manual drain can be cleared"
            }),
            Self::ProducerReplayConflict => json!({
                "error": self.error_code(),
                "message": "Producer sequence was already used with different event content"
            }),
            Self::ProducerSeqConflict {
                producer_id,
                expected,
                current,
            } => json!({
                "error": self.error_code(),
                "message": format!("Producer {producer_id} expected seq {expected}, got {current}")
            }),
            Self::DatabaseUnavailable => json!({
                "error": self.error_code(),
                "message": "Database is unavailable"
            }),
            Self::SessionNotOwned {
                owner_id,
                owner_public_url,
                epoch,
            } => json!({
                "error": self.error_code(),
                "message": "Session is not owned by this node",
                "owner_id": owner_id,
                "owner_url": owner_public_url,
                "epoch": epoch
            }),
            Self::QuorumUnavailable {
                required,
                acknowledged,
            } => json!({
                "error": self.error_code(),
                "message": "Append quorum is unavailable",
                "required": required,
                "acknowledged": acknowledged
            }),
            Self::NodeDraining {
                drain_source,
                retry_after_ms,
            } => json!({
                "error": self.error_code(),
                "message": "Node is draining",
                "drain_source": drain_source,
                "retry_after_ms": retry_after_ms
            }),
            Self::Internal | Self::Sqlx(_) => json!({
                "error": self.error_code(),
                "message": "Internal server error"
            }),
        }
    }
}

pub fn apply_drain_headers(
    headers: &mut HeaderMap,
    drain_source: Option<&'static str>,
    retry_after_ms: Option<u64>,
) {
    if let Some(drain_source) = drain_source {
        headers.insert(DRAIN_SOURCE_HEADER, HeaderValue::from_static(drain_source));
    }

    if let Some(retry_after_ms) = retry_after_ms {
        if let Ok(value) = HeaderValue::from_str(&retry_after_ms.to_string()) {
            headers.insert(RETRY_AFTER_MS_HEADER.clone(), value);
        }

        let retry_after_seconds = retry_after_ms.saturating_add(999) / 1_000;

        if let Ok(value) = HeaderValue::from_str(&retry_after_seconds.to_string()) {
            headers.insert(header::RETRY_AFTER, value);
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let Self::Sqlx(error) = &self {
            tracing::error!(error = ?error, "database request failed");
        }

        let mut response = (self.status_code(), Json(self.body())).into_response();

        match &self {
            Self::NodeDraining {
                drain_source,
                retry_after_ms,
            } => {
                apply_drain_headers(response.headers_mut(), *drain_source, *retry_after_ms);
            }
            Self::SessionNotOwned {
                owner_public_url: Some(owner_public_url),
                ..
            } => {
                if let Ok(value) = HeaderValue::from_str(owner_public_url) {
                    response.headers_mut().insert(OWNER_URL_HEADER, value);
                }
            }
            _ => {}
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use axum::{body, http::StatusCode, response::IntoResponse};
    use serde_json::json;

    use super::{AppError, DRAIN_SOURCE_HEADER, OWNER_URL_HEADER, RETRY_AFTER_MS_HEADER};
    use crate::ops::OpsSnapshot;

    #[tokio::test]
    async fn node_draining_response_includes_retry_headers() {
        let response = AppError::node_draining(&OpsSnapshot {
            mode: "draining",
            draining: true,
            drain_source: Some("shutdown"),
            retry_after_ms: Some(3_450),
            shutdown_drain_timeout_ms: 5_000,
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(&DRAIN_SOURCE_HEADER).unwrap(),
            "shutdown"
        );
        assert_eq!(
            response.headers().get(&RETRY_AFTER_MS_HEADER).unwrap(),
            "3450"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .unwrap(),
            "4"
        );

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let json_body: serde_json::Value = serde_json::from_slice(&body).expect("response json");

        assert_eq!(
            json_body,
            json!({
                "error": "node_draining",
                "message": "Node is draining",
                "drain_source": "shutdown",
                "retry_after_ms": 3450
            })
        );
    }

    #[tokio::test]
    async fn manual_node_draining_response_omits_retry_headers() {
        let response = AppError::node_draining(&OpsSnapshot {
            mode: "draining",
            draining: true,
            drain_source: Some("manual"),
            retry_after_ms: None,
            shutdown_drain_timeout_ms: 5_000,
        })
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(&DRAIN_SOURCE_HEADER).unwrap(),
            "manual"
        );
        assert!(response.headers().get(&RETRY_AFTER_MS_HEADER).is_none());
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }

    #[tokio::test]
    async fn quorum_unavailable_response_is_explicit() {
        let response = AppError::QuorumUnavailable {
            required: 2,
            acknowledged: 1,
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let json_body: serde_json::Value = serde_json::from_slice(&body).expect("response json");

        assert_eq!(
            json_body,
            json!({
                "error": "quorum_unavailable",
                "message": "Append quorum is unavailable",
                "required": 2,
                "acknowledged": 1
            })
        );
    }

    #[tokio::test]
    async fn session_not_owned_response_includes_owner_hint() {
        let response = AppError::SessionNotOwned {
            owner_id: "node-a".to_string(),
            owner_public_url: Some("http://127.0.0.1:4191".to_string()),
            epoch: 7,
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response.headers().get(&OWNER_URL_HEADER).unwrap(),
            "http://127.0.0.1:4191"
        );

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body bytes");
        let json_body: serde_json::Value = serde_json::from_slice(&body).expect("response json");

        assert_eq!(
            json_body,
            json!({
                "error": "session_not_owned",
                "message": "Session is not owned by this node",
                "owner_id": "node-a",
                "owner_url": "http://127.0.0.1:4191",
                "epoch": 7
            })
        );
    }
}
