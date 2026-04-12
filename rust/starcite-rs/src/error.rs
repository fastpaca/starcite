use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use thiserror::Error;

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
    #[error("internal error")]
    Internal,
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

impl AppError {
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
            | Self::ProducerSeqConflict { .. } => StatusCode::CONFLICT,
            Self::DatabaseUnavailable => StatusCode::SERVICE_UNAVAILABLE,
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
            Self::Internal | Self::Sqlx(_) => json!({
                "error": self.error_code(),
                "message": "Internal server error"
            }),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let Self::Sqlx(error) = &self {
            tracing::error!(error = ?error, "database request failed");
        }

        (self.status_code(), Json(self.body())).into_response()
    }
}
