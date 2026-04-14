use std::collections::HashMap;

use serde_json::{Value, json};

use crate::{
    auth::{self, AuthContext},
    cluster::owner_proxy::build_phoenix_socket_ws_url,
    config::AuthMode,
    error::AppError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SocketContext {
    pub(crate) auth: AuthContext,
    pub(crate) tenant_id: Option<String>,
    pub(crate) connect_params: HashMap<String, String>,
}

pub(crate) fn resolve_lifecycle_tenant_id(
    context: &SocketContext,
    payload: &Value,
) -> Result<String, AppError> {
    if context.auth.kind == AuthMode::UnsafeJwt {
        auth::can_subscribe_lifecycle(&context.auth)?;
        return Ok(context.auth.principal.tenant_id.clone());
    }

    if let Some(tenant_id) = context.tenant_id.as_ref() {
        return Ok(tenant_id.clone());
    }

    match payload.get("tenant_id").and_then(Value::as_str) {
        Some(tenant_id) if !tenant_id.is_empty() => Ok(tenant_id.to_string()),
        _ => Err(AppError::InvalidTenantId),
    }
}

pub(crate) fn tail_join_error_payload(error: &AppError, context: &SocketContext) -> Value {
    match error {
        AppError::SessionNotOwned {
            owner_id,
            owner_public_url,
            epoch,
        } => json!({
            "reason": error_reason(error),
            "owner_id": owner_id,
            "owner_url": owner_public_url,
            "owner_socket_url": owner_public_url
                .as_deref()
                .and_then(|owner_url| build_phoenix_socket_ws_url(owner_url, &context.connect_params)),
            "epoch": epoch
        }),
        _ => json!({"reason": error_reason(error)}),
    }
}

pub(crate) fn error_reason(error: &AppError) -> &'static str {
    match error {
        AppError::MissingBearerToken => "missing_bearer_token",
        AppError::InvalidBearerToken => "invalid_bearer_token",
        AppError::TokenExpired => "token_expired",
        AppError::Forbidden => "forbidden",
        AppError::ForbiddenScope => "forbidden_scope",
        AppError::ForbiddenSession => "forbidden_session",
        AppError::ForbiddenTenant => "forbidden_tenant",
        AppError::InvalidSessionId => "invalid_session_id",
        AppError::InvalidTenantId => "invalid_tenant_id",
        AppError::InvalidCursor => "invalid_cursor",
        AppError::InvalidTailBatchSize => "invalid_tail_batch_size",
        AppError::SessionNotFound => "session_not_found",
        AppError::SessionNotOwned { .. } => "session_not_owned",
        _ => "internal_error",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{SocketContext, resolve_lifecycle_tenant_id, tail_join_error_payload};
    use crate::{auth::AuthContext, config::AuthMode, error::AppError};

    #[test]
    fn tail_join_error_payload_includes_owner_socket_url() {
        let payload = tail_join_error_payload(
            &AppError::SessionNotOwned {
                owner_id: "node-a".to_string(),
                owner_public_url: Some("https://owner.example:4443".to_string()),
                epoch: 9,
            },
            &SocketContext {
                auth: AuthContext::none(),
                tenant_id: Some("acme".to_string()),
                connect_params: HashMap::from([
                    ("token".to_string(), "jwt-token".to_string()),
                    ("vsn".to_string(), "2.0.0".to_string()),
                ]),
            },
        );

        assert_eq!(payload["reason"], "session_not_owned");
        assert_eq!(payload["owner_url"], "https://owner.example:4443");
        assert_eq!(
            payload["owner_socket_url"],
            "wss://owner.example:4443/v1/socket/websocket?token=jwt-token&vsn=2.0.0"
        );
        assert_eq!(payload["epoch"], 9);
    }

    #[test]
    fn lifecycle_tenant_id_uses_socket_context_then_join_payload() {
        let context = SocketContext {
            auth: AuthContext::none(),
            tenant_id: Some("acme".to_string()),
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "other"}))
                .expect("context tenant id"),
            "acme"
        );

        let context = SocketContext {
            auth: AuthContext::none(),
            tenant_id: None,
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "acme"}))
                .expect("join tenant id"),
            "acme"
        );
    }

    #[test]
    fn lifecycle_tenant_id_uses_authenticated_service_tenant_in_unsafe_mode() {
        let context = SocketContext {
            auth: AuthContext {
                kind: AuthMode::UnsafeJwt,
                principal: crate::model::Principal {
                    tenant_id: "acme".to_string(),
                    id: "svc".to_string(),
                    principal_type: "service".to_string(),
                },
                scopes: vec!["session:read".to_string()],
                session_id: None,
                expires_at: Some(4_102_444_800_i64),
            },
            tenant_id: Some("other".to_string()),
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "other"}))
                .expect("auth tenant id"),
            "acme"
        );
    }
}
