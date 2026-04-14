use std::{collections::HashMap, time::Duration};

use axum::http::HeaderMap;
use chrono::Utc;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    config::AuthMode,
    error::AppError,
    model::{
        AppendEventRequest, CreateSessionRequest, JsonMap, ListOptions, Principal,
        ValidatedAppendEvent, ValidatedCreateSession, normalize_principal_type, optional_object,
        require_non_empty, require_object,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub kind: AuthMode,
    pub principal: Principal,
    pub scopes: Vec<String>,
    pub session_id: Option<String>,
    pub expires_at: Option<i64>,
}

impl AuthContext {
    pub fn none() -> Self {
        Self {
            kind: AuthMode::None,
            principal: Principal::service("service".to_string()),
            scopes: Vec::new(),
            session_id: None,
            expires_at: None,
        }
    }

    pub fn actor(&self) -> String {
        self.principal.actor()
    }

    pub fn expiry_delay(&self) -> Option<Duration> {
        let expires_at = self.expires_at?;
        let now = Utc::now().timestamp();

        if expires_at <= now {
            Some(Duration::from_secs(0))
        } else {
            Some(Duration::from_secs((expires_at - now) as u64))
        }
    }
}

pub fn authenticate_http(
    headers: &HeaderMap,
    auth_mode: AuthMode,
) -> Result<AuthContext, AppError> {
    match auth_mode {
        AuthMode::None => Ok(AuthContext::none()),
        AuthMode::UnsafeJwt => {
            let value = headers
                .get(axum::http::header::AUTHORIZATION)
                .ok_or(AppError::MissingBearerToken)?;
            let raw = value.to_str().map_err(|_| AppError::InvalidBearerToken)?;
            let token = extract_bearer_token(raw)?;
            authenticate_unsafe_jwt(token)
        }
    }
}

pub fn authenticate_socket(
    params: &HashMap<String, String>,
    auth_mode: AuthMode,
) -> Result<AuthContext, AppError> {
    if params.contains_key("access_token") {
        return Err(AppError::InvalidBearerToken);
    }

    match auth_mode {
        AuthMode::None => Ok(AuthContext::none()),
        AuthMode::UnsafeJwt => {
            let token = params
                .get("token")
                .filter(|token| !token.is_empty())
                .ok_or(AppError::MissingBearerToken)?;
            authenticate_unsafe_jwt(token)
        }
    }
}

pub fn can_create_session(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => has_scope(auth, "session:create"),
    }
}

pub fn can_read_session(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => has_scope(auth, "session:read"),
    }
}

pub fn can_subscribe_lifecycle(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => {
            has_scope(auth, "session:read")?;

            if auth.session_id.is_some() {
                return Err(AppError::ForbiddenSession);
            }

            if auth.principal.principal_type == "service" {
                Ok(())
            } else {
                Err(AppError::Forbidden)
            }
        }
    }
}

pub fn allowed_to_access_session(auth: &AuthContext, session_id: &str) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => match auth.session_id.as_deref() {
            None => Ok(()),
            Some(locked_session_id) if locked_session_id == session_id => Ok(()),
            Some(_) => Err(AppError::ForbiddenSession),
        },
    }
}

pub fn allow_read_session(
    auth: &AuthContext,
    session_id: &str,
    tenant_id: &str,
) -> Result<(), AppError> {
    session_permission(auth, session_id, tenant_id, "session:read")
}

pub fn allow_append_session(
    auth: &AuthContext,
    session_id: &str,
    tenant_id: &str,
) -> Result<(), AppError> {
    session_permission(auth, session_id, tenant_id, "session:append")
}

pub fn allow_manage_session(
    auth: &AuthContext,
    session_id: &str,
    tenant_id: &str,
) -> Result<(), AppError> {
    session_permission(auth, session_id, tenant_id, "session:create")
}

pub fn apply_list_scope(
    auth: &AuthContext,
    mut options: ListOptions,
) -> Result<ListOptions, AppError> {
    match auth.kind {
        AuthMode::None => Ok(options),
        AuthMode::UnsafeJwt => {
            can_read_session(auth)?;

            if let Some(requested_tenant) = options.tenant_id.as_ref()
                && requested_tenant != &auth.principal.tenant_id
            {
                return Err(AppError::ForbiddenTenant);
            }

            if let Some(requested_session_id) = options.session_id.as_ref() {
                allowed_to_access_session(auth, requested_session_id)?;
            }

            options.tenant_id = Some(auth.principal.tenant_id.clone());

            if let Some(session_id) = auth.session_id.as_ref() {
                options.session_id = Some(session_id.clone());
            }

            Ok(options)
        }
    }
}

pub fn validate_create_request(
    request: CreateSessionRequest,
    auth: &AuthContext,
) -> Result<ValidatedCreateSession, AppError> {
    if auth.kind == AuthMode::None {
        return request.validate();
    }

    can_create_session(auth)?;

    let metadata = optional_object(request.metadata, AppError::InvalidMetadata, true)?;
    let requested_tenant = request
        .tenant_id
        .map(|tenant_id| require_non_empty(tenant_id, AppError::InvalidSession))
        .transpose()?;

    if let Some(tenant_id) = requested_tenant.as_ref()
        && tenant_id != &auth.principal.tenant_id
    {
        return Err(AppError::ForbiddenTenant);
    }

    if let Some(creator_principal) = request.creator_principal
        && creator_principal.normalized()? != auth.principal
    {
        return Err(AppError::InvalidSession);
    }

    let id = match resolve_create_session_id(auth, request.id)? {
        Some(id) => require_non_empty(id, AppError::InvalidSession)?,
        None => format!("ses_{}", Uuid::now_v7().simple()),
    };

    Ok(ValidatedCreateSession {
        id,
        title: request.title,
        metadata,
        tenant_id: auth.principal.tenant_id.clone(),
        creator_principal: auth.principal.clone(),
    })
}

pub fn validate_append_request(
    request: AppendEventRequest,
    auth: &AuthContext,
) -> Result<ValidatedAppendEvent, AppError> {
    if auth.kind == AuthMode::None {
        return request.validate();
    }

    let event_type = require_non_empty(request.event_type, AppError::InvalidEvent)?;
    let payload = require_object(request.payload, AppError::InvalidEvent)?;
    let actor = resolve_event_actor(auth, request.actor)?;
    let source = request
        .source
        .map(|value| require_non_empty(value, AppError::InvalidEvent))
        .transpose()?;
    let metadata = attach_principal_metadata(
        auth,
        optional_object(request.metadata, AppError::InvalidMetadata, true)?,
    );
    let refs = optional_object(request.refs, AppError::InvalidRefs, true)?;
    let idempotency_key = request
        .idempotency_key
        .map(|value| require_non_empty(value, AppError::InvalidEvent))
        .transpose()?;
    let producer_id = require_non_empty(request.producer_id, AppError::InvalidEvent)?;

    if request.producer_seq <= 0 {
        return Err(AppError::InvalidEvent);
    }

    if let Some(expected_seq) = request.expected_seq
        && expected_seq < 0
    {
        return Err(AppError::InvalidEvent);
    }

    Ok(ValidatedAppendEvent {
        event_type,
        payload,
        actor,
        source,
        metadata,
        refs,
        idempotency_key,
        producer_id,
        producer_seq: request.producer_seq,
        expected_seq: request.expected_seq,
    })
}

pub fn has_scope(auth: &AuthContext, scope: &str) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => {
            if auth.scopes.iter().any(|candidate| candidate == scope) {
                Ok(())
            } else {
                Err(AppError::ForbiddenScope)
            }
        }
    }
}

fn session_permission(
    auth: &AuthContext,
    session_id: &str,
    tenant_id: &str,
    scope: &str,
) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => {
            has_scope(auth, scope)?;
            allowed_to_access_session(auth, session_id)?;
            require_same_tenant(auth, tenant_id)
        }
    }
}

fn require_same_tenant(auth: &AuthContext, tenant_id: &str) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::UnsafeJwt => {
            if auth.principal.tenant_id == tenant_id {
                Ok(())
            } else {
                Err(AppError::ForbiddenTenant)
            }
        }
    }
}

fn resolve_create_session_id(
    auth: &AuthContext,
    requested_id: Option<String>,
) -> Result<Option<String>, AppError> {
    match auth.kind {
        AuthMode::None => Ok(requested_id),
        AuthMode::UnsafeJwt => match (auth.session_id.as_ref(), requested_id) {
            (None, requested_id) => Ok(requested_id),
            (Some(session_id), None) => Ok(Some(session_id.clone())),
            (Some(session_id), Some(requested_id)) if session_id == &requested_id => {
                Ok(Some(requested_id))
            }
            (Some(_), Some(_)) => Err(AppError::ForbiddenSession),
        },
    }
}

fn resolve_event_actor(
    auth: &AuthContext,
    requested_actor: Option<String>,
) -> Result<String, AppError> {
    let actor = auth.actor();

    match requested_actor {
        None => Ok(actor),
        Some(requested_actor) => {
            let requested_actor = require_non_empty(requested_actor, AppError::InvalidEvent)?;

            if requested_actor == actor {
                Ok(actor)
            } else {
                Err(AppError::InvalidEvent)
            }
        }
    }
}

fn attach_principal_metadata(auth: &AuthContext, mut metadata: JsonMap) -> JsonMap {
    if auth.kind == AuthMode::None {
        return metadata;
    }

    let principal_metadata = json!({
        "tenant_id": auth.principal.tenant_id,
        "actor": auth.actor(),
        "principal_type": auth.principal.principal_type,
        "principal_id": auth.principal.id,
    });

    match metadata.get_mut("starcite_principal") {
        Some(Value::Object(existing)) => {
            existing.extend(principal_metadata.as_object().cloned().unwrap_or_default());
        }
        _ => {
            metadata.insert("starcite_principal".to_string(), principal_metadata);
        }
    }

    metadata
}

fn authenticate_unsafe_jwt(token: &str) -> Result<AuthContext, AppError> {
    let claims = parse_claims(token)?;
    let tenant_id = required_claim_string(&claims, "tenant_id")?;
    let subject = required_claim_string(&claims, "sub")?;
    let expires_at = required_claim_i64(&claims, "exp")?;

    if expires_at <= Utc::now().timestamp() {
        return Err(AppError::TokenExpired);
    }

    required_claim_string(&claims, "iss")?;
    require_audience_claim(&claims)?;

    let principal = principal_from_subject(tenant_id, subject)?;
    let scopes = claim_scopes(&claims)?;
    let session_id = optional_claim_string(&claims, "session_id")?;

    Ok(AuthContext {
        kind: AuthMode::UnsafeJwt,
        principal,
        scopes,
        session_id,
        expires_at: Some(expires_at),
    })
}

fn extract_bearer_token(header: &str) -> Result<&str, AppError> {
    match header.split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() => {
            Ok(token)
        }
        _ => Err(AppError::InvalidBearerToken),
    }
}

fn parse_claims(token: &str) -> Result<Value, AppError> {
    let mut segments = token.split('.');
    let _header = segments.next().ok_or(AppError::InvalidBearerToken)?;
    let payload = segments.next().ok_or(AppError::InvalidBearerToken)?;
    let _signature = segments.next().ok_or(AppError::InvalidBearerToken)?;

    if segments.next().is_some() {
        return Err(AppError::InvalidBearerToken);
    }

    let decoded = decode_base64url(payload)?;
    serde_json::from_slice(&decoded).map_err(|_| AppError::InvalidBearerToken)
}

fn decode_base64url(raw: &str) -> Result<Vec<u8>, AppError> {
    if raw.is_empty() || raw.len() % 4 == 1 {
        return Err(AppError::InvalidBearerToken);
    }

    let mut out = Vec::with_capacity(raw.len() * 3 / 4);
    let mut buffer = 0_u32;
    let mut bits = 0_u8;

    for byte in raw.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => break,
            _ => return Err(AppError::InvalidBearerToken),
        } as u32;

        buffer = (buffer << 6) | value;
        bits += 6;

        while bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }

    Ok(out)
}

fn required_claim_string(claims: &Value, key: &str) -> Result<String, AppError> {
    claims
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .ok_or(AppError::InvalidBearerToken)
}

fn optional_claim_string(claims: &Value, key: &str) -> Result<Option<String>, AppError> {
    match claims.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(value.clone())),
        _ => Err(AppError::InvalidBearerToken),
    }
}

fn required_claim_i64(claims: &Value, key: &str) -> Result<i64, AppError> {
    claims
        .get(key)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or(AppError::InvalidBearerToken)
}

fn require_audience_claim(claims: &Value) -> Result<(), AppError> {
    match claims.get("aud") {
        Some(Value::String(value)) if !value.is_empty() => Ok(()),
        Some(Value::Array(values))
            if values
                .iter()
                .all(|value| matches!(value, Value::String(value) if !value.is_empty())) =>
        {
            Ok(())
        }
        _ => Err(AppError::InvalidBearerToken),
    }
}

fn claim_scopes(claims: &Value) -> Result<Vec<String>, AppError> {
    let mut scopes = Vec::new();

    if let Some(scope) = claims.get("scope").and_then(Value::as_str) {
        scopes.extend(
            scope
                .split_whitespace()
                .filter(|scope| !scope.is_empty())
                .map(ToString::to_string),
        );
    }

    if let Some(Value::Array(values)) = claims.get("scopes") {
        for value in values {
            let scope = value
                .as_str()
                .filter(|scope| !scope.is_empty())
                .ok_or(AppError::InvalidBearerToken)?;
            scopes.push(scope.to_string());
        }
    }

    scopes.sort();
    scopes.dedup();

    if scopes.is_empty() {
        Err(AppError::InvalidBearerToken)
    } else {
        Ok(scopes)
    }
}

fn principal_from_subject(tenant_id: String, subject: String) -> Result<Principal, AppError> {
    let (principal_type, id) = subject
        .split_once(':')
        .ok_or(AppError::InvalidBearerToken)?;

    Ok(Principal {
        tenant_id,
        id: require_non_empty(id.to_string(), AppError::InvalidBearerToken)?,
        principal_type: normalize_principal_type(principal_type)
            .map_err(|_| AppError::InvalidBearerToken)?,
    })
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use serde_json::{Value, json};

    use super::{
        AppError, AuthContext, claim_scopes, decode_base64url, validate_append_request,
        validate_create_request,
    };
    use crate::{
        auth::{AuthMode, authenticate_http, can_subscribe_lifecycle},
        model::{AppendEventRequest, CreateSessionRequest, Principal},
    };

    #[test]
    fn unsafe_jwt_authenticates_http_claims() {
        let token = token_for(json!({
            "iss": "https://issuer.example",
            "aud": "starcite-api",
            "exp": 4_102_444_800_i64,
            "tenant_id": "acme",
            "sub": "service:rust-rewrite",
            "scope": "session:read session:create",
        }));
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        );

        let auth = authenticate_http(&headers, AuthMode::UnsafeJwt).expect("token should parse");

        assert_eq!(auth.principal.tenant_id, "acme");
        assert_eq!(auth.principal.id, "rust-rewrite");
        assert_eq!(auth.principal.principal_type, "service");
        assert!(auth.scopes.iter().any(|scope| scope == "session:read"));
    }

    #[test]
    fn unsafe_jwt_requires_service_principal_for_lifecycle() {
        let auth = AuthContext {
            kind: AuthMode::UnsafeJwt,
            principal: Principal {
                tenant_id: "acme".to_string(),
                id: "user-42".to_string(),
                principal_type: "user".to_string(),
            },
            scopes: vec!["session:read".to_string()],
            session_id: None,
            expires_at: Some(4_102_444_800_i64),
        };

        assert!(matches!(
            can_subscribe_lifecycle(&auth),
            Err(AppError::Forbidden)
        ));
    }

    #[test]
    fn unsafe_jwt_create_validation_uses_claim_tenant_and_session_lock() {
        let auth = AuthContext {
            kind: AuthMode::UnsafeJwt,
            principal: Principal {
                tenant_id: "acme".to_string(),
                id: "svc".to_string(),
                principal_type: "service".to_string(),
            },
            scopes: vec!["session:create".to_string()],
            session_id: Some("ses_locked".to_string()),
            expires_at: Some(4_102_444_800_i64),
        };

        let validated = validate_create_request(
            CreateSessionRequest {
                id: None,
                title: Some("Draft".to_string()),
                metadata: Some(json!({"workflow": "contract"})),
                tenant_id: Some("acme".to_string()),
                creator_principal: None,
            },
            &auth,
        )
        .expect("request should validate");

        assert_eq!(validated.id, "ses_locked");
        assert_eq!(validated.tenant_id, "acme");
        assert_eq!(validated.creator_principal.id, "svc");
    }

    #[test]
    fn unsafe_jwt_append_validation_derives_actor_and_principal_metadata() {
        let auth = AuthContext {
            kind: AuthMode::UnsafeJwt,
            principal: Principal {
                tenant_id: "acme".to_string(),
                id: "user-42".to_string(),
                principal_type: "user".to_string(),
            },
            scopes: vec!["session:append".to_string()],
            session_id: None,
            expires_at: Some(4_102_444_800_i64),
        };

        let validated = validate_append_request(
            AppendEventRequest {
                event_type: "content".to_string(),
                payload: json!({"text": "hello"}),
                actor: None,
                source: None,
                metadata: Some(json!({"workflow": "contract"})),
                refs: None,
                idempotency_key: None,
                producer_id: "writer-1".to_string(),
                producer_seq: 1,
                expected_seq: Some(0),
            },
            &auth,
        )
        .expect("append should validate");

        assert_eq!(validated.actor, "user:user-42");
        assert_eq!(
            validated.metadata["starcite_principal"],
            json!({
                "tenant_id": "acme",
                "actor": "user:user-42",
                "principal_type": "user",
                "principal_id": "user-42"
            })
        );
    }

    #[test]
    fn scope_claims_merge_string_and_array() {
        let scopes = claim_scopes(&json!({
            "scope": "session:read session:create",
            "scopes": ["session:append"]
        }))
        .expect("scopes should parse");

        assert_eq!(
            scopes,
            vec!["session:append", "session:create", "session:read"]
        );
    }

    #[test]
    fn base64url_decoder_round_trips_unpadded_payload() {
        let decoded = decode_base64url("eyJmb28iOiJiYXIifQ").expect("payload should decode");
        assert_eq!(
            String::from_utf8(decoded).expect("utf8"),
            "{\"foo\":\"bar\"}"
        );
    }

    fn token_for(claims: Value) -> String {
        let header = encode_base64url(r#"{"alg":"none","typ":"JWT"}"#.as_bytes());
        let payload = encode_base64url(serde_json::to_string(&claims).expect("claims").as_bytes());
        format!("{header}.{payload}.signature")
    }

    fn encode_base64url(raw: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

        let mut out = String::new();
        let mut index = 0;

        while index + 3 <= raw.len() {
            let block = ((raw[index] as u32) << 16)
                | ((raw[index + 1] as u32) << 8)
                | raw[index + 2] as u32;

            out.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
            out.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
            out.push(TABLE[((block >> 6) & 0x3f) as usize] as char);
            out.push(TABLE[(block & 0x3f) as usize] as char);
            index += 3;
        }

        match raw.len() - index {
            1 => {
                let block = (raw[index] as u32) << 16;
                out.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
                out.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
            }
            2 => {
                let block = ((raw[index] as u32) << 16) | ((raw[index + 1] as u32) << 8);
                out.push(TABLE[((block >> 18) & 0x3f) as usize] as char);
                out.push(TABLE[((block >> 12) & 0x3f) as usize] as char);
                out.push(TABLE[((block >> 6) & 0x3f) as usize] as char);
            }
            _ => {}
        }

        out
    }
}
