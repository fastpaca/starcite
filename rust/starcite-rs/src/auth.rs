use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::http::HeaderMap;
use chrono::Utc;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    errors::ErrorKind,
    jwk::{Jwk, JwkSet},
};
use reqwest::Client;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    config::{AuthMode, Config},
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

#[derive(Debug, Clone)]
pub struct AuthService {
    mode: AuthMode,
    jwt: Option<JwtVerifier>,
}

#[derive(Debug, Clone)]
struct JwtVerifier {
    issuer: Arc<str>,
    audience: Arc<str>,
    jwks_url: Arc<str>,
    leeway_seconds: u64,
    refresh_interval: Duration,
    hard_expiry: Duration,
    client: Client,
    cache: Arc<Mutex<JwksCache>>,
}

#[derive(Debug, Default)]
struct JwksCache {
    fetched_at: Option<Instant>,
    keys: Vec<Jwk>,
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

impl AuthService {
    pub fn new(config: &Config) -> Result<Self, String> {
        let jwt = match config.auth_mode {
            AuthMode::None => None,
            AuthMode::Jwt => Some(JwtVerifier::new(config)?),
        };

        Ok(Self {
            mode: config.auth_mode,
            jwt,
        })
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }
}

impl JwtVerifier {
    fn new(config: &Config) -> Result<Self, String> {
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| format!("failed to build JWKS client: {error}"))?;

        Ok(Self {
            issuer: Arc::from(
                config
                    .auth_issuer
                    .clone()
                    .ok_or_else(|| "missing JWT issuer config".to_string())?,
            ),
            audience: Arc::from(
                config
                    .auth_audience
                    .clone()
                    .ok_or_else(|| "missing JWT audience config".to_string())?,
            ),
            jwks_url: Arc::from(
                config
                    .auth_jwks_url
                    .clone()
                    .ok_or_else(|| "missing JWKS URL config".to_string())?,
            ),
            leeway_seconds: config.auth_jwt_leeway_seconds,
            refresh_interval: Duration::from_millis(config.auth_jwks_refresh_ms),
            hard_expiry: Duration::from_millis(config.auth_jwks_hard_expiry_ms),
            client,
            cache: Arc::new(Mutex::new(JwksCache::default())),
        })
    }
}

pub async fn authenticate_http(
    headers: &HeaderMap,
    auth: &AuthService,
) -> Result<AuthContext, AppError> {
    match auth.mode() {
        AuthMode::None => Ok(AuthContext::none()),
        AuthMode::Jwt => {
            let value = headers
                .get(axum::http::header::AUTHORIZATION)
                .ok_or(AppError::MissingBearerToken)?;
            let raw = value.to_str().map_err(|_| AppError::InvalidBearerToken)?;
            let token = extract_bearer_token(raw)?;
            authenticate_jwt(token, auth).await
        }
    }
}

pub async fn authenticate_socket(
    params: &HashMap<String, String>,
    auth: &AuthService,
) -> Result<AuthContext, AppError> {
    if params.contains_key("access_token") {
        return Err(AppError::InvalidBearerToken);
    }

    match auth.mode() {
        AuthMode::None => Ok(AuthContext::none()),
        AuthMode::Jwt => {
            let token = params
                .get("token")
                .filter(|token| !token.is_empty())
                .ok_or(AppError::MissingBearerToken)?;
            authenticate_jwt(token, auth).await
        }
    }
}

pub async fn authenticate_raw_socket(
    headers: &HeaderMap,
    params: &HashMap<String, String>,
    auth: &AuthService,
) -> Result<AuthContext, AppError> {
    match auth.mode() {
        AuthMode::None => Ok(AuthContext::none()),
        AuthMode::Jwt => {
            if headers.contains_key(axum::http::header::AUTHORIZATION) {
                return authenticate_http(headers, auth).await;
            }

            let token = params
                .get("access_token")
                .or_else(|| params.get("token"))
                .filter(|token| !token.is_empty())
                .ok_or(AppError::MissingBearerToken)?;

            authenticate_jwt(token, auth).await
        }
    }
}

pub fn can_create_session(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::Jwt => has_scope(auth, "session:create"),
    }
}

pub fn can_read_session(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::Jwt => has_scope(auth, "session:read"),
    }
}

pub fn can_subscribe_lifecycle(auth: &AuthContext) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::Jwt => {
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
        AuthMode::Jwt => match auth.session_id.as_deref() {
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
        AuthMode::Jwt => {
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
        AuthMode::Jwt => {
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
        AuthMode::Jwt => {
            has_scope(auth, scope)?;
            allowed_to_access_session(auth, session_id)?;
            require_same_tenant(auth, tenant_id)
        }
    }
}

fn require_same_tenant(auth: &AuthContext, tenant_id: &str) -> Result<(), AppError> {
    match auth.kind {
        AuthMode::None => Ok(()),
        AuthMode::Jwt => {
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
        AuthMode::Jwt => match (auth.session_id.as_ref(), requested_id) {
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

async fn authenticate_jwt(token: &str, auth: &AuthService) -> Result<AuthContext, AppError> {
    let verifier = auth.jwt.as_ref().ok_or(AppError::InvalidBearerToken)?;
    let header = decode_header(token).map_err(|_| AppError::InvalidBearerToken)?;
    let kid = header
        .kid
        .as_deref()
        .filter(|kid| !kid.is_empty())
        .ok_or(AppError::InvalidBearerToken)?;
    let algorithm = header.alg;
    ensure_supported_algorithm(algorithm)?;
    let jwk = verifier.signing_jwk(kid).await?;
    let decoding_key = DecodingKey::from_jwk(&jwk).map_err(|_| AppError::InvalidBearerToken)?;
    let validation = verifier.validation(algorithm);
    let token_data =
        decode::<Value>(token, &decoding_key, &validation).map_err(map_jwt_decode_error)?;

    validate_issued_at(token_data.claims.get("iat"), verifier.leeway_seconds)?;
    auth_context_from_claims(&token_data.claims)
}

impl JwtVerifier {
    async fn signing_jwk(&self, kid: &str) -> Result<Jwk, AppError> {
        if self.should_refresh(kid).await {
            match self.fetch_jwks().await {
                Ok(keys) => {
                    let mut cache = self.cache.lock().await;
                    cache.fetched_at = Some(Instant::now());
                    cache.keys = keys;
                }
                Err(_error) => {
                    let cache = self.cache.lock().await;

                    if !cache.is_usable(kid, self.hard_expiry) {
                        return Err(AppError::InvalidBearerToken);
                    }
                }
            }
        }

        let cache = self.cache.lock().await;
        cache.find(kid).cloned().ok_or(AppError::InvalidBearerToken)
    }

    async fn should_refresh(&self, kid: &str) -> bool {
        let cache = self.cache.lock().await;

        if cache.keys.is_empty() || cache.find(kid).is_none() {
            return true;
        }

        match cache.fetched_at {
            Some(fetched_at) => fetched_at.elapsed() >= self.refresh_interval,
            None => true,
        }
    }

    async fn fetch_jwks(&self) -> Result<Vec<Jwk>, AppError> {
        let response = self
            .client
            .get(self.jwks_url.as_ref())
            .send()
            .await
            .map_err(|_| AppError::InvalidBearerToken)?;
        let response = response
            .error_for_status()
            .map_err(|_| AppError::InvalidBearerToken)?;
        let jwks = response
            .json::<JwkSet>()
            .await
            .map_err(|_| AppError::InvalidBearerToken)?;

        if jwks.keys.is_empty() {
            return Err(AppError::InvalidBearerToken);
        }

        Ok(jwks.keys)
    }

    fn validation(&self, algorithm: Algorithm) -> Validation {
        let mut validation = Validation::new(algorithm);
        validation.leeway = self.leeway_seconds;
        validation.validate_nbf = true;
        validation.set_issuer(&[self.issuer.as_ref()]);
        validation.set_audience(&[self.audience.as_ref()]);
        validation.required_spec_claims.extend([
            "exp".to_string(),
            "iss".to_string(),
            "aud".to_string(),
        ]);
        validation
    }
}

impl JwksCache {
    fn find(&self, kid: &str) -> Option<&Jwk> {
        self.keys
            .iter()
            .find(|jwk| jwk.common.key_id.as_deref() == Some(kid))
    }

    fn is_usable(&self, kid: &str, hard_expiry: Duration) -> bool {
        match self.fetched_at {
            Some(fetched_at) if fetched_at.elapsed() <= hard_expiry => self.find(kid).is_some(),
            _ => false,
        }
    }
}

fn auth_context_from_claims(claims: &Value) -> Result<AuthContext, AppError> {
    let tenant_id = required_claim_string(claims, "tenant_id")?;
    let subject = required_claim_string(claims, "sub")?;
    let expires_at = required_claim_i64(claims, "exp")?;
    let principal = principal_from_subject(tenant_id, subject)?;
    let scopes = claim_scopes(claims)?;
    let session_id = optional_claim_string(claims, "session_id")?;

    Ok(AuthContext {
        kind: AuthMode::Jwt,
        principal,
        scopes,
        session_id,
        expires_at: Some(expires_at),
    })
}

fn ensure_supported_algorithm(algorithm: Algorithm) -> Result<(), AppError> {
    match algorithm {
        Algorithm::HS256
        | Algorithm::HS384
        | Algorithm::HS512
        | Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512
        | Algorithm::ES256
        | Algorithm::ES384
        | Algorithm::EdDSA => Ok(()),
    }
}

fn map_jwt_decode_error(error: jsonwebtoken::errors::Error) -> AppError {
    match error.kind() {
        ErrorKind::ExpiredSignature => AppError::TokenExpired,
        _ => AppError::InvalidBearerToken,
    }
}

fn validate_issued_at(iat: Option<&Value>, leeway_seconds: u64) -> Result<(), AppError> {
    let Some(iat) = iat else {
        return Ok(());
    };
    let issued_at = iat.as_i64().ok_or(AppError::InvalidBearerToken)?;
    let now = Utc::now().timestamp();

    if issued_at <= now + leeway_seconds as i64 {
        Ok(())
    } else {
        Err(AppError::InvalidBearerToken)
    }
}

fn extract_bearer_token(header: &str) -> Result<&str, AppError> {
    match header.split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") && !token.is_empty() => {
            Ok(token)
        }
        _ => Err(AppError::InvalidBearerToken),
    }
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
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use axum::{
        Router,
        http::{HeaderMap, HeaderValue, StatusCode, header::AUTHORIZATION},
        routing::get,
    };
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use serde_json::{Value, json};
    use tokio::{
        sync::Mutex,
        task::JoinHandle,
        time::{Duration, sleep},
    };

    use super::{
        AppError, AuthContext, AuthService, auth_context_from_claims, claim_scopes,
        validate_append_request, validate_create_request,
    };
    use crate::{
        auth::{AuthMode, authenticate_http, can_subscribe_lifecycle},
        config::Config,
        model::{AppendEventRequest, CreateSessionRequest, Principal},
    };

    #[tokio::test]
    async fn jwt_authenticates_http_claims() {
        let secret = "super-secret";
        let (jwks_url, _state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service = AuthService::new(&jwt_config(&jwks_url)).expect("auth config");
        let token = token_for(
            json!({
            "iss": "https://issuer.example",
            "aud": "starcite-api",
            "exp": 4_102_444_800_i64,
            "tenant_id": "acme",
            "sub": "service:rust-rewrite",
            "scope": "session:read session:create",
            }),
            secret,
            "kid-1",
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        );

        let auth = authenticate_http(&headers, &auth_service)
            .await
            .expect("token should parse");

        assert_eq!(auth.principal.tenant_id, "acme");
        assert_eq!(auth.principal.id, "rust-rewrite");
        assert_eq!(auth.principal.principal_type, "service");
        assert!(auth.scopes.iter().any(|scope| scope == "session:read"));

        server.abort();
    }

    #[tokio::test]
    async fn jwt_authenticates_socket_claims() {
        let secret = "super-secret";
        let (jwks_url, _state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service = AuthService::new(&jwt_config(&jwks_url)).expect("auth config");
        let token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            secret,
            "kid-1",
        );
        let params = HashMap::from([("token".to_string(), token)]);

        let auth = super::authenticate_socket(&params, &auth_service)
            .await
            .expect("socket token should authenticate");

        assert_eq!(auth.principal.tenant_id, "acme");
        assert_eq!(auth.principal.id, "rust-rewrite");
        assert_eq!(auth.principal.principal_type, "service");

        server.abort();
    }

    #[tokio::test]
    async fn socket_auth_rejects_access_token_param() {
        let secret = "super-secret";
        let (jwks_url, _state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service = AuthService::new(&jwt_config(&jwks_url)).expect("auth config");
        let token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            secret,
            "kid-1",
        );
        let params = HashMap::from([("access_token".to_string(), token)]);

        assert!(matches!(
            super::authenticate_socket(&params, &auth_service).await,
            Err(AppError::InvalidBearerToken)
        ));

        server.abort();
    }

    #[tokio::test]
    async fn raw_socket_auth_accepts_access_token_param() {
        let secret = "super-secret";
        let (jwks_url, _state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service = AuthService::new(&jwt_config(&jwks_url)).expect("auth config");
        let token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            secret,
            "kid-1",
        );
        let headers = HeaderMap::new();
        let params = HashMap::from([("access_token".to_string(), token)]);

        let auth = super::authenticate_raw_socket(&headers, &params, &auth_service)
            .await
            .expect("raw socket token should authenticate");

        assert_eq!(auth.principal.tenant_id, "acme");
        assert_eq!(auth.principal.id, "rust-rewrite");

        server.abort();
    }

    #[tokio::test]
    async fn raw_socket_auth_prefers_authorization_header() {
        let secret = "super-secret";
        let (jwks_url, _state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service = AuthService::new(&jwt_config(&jwks_url)).expect("auth config");
        let header_token = token_for(
            base_claims("service:header-auth", "session:read"),
            secret,
            "kid-1",
        );
        let query_token = token_for(
            base_claims("service:query-auth", "session:read"),
            secret,
            "kid-1",
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {header_token}")).expect("header"),
        );
        let params = HashMap::from([("access_token".to_string(), query_token)]);

        let auth = super::authenticate_raw_socket(&headers, &params, &auth_service)
            .await
            .expect("header token should authenticate");

        assert_eq!(auth.principal.id, "header-auth");

        server.abort();
    }

    #[tokio::test]
    async fn jwt_refreshes_when_a_new_kid_appears() {
        let (jwks_url, state, server) = spawn_jwks_server(jwks("super-secret", "kid-1")).await;
        let auth_service =
            AuthService::new(&jwt_config_with_jwks_cache(&jwks_url, 5, 100)).expect("auth config");

        let first_token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            "super-secret",
            "kid-1",
        );
        let second_token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            "rotated-secret",
            "kid-2",
        );

        authenticate_bearer(&auth_service, &first_token)
            .await
            .expect("first token should authenticate");

        set_jwks_response(&state, StatusCode::OK, jwks("rotated-secret", "kid-2")).await;

        let rotated = authenticate_bearer(&auth_service, &second_token)
            .await
            .expect("rotated token should authenticate");

        assert_eq!(rotated.principal.id, "rust-rewrite");
        server.abort();
    }

    #[tokio::test]
    async fn jwt_uses_cached_key_when_refresh_fails_before_hard_expiry() {
        let secret = "super-secret";
        let (jwks_url, state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service =
            AuthService::new(&jwt_config_with_jwks_cache(&jwks_url, 5, 100)).expect("auth config");
        let token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            secret,
            "kid-1",
        );

        authenticate_bearer(&auth_service, &token)
            .await
            .expect("initial token should authenticate");

        sleep(Duration::from_millis(15)).await;
        set_jwks_response(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "boom"}),
        )
        .await;

        authenticate_bearer(&auth_service, &token)
            .await
            .expect("cached key should remain usable before hard expiry");

        server.abort();
    }

    #[tokio::test]
    async fn jwt_rejects_when_refresh_fails_after_hard_expiry() {
        let secret = "super-secret";
        let (jwks_url, state, server) = spawn_jwks_server(jwks(secret, "kid-1")).await;
        let auth_service =
            AuthService::new(&jwt_config_with_jwks_cache(&jwks_url, 5, 10)).expect("auth config");
        let token = token_for(
            base_claims("service:rust-rewrite", "session:read"),
            secret,
            "kid-1",
        );

        authenticate_bearer(&auth_service, &token)
            .await
            .expect("initial token should authenticate");

        sleep(Duration::from_millis(20)).await;
        set_jwks_response(
            &state,
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "boom"}),
        )
        .await;

        assert!(matches!(
            authenticate_bearer(&auth_service, &token).await,
            Err(AppError::InvalidBearerToken)
        ));

        server.abort();
    }

    #[test]
    fn jwt_requires_service_principal_for_lifecycle() {
        let auth = AuthContext {
            kind: AuthMode::Jwt,
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
    fn jwt_create_validation_uses_claim_tenant_and_session_lock() {
        let auth = AuthContext {
            kind: AuthMode::Jwt,
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
    fn jwt_append_validation_derives_actor_and_principal_metadata() {
        let auth = AuthContext {
            kind: AuthMode::Jwt,
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
    fn claims_to_auth_context_requires_verified_claim_shape() {
        let auth = auth_context_from_claims(&json!({
            "exp": 4_102_444_800_i64,
            "tenant_id": "acme",
            "sub": "service:rust-rewrite",
            "scope": "session:read session:create"
        }))
        .expect("claims should map");

        assert_eq!(auth.kind, AuthMode::Jwt);
        assert_eq!(auth.principal.tenant_id, "acme");
        assert_eq!(auth.principal.id, "rust-rewrite");
    }

    #[derive(Debug)]
    struct MutableJwksResponse {
        status: StatusCode,
        body: String,
    }

    async fn spawn_jwks_server(
        body: Value,
    ) -> (
        String,
        std::sync::Arc<Mutex<MutableJwksResponse>>,
        JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let state = std::sync::Arc::new(Mutex::new(MutableJwksResponse {
            status: StatusCode::OK,
            body: body.to_string(),
        }));
        let route_state = state.clone();
        let app = Router::new().route(
            "/jwks",
            get(move || {
                let route_state = route_state.clone();
                async move {
                    let response = route_state.lock().await;
                    (response.status, response.body.clone())
                }
            }),
        );
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve jwks");
        });

        (format!("http://{addr}/jwks"), state, handle)
    }

    async fn set_jwks_response(
        state: &std::sync::Arc<Mutex<MutableJwksResponse>>,
        status: StatusCode,
        body: Value,
    ) {
        let mut response = state.lock().await;
        response.status = status;
        response.body = body.to_string();
    }

    fn jwt_config(jwks_url: &str) -> Config {
        jwt_config_with_jwks_cache(jwks_url, 60_000, 60_000)
    }

    fn jwt_config_with_jwks_cache(jwks_url: &str, refresh_ms: u64, hard_expiry_ms: u64) -> Config {
        Config {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4001),
            ops_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 4002),
            database_url: "postgres://postgres:postgres@localhost/starcite_test".to_string(),
            max_connections: 1,
            archive_flush_interval_ms: 5_000,
            migrate_on_boot: false,
            auth_mode: AuthMode::Jwt,
            auth_issuer: Some("https://issuer.example".to_string()),
            auth_audience: Some("starcite-api".to_string()),
            auth_jwks_url: Some(jwks_url.to_string()),
            auth_jwt_leeway_seconds: 1,
            auth_jwks_refresh_ms: refresh_ms,
            auth_jwks_hard_expiry_ms: hard_expiry_ms,
            telemetry_enabled: false,
            shutdown_drain_timeout_ms: 30_000,
            session_runtime_idle_timeout_ms: 30_000,
            commit_flush_interval_ms: 100,
            local_async_lease_ttl_ms: 5_000,
            local_async_node_public_url: None,
            local_async_node_ops_url: None,
            local_async_node_ttl_ms: 2_000,
            local_async_owner_proxy_timeout_ms: 1_000,
            local_async_standby_url: None,
            local_async_replication_timeout_ms: 500,
        }
    }

    async fn authenticate_bearer(
        auth_service: &AuthService,
        token: &str,
    ) -> Result<AuthContext, AppError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).expect("header"),
        );
        authenticate_http(&headers, auth_service).await
    }

    fn base_claims(subject: &str, scope: &str) -> Value {
        json!({
            "iss": "https://issuer.example",
            "aud": "starcite-api",
            "exp": 4_102_444_800_i64,
            "tenant_id": "acme",
            "sub": subject,
            "scope": scope,
        })
    }

    fn jwks(secret: &str, kid: &str) -> Value {
        json!({
            "keys": [{
                "kty": "oct",
                "alg": "HS256",
                "use": "sig",
                "kid": kid,
                "k": encode_base64url(secret.as_bytes()),
            }]
        })
    }

    fn token_for(claims: Value, secret: &str, kid: &str) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());

        encode(
            &header,
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("encode token")
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
