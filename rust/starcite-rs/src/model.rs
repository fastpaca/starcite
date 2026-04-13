use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::AppError;

pub type JsonMap = Map<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Principal {
    pub tenant_id: String,
    pub id: String,
    #[serde(rename = "type")]
    pub principal_type: String,
}

impl Principal {
    pub fn normalized(mut self) -> Result<Self, AppError> {
        self.tenant_id = require_non_empty(self.tenant_id, AppError::InvalidSession)?;
        self.id = require_non_empty(self.id, AppError::InvalidSession)?;
        self.principal_type = normalize_principal_type(&self.principal_type)?;
        Ok(self)
    }

    pub fn service(tenant_id: String) -> Self {
        Self {
            tenant_id,
            id: "service".to_string(),
            principal_type: "service".to_string(),
        }
    }

    pub fn actor(&self) -> String {
        format!("{}:{}", self.principal_type, self.id)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateSessionRequest {
    pub id: Option<String>,
    pub title: Option<String>,
    pub metadata: Option<Value>,
    pub tenant_id: Option<String>,
    pub creator_principal: Option<Principal>,
}

#[derive(Debug, Clone)]
pub struct ValidatedCreateSession {
    pub id: String,
    pub title: Option<String>,
    pub metadata: JsonMap,
    pub tenant_id: String,
    pub creator_principal: Principal,
}

impl CreateSessionRequest {
    pub fn validate(self) -> Result<ValidatedCreateSession, AppError> {
        let metadata = optional_object(self.metadata, AppError::InvalidMetadata, true)?;
        let requested_tenant = self
            .tenant_id
            .map(|value| require_non_empty(value, AppError::InvalidSession));
        let requested_tenant = requested_tenant.transpose()?;

        let creator_principal = match self.creator_principal {
            Some(principal) => principal.normalized()?,
            None => Principal::service(
                requested_tenant
                    .clone()
                    .unwrap_or_else(|| "service".to_string()),
            ),
        };

        if let Some(tenant_id) = requested_tenant.as_ref() {
            if tenant_id != &creator_principal.tenant_id {
                return Err(AppError::InvalidSession);
            }
        }

        let id = match self.id {
            Some(id) => require_non_empty(id, AppError::InvalidSession)?,
            None => format!("ses_{}", Uuid::now_v7().simple()),
        };

        Ok(ValidatedCreateSession {
            id,
            title: self.title,
            metadata,
            tenant_id: creator_principal.tenant_id.clone(),
            creator_principal,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateSessionRequest {
    #[serde(default)]
    pub title: Option<Option<String>>,
    #[serde(default)]
    pub metadata: Option<Value>,
    pub expected_version: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ValidatedUpdateSession {
    pub title: Option<Option<String>>,
    pub metadata: Option<JsonMap>,
    pub expected_version: Option<i64>,
}

impl UpdateSessionRequest {
    pub fn validate(self) -> Result<ValidatedUpdateSession, AppError> {
        if self.title.is_none() && self.metadata.is_none() {
            return Err(AppError::InvalidSession);
        }

        let metadata = match self.metadata {
            Some(value) => Some(optional_object(
                Some(value),
                AppError::InvalidMetadata,
                false,
            )?),
            None => None,
        };

        if self.title.is_none() && matches!(metadata.as_ref(), Some(map) if map.is_empty()) {
            return Err(AppError::InvalidSession);
        }

        if let Some(expected_version) = self.expected_version {
            if expected_version <= 0 {
                return Err(AppError::InvalidSession);
            }
        }

        Ok(ValidatedUpdateSession {
            title: self.title,
            metadata,
            expected_version: self.expected_version,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEventRequest {
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: Value,
    pub actor: Option<String>,
    pub source: Option<String>,
    pub metadata: Option<Value>,
    pub refs: Option<Value>,
    pub idempotency_key: Option<String>,
    pub producer_id: String,
    pub producer_seq: i64,
    pub expected_seq: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ValidatedAppendEvent {
    pub event_type: String,
    pub payload: JsonMap,
    pub actor: String,
    pub source: Option<String>,
    pub metadata: JsonMap,
    pub refs: JsonMap,
    pub idempotency_key: Option<String>,
    pub producer_id: String,
    pub producer_seq: i64,
    pub expected_seq: Option<i64>,
}

impl AppendEventRequest {
    pub fn validate(self) -> Result<ValidatedAppendEvent, AppError> {
        let event_type = require_non_empty(self.event_type, AppError::InvalidEvent)?;
        let payload = require_object(self.payload, AppError::InvalidEvent)?;
        let actor = self
            .actor
            .map(|value| require_non_empty(value, AppError::InvalidEvent))
            .transpose()?
            .unwrap_or_else(|| "service:service".to_string());
        let source = self
            .source
            .map(|value| require_non_empty(value, AppError::InvalidEvent))
            .transpose()?;
        let metadata = optional_object(self.metadata, AppError::InvalidMetadata, true)?;
        let refs = optional_object(self.refs, AppError::InvalidRefs, true)?;
        let idempotency_key = self
            .idempotency_key
            .map(|value| require_non_empty(value, AppError::InvalidEvent))
            .transpose()?;
        let producer_id = require_non_empty(self.producer_id, AppError::InvalidEvent)?;

        if self.producer_seq <= 0 {
            return Err(AppError::InvalidEvent);
        }

        if let Some(expected_seq) = self.expected_seq {
            if expected_seq < 0 {
                return Err(AppError::InvalidEvent);
            }
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
            producer_seq: self.producer_seq,
            expected_seq: self.expected_seq,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedFilter {
    Active,
    Archived,
    All,
}

#[derive(Debug, Clone)]
pub struct ListOptions {
    pub limit: u32,
    pub cursor: Option<String>,
    pub archived: ArchivedFilter,
    pub metadata: JsonMap,
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EventsOptions {
    pub cursor: i64,
    pub limit: u32,
}

#[derive(Debug, Clone, FromRow)]
pub struct SessionRow {
    pub id: String,
    pub title: Option<String>,
    pub tenant_id: String,
    pub creator_id: Option<String>,
    pub creator_type: Option<String>,
    pub metadata: Value,
    pub last_seq: i64,
    pub archived: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: i64,
}

#[derive(Debug, Clone, FromRow)]
pub struct EventRow {
    pub session_id: String,
    pub seq: i64,
    #[sqlx(rename = "type")]
    pub event_type: String,
    pub payload: Value,
    pub actor: String,
    pub source: Option<String>,
    pub metadata: Value,
    pub refs: Value,
    pub idempotency_key: Option<String>,
    pub producer_id: String,
    pub producer_seq: i64,
    pub tenant_id: String,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct LifecycleRow {
    pub seq: i64,
    pub tenant_id: String,
    pub session_id: String,
    pub event: Value,
    pub inserted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionResponse {
    pub id: String,
    pub title: Option<String>,
    pub creator_principal: Option<Principal>,
    pub metadata: JsonMap,
    pub last_seq: i64,
    pub created_at: String,
    pub updated_at: String,
    pub version: i64,
    pub archived: bool,
}

impl TryFrom<SessionRow> for SessionResponse {
    type Error = AppError;

    fn try_from(row: SessionRow) -> Result<Self, Self::Error> {
        let creator_principal = creator_principal_from_row(&row)?;
        let metadata = value_to_object(row.metadata, AppError::Internal)?;

        Ok(Self {
            id: row.id,
            title: row.title,
            creator_principal,
            metadata,
            last_seq: row.last_seq,
            created_at: iso8601(row.created_at),
            updated_at: iso8601(row.updated_at),
            version: row.version,
            archived: row.archived,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventResponse {
    pub session_id: String,
    pub seq: i64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub payload: JsonMap,
    pub actor: String,
    pub source: Option<String>,
    pub metadata: JsonMap,
    pub refs: JsonMap,
    pub idempotency_key: Option<String>,
    pub producer_id: String,
    pub producer_seq: i64,
    pub tenant_id: String,
    pub inserted_at: String,
    pub cursor: i64,
}

impl TryFrom<EventRow> for EventResponse {
    type Error = AppError;

    fn try_from(row: EventRow) -> Result<Self, Self::Error> {
        let payload = value_to_object(row.payload, AppError::Internal)?;
        let metadata = value_to_object(row.metadata, AppError::Internal)?;
        let refs = value_to_object(row.refs, AppError::Internal)?;
        let seq = row.seq;

        Ok(Self {
            session_id: row.session_id,
            seq,
            event_type: row.event_type,
            payload,
            actor: row.actor,
            source: row.source,
            metadata,
            refs,
            idempotency_key: row.idempotency_key,
            producer_id: row.producer_id,
            producer_seq: row.producer_seq,
            tenant_id: row.tenant_id,
            inserted_at: iso8601(row.inserted_at),
            cursor: seq,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionsPage {
    pub sessions: Vec<SessionResponse>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventsPage {
    pub events: Vec<EventResponse>,
    pub next_cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct LifecycleResponse {
    pub cursor: i64,
    pub inserted_at: String,
    pub event: LifecycleEvent,
}

impl TryFrom<LifecycleRow> for LifecycleResponse {
    type Error = AppError;

    fn try_from(row: LifecycleRow) -> Result<Self, Self::Error> {
        Ok(Self {
            cursor: row.seq,
            inserted_at: iso8601(row.inserted_at),
            event: serde_json::from_value(row.event).map_err(|_| AppError::Internal)?,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LifecyclePage {
    pub events: Vec<LifecycleResponse>,
    pub next_cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AppendReply {
    pub seq: i64,
    pub last_seq: i64,
    pub deduped: bool,
    pub cursor: i64,
    pub committed_cursor: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum LifecycleEvent {
    #[serde(rename = "session.activated")]
    SessionActivated {
        session_id: String,
        tenant_id: String,
    },
    #[serde(rename = "session.hydrating")]
    SessionHydrating {
        session_id: String,
        tenant_id: String,
    },
    #[serde(rename = "session.freezing")]
    SessionFreezing {
        session_id: String,
        tenant_id: String,
    },
    #[serde(rename = "session.frozen")]
    SessionFrozen {
        session_id: String,
        tenant_id: String,
    },
    #[serde(rename = "session.created")]
    SessionCreated {
        session_id: String,
        tenant_id: String,
        title: Option<String>,
        metadata: JsonMap,
        created_at: String,
        version: i64,
    },
    #[serde(rename = "session.updated")]
    SessionUpdated {
        session_id: String,
        tenant_id: String,
        title: Option<String>,
        metadata: JsonMap,
        updated_at: String,
        version: i64,
    },
    #[serde(rename = "session.archived")]
    SessionArchived {
        session_id: String,
        tenant_id: String,
        archived: bool,
    },
    #[serde(rename = "session.unarchived")]
    SessionUnarchived {
        session_id: String,
        tenant_id: String,
        archived: bool,
    },
}

impl LifecycleEvent {
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::SessionActivated { tenant_id, .. }
            | Self::SessionHydrating { tenant_id, .. }
            | Self::SessionFreezing { tenant_id, .. }
            | Self::SessionFrozen { tenant_id, .. }
            | Self::SessionCreated { tenant_id, .. }
            | Self::SessionUpdated { tenant_id, .. }
            | Self::SessionArchived { tenant_id, .. }
            | Self::SessionUnarchived { tenant_id, .. } => tenant_id,
        }
    }

    pub fn session_id(&self) -> &str {
        match self {
            Self::SessionActivated { session_id, .. }
            | Self::SessionHydrating { session_id, .. }
            | Self::SessionFreezing { session_id, .. }
            | Self::SessionFrozen { session_id, .. }
            | Self::SessionCreated { session_id, .. }
            | Self::SessionUpdated { session_id, .. }
            | Self::SessionArchived { session_id, .. }
            | Self::SessionUnarchived { session_id, .. } => session_id,
        }
    }

    pub fn activated(session_id: String, tenant_id: String) -> Self {
        Self::SessionActivated {
            session_id,
            tenant_id,
        }
    }

    pub fn hydrating(session_id: String, tenant_id: String) -> Self {
        Self::SessionHydrating {
            session_id,
            tenant_id,
        }
    }

    pub fn freezing(session_id: String, tenant_id: String) -> Self {
        Self::SessionFreezing {
            session_id,
            tenant_id,
        }
    }

    pub fn frozen(session_id: String, tenant_id: String) -> Self {
        Self::SessionFrozen {
            session_id,
            tenant_id,
        }
    }

    pub fn created(tenant_id: String, session: &SessionResponse) -> Self {
        Self::SessionCreated {
            session_id: session.id.clone(),
            tenant_id,
            title: session.title.clone(),
            metadata: session.metadata.clone(),
            created_at: session.created_at.clone(),
            version: session.version,
        }
    }

    pub fn updated(tenant_id: String, session: &SessionResponse) -> Self {
        Self::SessionUpdated {
            session_id: session.id.clone(),
            tenant_id,
            title: session.title.clone(),
            metadata: session.metadata.clone(),
            updated_at: session.updated_at.clone(),
            version: session.version,
        }
    }

    pub fn archived(tenant_id: String, session: &SessionResponse) -> Self {
        Self::SessionArchived {
            session_id: session.id.clone(),
            tenant_id,
            archived: true,
        }
    }

    pub fn unarchived(tenant_id: String, session: &SessionResponse) -> Self {
        Self::SessionUnarchived {
            session_id: session.id.clone(),
            tenant_id,
            archived: false,
        }
    }
}

pub fn iso8601(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

pub fn merge_metadata(current: &JsonMap, patch: &JsonMap) -> JsonMap {
    let mut merged = current.clone();

    for (key, value) in patch {
        merged.insert(key.clone(), value.clone());
    }

    merged
}

pub fn parse_query_scalar(raw: &str) -> Value {
    if let Ok(boolean) = raw.parse::<bool>() {
        return Value::Bool(boolean);
    }

    if let Ok(integer) = raw.parse::<i64>() {
        return Value::Number(integer.into());
    }

    if let Ok(float) = raw.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(float) {
            return Value::Number(number);
        }
    }

    Value::String(raw.to_string())
}

fn creator_principal_from_row(row: &SessionRow) -> Result<Option<Principal>, AppError> {
    match (&row.creator_id, &row.creator_type) {
        (Some(id), Some(principal_type)) => Ok(Some(Principal {
            tenant_id: row.tenant_id.clone(),
            id: id.clone(),
            principal_type: normalize_principal_type(principal_type)?,
        })),
        (None, None) => Ok(None),
        _ => Err(AppError::Internal),
    }
}

pub(crate) fn normalize_principal_type(raw: &str) -> Result<String, AppError> {
    match raw {
        "user" | "agent" | "service" => Ok(raw.to_string()),
        "svc" => Ok("service".to_string()),
        _ => Err(AppError::InvalidSession),
    }
}

pub(crate) fn require_non_empty(value: String, error: AppError) -> Result<String, AppError> {
    if value.is_empty() {
        Err(error)
    } else {
        Ok(value)
    }
}

pub(crate) fn optional_object(
    value: Option<Value>,
    error: AppError,
    allow_null_as_default: bool,
) -> Result<JsonMap, AppError> {
    match value {
        None => Ok(JsonMap::new()),
        Some(Value::Null) if allow_null_as_default => Ok(JsonMap::new()),
        Some(value) => value_to_object(value, error),
    }
}

pub(crate) fn require_object(value: Value, error: AppError) -> Result<JsonMap, AppError> {
    value_to_object(value, error)
}

pub(crate) fn value_to_object(value: Value, error: AppError) -> Result<JsonMap, AppError> {
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CreateSessionRequest, LifecycleEvent, LifecycleResponse, Principal, SessionResponse,
        UpdateSessionRequest, merge_metadata, parse_query_scalar,
    };

    #[test]
    fn create_defaults_to_service_principal() {
        let request = CreateSessionRequest {
            id: None,
            title: Some("Draft".to_string()),
            metadata: Some(json!({"workflow": "legal"})),
            tenant_id: Some("acme".to_string()),
            creator_principal: None,
        };

        let validated = request.validate().expect("request should validate");

        assert_eq!(validated.tenant_id, "acme");
        assert_eq!(
            validated.creator_principal,
            Principal {
                tenant_id: "acme".to_string(),
                id: "service".to_string(),
                principal_type: "service".to_string(),
            }
        );
    }

    #[test]
    fn update_rejects_metadata_only_noop() {
        let request = UpdateSessionRequest {
            title: None,
            metadata: Some(json!({})),
            expected_version: None,
        };

        assert!(request.validate().is_err());
    }

    #[test]
    fn metadata_merge_is_shallow() {
        let current = json!({"workflow": "contract", "tags": ["one"]})
            .as_object()
            .cloned()
            .expect("object");
        let patch = json!({"summary": "ready", "tags": ["one", "two"]})
            .as_object()
            .cloned()
            .expect("object");

        let merged = merge_metadata(&current, &patch);

        assert_eq!(
            merged,
            json!({
                "workflow": "contract",
                "summary": "ready",
                "tags": ["one", "two"]
            })
            .as_object()
            .cloned()
            .expect("object")
        );
    }

    #[test]
    fn query_scalar_parser_preserves_booleans_and_numbers() {
        assert_eq!(parse_query_scalar("true"), json!(true));
        assert_eq!(parse_query_scalar("42"), json!(42));
        assert_eq!(parse_query_scalar("3.5"), json!(3.5));
        assert_eq!(parse_query_scalar("ses_demo"), json!("ses_demo"));
    }

    #[test]
    fn lifecycle_created_keeps_null_title_and_header_fields() {
        let event = LifecycleEvent::created("acme".to_string(), &sample_session(None, false));
        let value = serde_json::to_value(event).expect("event should serialize");

        assert_eq!(value["kind"], "session.created");
        assert_eq!(value["session_id"], "ses_demo");
        assert_eq!(value["tenant_id"], "acme");
        assert!(value["title"].is_null());
        assert_eq!(value["metadata"], json!({"workflow": "contract"}));
        assert_eq!(value["version"], 1);
        assert_eq!(value["created_at"], "2026-04-11T00:00:00.000000Z");
        assert!(value.get("updated_at").is_none());
    }

    #[test]
    fn lifecycle_archive_is_minimal_and_explicit() {
        let event =
            LifecycleEvent::archived("acme".to_string(), &sample_session(Some("Draft"), true));
        let value = serde_json::to_value(event).expect("event should serialize");

        assert_eq!(value["kind"], "session.archived");
        assert_eq!(value["session_id"], "ses_demo");
        assert_eq!(value["tenant_id"], "acme");
        assert_eq!(value["archived"], true);
        assert!(value.get("metadata").is_none());
        assert!(value.get("title").is_none());
    }

    #[test]
    fn lifecycle_runtime_states_are_minimal() {
        let event = LifecycleEvent::freezing("ses_demo".to_string(), "acme".to_string());
        let value = serde_json::to_value(event).expect("event should serialize");

        assert_eq!(value["kind"], "session.freezing");
        assert_eq!(value["session_id"], "ses_demo");
        assert_eq!(value["tenant_id"], "acme");
        assert!(value.get("metadata").is_none());
        assert!(value.get("title").is_none());
        assert!(value.get("archived").is_none());
    }

    #[test]
    fn lifecycle_response_wraps_cursor_and_event() {
        let response = LifecycleResponse {
            cursor: 12,
            inserted_at: "2026-04-11T00:00:00.000000Z".to_string(),
            event: LifecycleEvent::activated("ses_demo".to_string(), "acme".to_string()),
        };
        let value = serde_json::to_value(response).expect("response should serialize");

        assert_eq!(value["cursor"], 12);
        assert_eq!(value["event"]["kind"], "session.activated");
    }

    fn sample_session(title: Option<&str>, archived: bool) -> SessionResponse {
        SessionResponse {
            id: "ses_demo".to_string(),
            title: title.map(str::to_string),
            creator_principal: None,
            metadata: json!({"workflow": "contract"})
                .as_object()
                .cloned()
                .expect("object"),
            last_seq: 0,
            created_at: "2026-04-11T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-11T00:00:00.000000Z".to_string(),
            version: 1,
            archived,
        }
    }
}
