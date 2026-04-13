use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::RwLock;

use crate::{
    error::AppError,
    model::{LifecycleEvent, SessionResponse},
    repository,
};

#[derive(Debug, Clone, Default)]
pub struct HotSessionStore {
    sessions: Arc<RwLock<HashMap<String, SessionCacheEntry>>>,
}

#[derive(Debug, Clone)]
struct SessionCacheEntry {
    tenant_id: String,
    last_seq: i64,
    session: Option<SessionResponse>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HotSessionStoreSnapshot {
    pub entry_count: usize,
    pub full_session_count: usize,
    pub partial_session_count: usize,
    pub sessions: Vec<HotSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HotSessionSnapshot {
    pub session_id: String,
    pub tenant_id: String,
    pub last_seq: i64,
    pub has_session: bool,
    pub archived: Option<bool>,
    pub version: Option<i64>,
}

impl HotSessionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put_session(&self, tenant_id: &str, mut session: SessionResponse) {
        let mut sessions = self.sessions.write().await;
        let entry = sessions
            .entry(session.id.clone())
            .or_insert_with(|| SessionCacheEntry {
                tenant_id: tenant_id.to_string(),
                last_seq: session.last_seq,
                session: None,
            });

        entry.tenant_id = tenant_id.to_string();
        entry.last_seq = entry.last_seq.max(session.last_seq);
        session.last_seq = entry.last_seq;
        entry.session = Some(session);
    }

    pub async fn put_tenant(&self, session_id: &str, tenant_id: &str) {
        let mut sessions = self.sessions.write().await;
        let entry = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionCacheEntry {
                tenant_id: tenant_id.to_string(),
                last_seq: 0,
                session: None,
            });

        entry.tenant_id = tenant_id.to_string();
    }

    pub async fn bump_last_seq(&self, session_id: &str, tenant_id: &str, last_seq: i64) {
        let mut sessions = self.sessions.write().await;
        let entry = sessions
            .entry(session_id.to_string())
            .or_insert_with(|| SessionCacheEntry {
                tenant_id: tenant_id.to_string(),
                last_seq,
                session: None,
            });

        entry.tenant_id = tenant_id.to_string();
        entry.last_seq = entry.last_seq.max(last_seq);

        if let Some(session) = entry.session.as_mut() {
            session.last_seq = session.last_seq.max(last_seq);
        }
    }

    pub async fn apply_lifecycle_hint(&self, event: &LifecycleEvent) {
        let mut sessions = self.sessions.write().await;
        let entry = sessions
            .entry(event.session_id().to_string())
            .or_insert_with(|| SessionCacheEntry {
                tenant_id: event.tenant_id().to_string(),
                last_seq: 0,
                session: None,
            });

        entry.tenant_id = event.tenant_id().to_string();

        let Some(session) = entry.session.as_mut() else {
            return;
        };

        match event {
            LifecycleEvent::SessionCreated {
                title,
                metadata,
                created_at,
                version,
                ..
            } => {
                session.title = title.clone();
                session.metadata = metadata.clone();
                session.created_at = created_at.clone();
                session.updated_at = created_at.clone();
                session.version = *version;
                session.archived = false;
            }
            LifecycleEvent::SessionUpdated {
                title,
                metadata,
                updated_at,
                version,
                ..
            } => {
                session.title = title.clone();
                session.metadata = metadata.clone();
                session.updated_at = updated_at.clone();
                session.version = *version;
            }
            LifecycleEvent::SessionArchived { archived, .. }
            | LifecycleEvent::SessionUnarchived { archived, .. } => {
                session.archived = *archived;
            }
            LifecycleEvent::SessionActivated { .. }
            | LifecycleEvent::SessionHydrating { .. }
            | LifecycleEvent::SessionFreezing { .. }
            | LifecycleEvent::SessionFrozen { .. } => {}
        }
    }

    pub async fn get_session(&self, session_id: &str) -> Option<SessionResponse> {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .and_then(|entry| entry.session.clone())
    }

    pub async fn get_tenant_id(&self, session_id: &str) -> Option<String> {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .map(|entry| entry.tenant_id.clone())
    }

    pub async fn get_last_seq(&self, session_id: &str) -> Option<i64> {
        let sessions = self.sessions.read().await;
        sessions.get(session_id).map(|entry| entry.last_seq)
    }

    pub async fn snapshot(&self) -> HotSessionStoreSnapshot {
        let sessions = self.sessions.read().await;
        let mut full_session_count = 0_usize;
        let mut snapshot = sessions
            .iter()
            .map(|(session_id, entry)| {
                if entry.session.is_some() {
                    full_session_count += 1;
                }

                HotSessionSnapshot {
                    session_id: session_id.clone(),
                    tenant_id: entry.tenant_id.clone(),
                    last_seq: entry.last_seq,
                    has_session: entry.session.is_some(),
                    archived: entry.session.as_ref().map(|session| session.archived),
                    version: entry.session.as_ref().map(|session| session.version),
                }
            })
            .collect::<Vec<_>>();

        snapshot.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        HotSessionStoreSnapshot {
            entry_count: snapshot.len(),
            full_session_count,
            partial_session_count: snapshot.len().saturating_sub(full_session_count),
            sessions: snapshot,
        }
    }
}

pub async fn resolve_session(
    store: &HotSessionStore,
    pool: &PgPool,
    session_id: &str,
) -> Result<SessionResponse, AppError> {
    if let Some(session) = store.get_session(session_id).await {
        return Ok(session);
    }

    let snapshot = repository::get_session_snapshot(pool, session_id).await?;
    let session = snapshot.session.clone();
    store
        .put_session(&snapshot.tenant_id, snapshot.session)
        .await;
    Ok(session)
}

pub async fn resolve_session_tenant_id(
    store: &HotSessionStore,
    pool: &PgPool,
    session_id: &str,
) -> Result<String, AppError> {
    if let Some(tenant_id) = store.get_tenant_id(session_id).await {
        return Ok(tenant_id);
    }

    let snapshot = repository::get_session_snapshot(pool, session_id).await?;
    let tenant_id = snapshot.tenant_id.clone();
    store.put_session(&tenant_id, snapshot.session).await;
    Ok(tenant_id)
}

pub async fn resolve_session_last_seq(
    store: &HotSessionStore,
    pool: &PgPool,
    session_id: &str,
) -> Result<i64, AppError> {
    if let Some(last_seq) = store.get_last_seq(session_id).await {
        return Ok(last_seq);
    }

    let snapshot = repository::get_session_snapshot(pool, session_id).await?;
    let last_seq = snapshot.session.last_seq;
    store
        .put_session(&snapshot.tenant_id, snapshot.session)
        .await;
    Ok(last_seq)
}

pub async fn refresh_from_lifecycle(
    store: &HotSessionStore,
    pool: &PgPool,
    event: &crate::model::LifecycleResponse,
) -> Result<(), AppError> {
    store.apply_lifecycle_hint(&event.event).await;

    if is_catalog_event(&event.event) {
        let snapshot = repository::get_session_snapshot(pool, event.event.session_id()).await?;
        store
            .put_session(&snapshot.tenant_id, snapshot.session)
            .await;
    }

    Ok(())
}

fn is_catalog_event(event: &LifecycleEvent) -> bool {
    matches!(
        event,
        LifecycleEvent::SessionCreated { .. }
            | LifecycleEvent::SessionUpdated { .. }
            | LifecycleEvent::SessionArchived { .. }
            | LifecycleEvent::SessionUnarchived { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::HotSessionStore;
    use crate::model::{LifecycleEvent, Principal, SessionResponse};
    use serde_json::Map;

    fn sample_session() -> SessionResponse {
        SessionResponse {
            id: "ses_demo".to_string(),
            title: Some("Draft".to_string()),
            creator_principal: Some(Principal {
                tenant_id: "acme".to_string(),
                id: "user-42".to_string(),
                principal_type: "user".to_string(),
            }),
            metadata: Map::new(),
            last_seq: 0,
            created_at: "2026-04-13T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-13T00:00:00.000000Z".to_string(),
            version: 1,
            archived: false,
        }
    }

    #[tokio::test]
    async fn bump_last_seq_updates_cached_full_session() {
        let store = HotSessionStore::new();
        store.put_session("acme", sample_session()).await;

        store.bump_last_seq("ses_demo", "acme", 4).await;

        let session = store
            .get_session("ses_demo")
            .await
            .expect("session should stay cached");
        assert_eq!(session.last_seq, 4);
    }

    #[tokio::test]
    async fn lifecycle_hint_updates_existing_cached_session() {
        let store = HotSessionStore::new();
        store.put_session("acme", sample_session()).await;

        store
            .apply_lifecycle_hint(&LifecycleEvent::SessionUpdated {
                session_id: "ses_demo".to_string(),
                tenant_id: "acme".to_string(),
                title: Some("Renamed".to_string()),
                metadata: {
                    let mut metadata = Map::new();
                    metadata.insert("phase".to_string(), "review".into());
                    metadata
                },
                updated_at: "2026-04-13T00:01:00.000000Z".to_string(),
                version: 2,
            })
            .await;

        let session = store
            .get_session("ses_demo")
            .await
            .expect("session should stay cached");
        assert_eq!(session.title.as_deref(), Some("Renamed"));
        assert_eq!(session.version, 2);
        assert_eq!(
            session
                .metadata
                .get("phase")
                .and_then(|value| value.as_str()),
            Some("review")
        );
    }

    #[tokio::test]
    async fn snapshot_reports_full_and_partial_entries() {
        let store = HotSessionStore::new();
        store.put_session("acme", sample_session()).await;
        store.put_tenant("ses_other", "acme").await;
        store.bump_last_seq("ses_other", "acme", 3).await;

        let snapshot = store.snapshot().await;

        assert_eq!(snapshot.entry_count, 2);
        assert_eq!(snapshot.full_session_count, 1);
        assert_eq!(snapshot.partial_session_count, 1);
        assert_eq!(snapshot.sessions[0].session_id, "ses_demo");
        assert!(snapshot.sessions[0].has_session);
        assert_eq!(snapshot.sessions[1].session_id, "ses_other");
        assert_eq!(snapshot.sessions[1].last_seq, 3);
        assert!(!snapshot.sessions[1].has_session);
    }
}
