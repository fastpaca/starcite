use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use tokio::sync::RwLock;

use crate::model::EventResponse;

#[derive(Debug, Clone, Default)]
pub struct ReplicaStore {
    sessions: Arc<RwLock<HashMap<String, PendingReplica>>>,
}

#[derive(Debug, Clone)]
struct PendingReplica {
    owner_id: String,
    epoch: i64,
    event: EventResponse,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicaStoreSnapshot {
    pub pending_session_count: usize,
    pub sessions: Vec<ReplicaSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicaSessionSnapshot {
    pub session_id: String,
    pub owner_id: String,
    pub epoch: i64,
    pub seq: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStoreError {
    StaleEpoch,
    ConflictingOwner,
    ConflictingSeq,
}

impl ReplicaStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn prepare(
        &self,
        owner_id: &str,
        epoch: i64,
        event: EventResponse,
    ) -> Result<(), ReplicaStoreError> {
        let mut sessions = self.sessions.write().await;

        match sessions.get(event.session_id.as_str()) {
            None => {}
            Some(existing) if epoch < existing.epoch => {
                return Err(ReplicaStoreError::StaleEpoch);
            }
            Some(existing) if epoch == existing.epoch && existing.owner_id != owner_id => {
                return Err(ReplicaStoreError::ConflictingOwner);
            }
            Some(existing) if epoch == existing.epoch && existing.event.seq != event.seq => {
                return Err(ReplicaStoreError::ConflictingSeq);
            }
            Some(_) => {}
        }

        sessions.insert(
            event.session_id.clone(),
            PendingReplica {
                owner_id: owner_id.to_string(),
                epoch,
                event,
            },
        );

        Ok(())
    }

    pub async fn commit(
        &self,
        owner_id: &str,
        epoch: i64,
        session_id: &str,
        seq: i64,
    ) -> Result<Option<EventResponse>, ReplicaStoreError> {
        let mut sessions = self.sessions.write().await;
        let Some(existing) = sessions.get(session_id) else {
            return Ok(None);
        };

        if epoch < existing.epoch {
            return Err(ReplicaStoreError::StaleEpoch);
        }

        if existing.owner_id != owner_id {
            return Err(ReplicaStoreError::ConflictingOwner);
        }

        if existing.epoch != epoch || existing.event.seq != seq {
            return Err(ReplicaStoreError::ConflictingSeq);
        }

        Ok(sessions.remove(session_id).map(|pending| pending.event))
    }

    pub async fn discard_before_epoch(&self, session_id: &str, epoch: i64) {
        let mut sessions = self.sessions.write().await;
        let should_remove = sessions
            .get(session_id)
            .is_some_and(|pending| pending.epoch < epoch);

        if should_remove {
            sessions.remove(session_id);
        }
    }

    pub async fn snapshot(&self) -> ReplicaStoreSnapshot {
        let sessions = self.sessions.read().await;
        let mut snapshot = sessions
            .iter()
            .map(|(session_id, pending)| ReplicaSessionSnapshot {
                session_id: session_id.clone(),
                owner_id: pending.owner_id.clone(),
                epoch: pending.epoch,
                seq: pending.event.seq,
            })
            .collect::<Vec<_>>();

        snapshot.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        ReplicaStoreSnapshot {
            pending_session_count: snapshot.len(),
            sessions: snapshot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReplicaStore, ReplicaStoreError};
    use crate::model::EventResponse;
    use serde_json::Map;

    fn event(session_id: &str, seq: i64) -> EventResponse {
        EventResponse {
            session_id: session_id.to_string(),
            seq,
            event_type: "content".to_string(),
            payload: Map::new(),
            actor: "service:bench".to_string(),
            source: Some("test".to_string()),
            metadata: Map::new(),
            refs: Map::new(),
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq: seq,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-13T00:00:00Z".to_string(),
            cursor: seq,
        }
    }

    #[tokio::test]
    async fn prepare_and_commit_round_trip() {
        let store = ReplicaStore::new();
        let prepared = event("ses_demo", 1);

        store
            .prepare("node-a", 1, prepared.clone())
            .await
            .expect("prepare");

        let committed = store
            .commit("node-a", 1, "ses_demo", 1)
            .await
            .expect("commit")
            .expect("pending event");

        assert_eq!(committed, prepared);
        assert_eq!(store.snapshot().await.pending_session_count, 0);
    }

    #[tokio::test]
    async fn newer_epoch_replaces_stale_prepare() {
        let store = ReplicaStore::new();
        store
            .prepare("node-a", 1, event("ses_demo", 1))
            .await
            .expect("prepare");
        store
            .prepare("node-b", 2, event("ses_demo", 1))
            .await
            .expect("replace");

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.sessions[0].owner_id, "node-b");
        assert_eq!(snapshot.sessions[0].epoch, 2);
    }

    #[tokio::test]
    async fn same_epoch_conflicting_seq_is_rejected() {
        let store = ReplicaStore::new();
        store
            .prepare("node-a", 1, event("ses_demo", 1))
            .await
            .expect("prepare");

        let error = store
            .prepare("node-a", 1, event("ses_demo", 2))
            .await
            .expect_err("conflicting seq");

        assert_eq!(error, ReplicaStoreError::ConflictingSeq);
    }

    #[tokio::test]
    async fn discard_before_epoch_prunes_stale_pending_replica() {
        let store = ReplicaStore::new();
        store
            .prepare("node-a", 7, event("ses_demo", 11))
            .await
            .expect("prepare");

        store.discard_before_epoch("ses_demo", 8).await;

        assert_eq!(store.snapshot().await.pending_session_count, 0);
    }
}
