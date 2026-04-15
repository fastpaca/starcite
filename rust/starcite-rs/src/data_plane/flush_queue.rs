use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use serde::Serialize;
use tokio::sync::{Mutex, Notify};

use crate::model::EventResponse;

#[derive(Debug, Clone, Default)]
pub struct PendingFlushQueue {
    inner: Arc<PendingFlushQueueInner>,
}

#[derive(Debug, Default)]
struct PendingFlushQueueInner {
    pending_sessions: Mutex<BTreeSet<String>>,
    events: Mutex<HashMap<String, BTreeMap<i64, EventResponse>>>,
    notify: Notify,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PendingFlushSnapshot {
    pub pending_session_count: usize,
    pub pending_event_count: usize,
    pub sessions: Vec<PendingFlushSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PendingFlushSessionSnapshot {
    pub session_id: String,
    pub event_count: usize,
    pub first_seq: Option<i64>,
    pub last_seq: Option<i64>,
}

impl PendingFlushQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn enqueue(&self, event: EventResponse) {
        let session_id = event.session_id.clone();
        {
            let mut events = self.inner.events.lock().await;
            events
                .entry(session_id.clone())
                .or_insert_with(BTreeMap::new)
                .insert(event.seq, event);
        }

        let mut pending_sessions = self.inner.pending_sessions.lock().await;
        let inserted = pending_sessions.insert(session_id);
        drop(pending_sessions);

        if inserted {
            self.inner.notify.notify_one();
        }
    }

    pub async fn drain_sessions(&self) -> Vec<String> {
        let mut pending_sessions = self.inner.pending_sessions.lock().await;
        let sessions = pending_sessions.iter().cloned().collect::<Vec<_>>();
        pending_sessions.clear();
        sessions
    }

    pub async fn session_events(&self, session_id: &str) -> Vec<EventResponse> {
        let events = self.inner.events.lock().await;
        events
            .get(session_id)
            .map(|session| session.values().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn mark_flushed(&self, session_id: &str, seq: i64) {
        let has_remaining = {
            let mut events = self.inner.events.lock().await;
            let Some(session_events) = events.get_mut(session_id) else {
                return;
            };

            let flushed = session_events
                .keys()
                .copied()
                .take_while(|candidate| *candidate <= seq)
                .collect::<Vec<_>>();

            for flushed_seq in flushed {
                session_events.remove(&flushed_seq);
            }

            if session_events.is_empty() {
                events.remove(session_id);
                false
            } else {
                true
            }
        };

        if has_remaining {
            let mut pending_sessions = self.inner.pending_sessions.lock().await;
            pending_sessions.insert(session_id.to_string());
        }
    }

    pub async fn requeue_session(&self, session_id: &str) {
        let events = self.inner.events.lock().await;
        if !events.contains_key(session_id) {
            return;
        }
        drop(events);

        let mut pending_sessions = self.inner.pending_sessions.lock().await;
        pending_sessions.insert(session_id.to_string());
    }

    pub async fn wait(&self) {
        self.inner.notify.notified().await;
    }

    pub async fn snapshot(&self) -> PendingFlushSnapshot {
        let events = self.inner.events.lock().await;
        let mut pending_event_count = 0_usize;
        let mut sessions = events
            .iter()
            .map(|(session_id, session_events)| {
                pending_event_count += session_events.len();

                PendingFlushSessionSnapshot {
                    session_id: session_id.clone(),
                    event_count: session_events.len(),
                    first_seq: session_events.first_key_value().map(|(seq, _event)| *seq),
                    last_seq: session_events.last_key_value().map(|(seq, _event)| *seq),
                }
            })
            .collect::<Vec<_>>();

        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        PendingFlushSnapshot {
            pending_session_count: sessions.len(),
            pending_event_count,
            sessions,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PendingFlushQueue;
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
            epoch: None,
            cursor: seq,
        }
    }

    #[tokio::test]
    async fn snapshot_reports_pending_events() {
        let queue = PendingFlushQueue::new();
        queue.enqueue(event("ses_b", 2)).await;
        queue.enqueue(event("ses_b", 3)).await;
        queue.enqueue(event("ses_a", 1)).await;

        let snapshot = queue.snapshot().await;

        assert_eq!(snapshot.pending_session_count, 2);
        assert_eq!(snapshot.pending_event_count, 3);
        assert_eq!(snapshot.sessions[0].session_id, "ses_a");
        assert_eq!(snapshot.sessions[1].first_seq, Some(2));
        assert_eq!(snapshot.sessions[1].last_seq, Some(3));
    }

    #[tokio::test]
    async fn mark_flushed_removes_only_flushed_prefix() {
        let queue = PendingFlushQueue::new();
        queue.enqueue(event("ses_demo", 1)).await;
        queue.enqueue(event("ses_demo", 2)).await;

        assert_eq!(queue.drain_sessions().await, vec!["ses_demo".to_string()]);

        queue.mark_flushed("ses_demo", 1).await;
        let snapshot = queue.snapshot().await;

        assert_eq!(snapshot.pending_event_count, 1);
        assert_eq!(snapshot.sessions[0].first_seq, Some(2));
    }
}
