use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use serde::Serialize;
use tokio::sync::RwLock;

use crate::model::EventResponse;

#[derive(Debug, Clone, Default)]
pub struct HotEventStore {
    sessions: Arc<RwLock<HashMap<String, SessionBuffer>>>,
}

#[derive(Debug, Clone, Default)]
struct SessionBuffer {
    events: BTreeMap<i64, EventResponse>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HotEventStoreSnapshot {
    pub session_count: usize,
    pub event_count: usize,
    pub sessions: Vec<HotSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HotSessionSnapshot {
    pub session_id: String,
    pub event_count: usize,
    pub first_seq: Option<i64>,
    pub last_seq: Option<i64>,
}

impl HotEventStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn put_event(&self, event: EventResponse) {
        let mut sessions = self.sessions.write().await;
        let buffer = sessions
            .entry(event.session_id.clone())
            .or_insert_with(SessionBuffer::default);

        buffer.events.insert(event.seq, event);
    }

    pub async fn put_events<I>(&self, events: I)
    where
        I: IntoIterator<Item = EventResponse>,
    {
        let mut sessions = self.sessions.write().await;

        for event in events {
            let buffer = sessions
                .entry(event.session_id.clone())
                .or_insert_with(SessionBuffer::default);

            buffer.events.insert(event.seq, event);
        }
    }

    pub async fn from_cursor(
        &self,
        session_id: &str,
        cursor: i64,
        limit: u32,
    ) -> Vec<EventResponse> {
        let sessions = self.sessions.read().await;
        let Some(buffer) = sessions.get(session_id) else {
            return Vec::new();
        };

        buffer
            .events
            .range((cursor + 1)..)
            .take(limit as usize)
            .map(|(_seq, event)| event.clone())
            .collect()
    }

    pub async fn delete_below(&self, session_id: &str, floor_seq: i64) -> usize {
        let mut sessions = self.sessions.write().await;
        let Some(buffer) = sessions.get_mut(session_id) else {
            return 0;
        };

        let stale = buffer
            .events
            .keys()
            .copied()
            .take_while(|seq| *seq < floor_seq)
            .collect::<Vec<_>>();

        for seq in &stale {
            buffer.events.remove(seq);
        }

        if buffer.events.is_empty() {
            sessions.remove(session_id);
        }

        stale.len()
    }

    pub async fn max_seq(&self, session_id: &str) -> Option<i64> {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .and_then(|buffer| buffer.events.last_key_value().map(|(seq, _event)| *seq))
    }

    pub async fn session_ids(&self) -> Vec<String> {
        let sessions = self.sessions.read().await;
        let mut ids = sessions.keys().cloned().collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub async fn snapshot(&self) -> HotEventStoreSnapshot {
        let sessions = self.sessions.read().await;
        let mut total_events = 0_usize;
        let mut snapshot = sessions
            .iter()
            .map(|(session_id, buffer)| {
                total_events += buffer.events.len();
                HotSessionSnapshot {
                    session_id: session_id.clone(),
                    event_count: buffer.events.len(),
                    first_seq: buffer.events.first_key_value().map(|(seq, _event)| *seq),
                    last_seq: buffer.events.last_key_value().map(|(seq, _event)| *seq),
                }
            })
            .collect::<Vec<_>>();

        snapshot.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        HotEventStoreSnapshot {
            session_count: snapshot.len(),
            event_count: total_events,
            sessions: snapshot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HotEventStore;
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
    async fn returns_ordered_events_after_cursor() {
        let store = HotEventStore::new();
        store.put_event(event("ses_demo", 2)).await;
        store.put_event(event("ses_demo", 1)).await;
        store.put_event(event("ses_demo", 3)).await;

        let events = store.from_cursor("ses_demo", 1, 10).await;
        let seqs = events
            .into_iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>();

        assert_eq!(seqs, vec![2, 3]);
    }

    #[tokio::test]
    async fn delete_below_prunes_session_when_empty() {
        let store = HotEventStore::new();
        store.put_event(event("ses_demo", 1)).await;
        store.put_event(event("ses_demo", 2)).await;

        assert_eq!(store.delete_below("ses_demo", 3).await, 2);
        assert_eq!(store.session_ids().await, Vec::<String>::new());
    }

    #[tokio::test]
    async fn snapshot_reports_counts_and_bounds() {
        let store = HotEventStore::new();
        store.put_event(event("ses_a", 2)).await;
        store.put_event(event("ses_a", 4)).await;
        store.put_event(event("ses_b", 1)).await;

        let snapshot = store.snapshot().await;

        assert_eq!(snapshot.session_count, 2);
        assert_eq!(snapshot.event_count, 3);
        assert_eq!(snapshot.sessions[0].session_id, "ses_a");
        assert_eq!(snapshot.sessions[0].first_seq, Some(2));
        assert_eq!(snapshot.sessions[0].last_seq, Some(4));
        assert_eq!(snapshot.sessions[1].session_id, "ses_b");
        assert_eq!(snapshot.sessions[1].first_seq, Some(1));
        assert_eq!(snapshot.sessions[1].last_seq, Some(1));
    }
}
