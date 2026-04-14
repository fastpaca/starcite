use std::{collections::BTreeSet, sync::Arc};

use serde::Serialize;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Default)]
pub struct ArchiveQueue {
    inner: Arc<ArchiveQueueInner>,
}

#[derive(Debug, Default)]
struct ArchiveQueueInner {
    pending: Mutex<BTreeSet<String>>,
    notify: Notify,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ArchiveQueueSnapshot {
    pub pending_session_count: usize,
    pub sessions: Vec<String>,
}

impl ArchiveQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn enqueue(&self, session_id: &str) {
        let mut pending = self.inner.pending.lock().await;
        let inserted = pending.insert(session_id.to_string());
        drop(pending);

        if inserted {
            self.inner.notify.notify_one();
        }
    }

    pub async fn drain(&self) -> Vec<String> {
        let mut pending = self.inner.pending.lock().await;
        let sessions = pending.iter().cloned().collect::<Vec<_>>();
        pending.clear();
        sessions
    }

    pub async fn wait(&self) {
        self.inner.notify.notified().await;
    }

    pub async fn snapshot(&self) -> ArchiveQueueSnapshot {
        let pending = self.inner.pending.lock().await;
        ArchiveQueueSnapshot {
            pending_session_count: pending.len(),
            sessions: pending.iter().cloned().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArchiveQueue;

    #[tokio::test]
    async fn dedupes_pending_sessions_and_drains_in_order() {
        let queue = ArchiveQueue::new();

        queue.enqueue("ses_b").await;
        queue.enqueue("ses_a").await;
        queue.enqueue("ses_b").await;

        assert_eq!(
            queue.drain().await,
            vec!["ses_a".to_string(), "ses_b".to_string()]
        );
        assert!(queue.drain().await.is_empty());
    }

    #[tokio::test]
    async fn snapshot_reports_pending_sessions() {
        let queue = ArchiveQueue::new();

        queue.enqueue("ses_b").await;
        queue.enqueue("ses_a").await;

        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.pending_session_count, 2);
        assert_eq!(
            snapshot.sessions,
            vec!["ses_a".to_string(), "ses_b".to_string()]
        );
    }
}
