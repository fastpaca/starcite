use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tokio::time::sleep;

use super::{
    archive_queue::ArchiveQueue, hot_store::HotEventStore, repository,
    session_store::HotSessionStore,
};
use crate::{cluster::OwnershipManager, error::AppError, runtime::SessionManager};

#[derive(Clone)]
pub struct ArchiveWorker {
    pool: PgPool,
    hot_store: HotEventStore,
    session_store: HotSessionStore,
    queue: ArchiveQueue,
    ownership: OwnershipManager,
    session_manager: SessionManager,
    flush_interval: Duration,
    instance_id: Arc<str>,
}

impl ArchiveWorker {
    pub fn new(
        pool: PgPool,
        hot_store: HotEventStore,
        session_store: HotSessionStore,
        queue: ArchiveQueue,
        ownership: OwnershipManager,
        session_manager: SessionManager,
        flush_interval: Duration,
        instance_id: Arc<str>,
    ) -> Self {
        Self {
            pool,
            hot_store,
            session_store,
            queue,
            ownership,
            session_manager,
            flush_interval,
            instance_id,
        }
    }

    pub fn spawn(self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    async fn run(self) {
        loop {
            tokio::select! {
                _ = sleep(self.flush_interval) => {},
                _ = self.queue.wait() => {},
            }
            self.flush_once().await;
        }
    }

    async fn flush_once(&self) {
        let session_ids = self.queue.drain().await;

        for session_id in session_ids {
            if let Err(error) = self.flush_session(&session_id).await {
                tracing::warn!(error = ?error, session_id, "archive flush tick failed");
                self.queue.enqueue(&session_id).await;
            }
        }
    }

    async fn flush_session(&self, session_id: &str) -> Result<(), AppError> {
        if self.ownership.owned_epoch(session_id).await.is_none() {
            return Ok(());
        }

        let Some(max_hot_seq) = self.hot_store.max_seq(session_id).await else {
            return Ok(());
        };

        let local_archived_seq = self
            .session_store
            .get_archived_seq(session_id)
            .await
            .unwrap_or(0);
        let state = repository::get_archive_state(&self.pool, session_id).await?;
        let Some(target_seq) =
            next_flush_target(local_archived_seq, state.last_seq, Some(max_hot_seq))
        else {
            return Ok(());
        };

        let archived_seq = if state.archived_seq < target_seq {
            repository::mark_archived_seq(&self.pool, session_id, target_seq).await?
        } else {
            state.archived_seq
        };

        self.session_manager
            .ack_archived(session_id, &state.tenant_id, archived_seq)
            .await?;

        if let Err(error) = repository::publish_archive_progress(
            &self.pool,
            self.instance_id.as_ref(),
            session_id,
            &state.tenant_id,
            archived_seq,
        )
        .await
        {
            tracing::warn!(error = ?error, session_id, archived_seq, "failed to publish archive frontier");
        }

        Ok(())
    }
}

fn next_flush_target(archived_seq: i64, last_seq: i64, max_hot_seq: Option<i64>) -> Option<i64> {
    let max_hot_seq = max_hot_seq?;
    let capped = max_hot_seq.min(last_seq);

    if capped <= archived_seq {
        None
    } else {
        Some(capped)
    }
}

#[cfg(test)]
mod tests {
    use super::next_flush_target;

    #[test]
    fn skips_when_no_hot_events_exist() {
        assert_eq!(next_flush_target(0, 10, None), None);
    }

    #[test]
    fn skips_when_archive_is_already_caught_up() {
        assert_eq!(next_flush_target(5, 5, Some(5)), None);
        assert_eq!(next_flush_target(5, 10, Some(5)), None);
    }

    #[test]
    fn caps_archive_progress_at_session_last_seq() {
        assert_eq!(next_flush_target(2, 4, Some(9)), Some(4));
    }

    #[test]
    fn advances_to_latest_hot_seq_when_it_is_newer_than_archive() {
        assert_eq!(next_flush_target(2, 10, Some(7)), Some(7));
    }
}
