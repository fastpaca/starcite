use std::{sync::Arc, time::Duration};

use sqlx::PgPool;
use tokio::time::sleep;

use crate::{
    archive_queue::ArchiveQueue, error::AppError, flush_queue::PendingFlushQueue, repository,
};

#[derive(Debug, Clone)]
pub struct FlushWorker {
    pool: PgPool,
    queue: PendingFlushQueue,
    archive_queue: ArchiveQueue,
    instance_id: Arc<str>,
    flush_interval: Duration,
}

impl FlushWorker {
    pub fn new(
        pool: PgPool,
        queue: PendingFlushQueue,
        archive_queue: ArchiveQueue,
        instance_id: Arc<str>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            pool,
            queue,
            archive_queue,
            instance_id,
            flush_interval,
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
        let session_ids = self.queue.drain_sessions().await;

        for session_id in session_ids {
            if let Err(error) = self.flush_session(&session_id).await {
                tracing::warn!(error = ?error, session_id, "pending flush tick failed");
                self.queue.requeue_session(&session_id).await;
            }
        }
    }

    async fn flush_session(&self, session_id: &str) -> Result<(), AppError> {
        let events = self.queue.session_events(session_id).await;

        if events.is_empty() {
            return Ok(());
        }

        let last_seq = events
            .last()
            .map(|event| event.seq)
            .ok_or(AppError::Internal)?;
        repository::persist_flushed_events(&self.pool, &events, &self.instance_id).await?;
        self.queue.mark_flushed(session_id, last_seq).await;
        self.archive_queue.enqueue(session_id).await;
        Ok(())
    }
}
