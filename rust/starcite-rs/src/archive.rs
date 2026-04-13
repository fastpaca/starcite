use std::time::Duration;

use sqlx::PgPool;
use tokio::time::sleep;

use crate::{error::AppError, hot_store::HotEventStore, repository};

#[derive(Debug, Clone)]
pub struct ArchiveWorker {
    pool: PgPool,
    hot_store: HotEventStore,
    flush_interval: Duration,
}

impl ArchiveWorker {
    pub fn new(pool: PgPool, hot_store: HotEventStore, flush_interval: Duration) -> Self {
        Self {
            pool,
            hot_store,
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
            sleep(self.flush_interval).await;
            self.flush_once().await;
        }
    }

    async fn flush_once(&self) {
        let session_ids = self.hot_store.session_ids().await;

        for session_id in session_ids {
            if let Err(error) = self.flush_session(&session_id).await {
                tracing::warn!(error = ?error, session_id, "archive flush tick failed");
            }
        }
    }

    async fn flush_session(&self, session_id: &str) -> Result<(), AppError> {
        let Some(max_hot_seq) = self.hot_store.max_seq(session_id).await else {
            return Ok(());
        };

        let state = repository::get_archive_state(&self.pool, session_id).await?;
        let Some(target_seq) =
            next_flush_target(state.archived_seq, state.last_seq, Some(max_hot_seq))
        else {
            return Ok(());
        };

        let archived_seq =
            repository::mark_archived_seq(&self.pool, session_id, target_seq).await?;
        let prune_floor = archived_seq.saturating_add(1);
        let deleted = self.hot_store.delete_below(session_id, prune_floor).await;

        tracing::debug!(
            session_id,
            archived_seq,
            deleted_hot_events = deleted,
            "archive worker pruned local hot events"
        );

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
