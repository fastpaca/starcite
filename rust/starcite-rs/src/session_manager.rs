use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use serde::Serialize;
use sqlx::PgPool;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::timeout,
};

use crate::{
    archive_queue::ArchiveQueue,
    config::CommitMode,
    error::AppError,
    fanout::SessionFanout,
    flush_queue::PendingFlushQueue,
    hot_store::HotEventStore,
    model::{AppendReply, EventResponse, ValidatedAppendEvent, iso8601},
    ownership::OwnershipManager,
    replication::ReplicationCoordinator,
    repository::{self, AppendOutcome, ProducerSequenceCheck},
    session_store::{HotSessionStore, resolve_session_last_seq},
};

const APPEND_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct SessionManager {
    workers: Arc<Mutex<HashMap<String, SessionWorkerHandle>>>,
    pool: PgPool,
    fanout: SessionFanout,
    hot_store: HotEventStore,
    archive_queue: ArchiveQueue,
    pending_flush: PendingFlushQueue,
    session_store: HotSessionStore,
    ownership: OwnershipManager,
    replication: ReplicationCoordinator,
    commit_mode: CommitMode,
    instance_id: Arc<str>,
    idle_timeout: Duration,
    next_worker_id: Arc<AtomicU64>,
}

#[derive(Clone)]
struct SessionWorkerHandle {
    worker_id: u64,
    sender: mpsc::Sender<AppendCommand>,
}

struct AppendCommand {
    tenant_id: String,
    input: ValidatedAppendEvent,
    reply_tx: oneshot::Sender<Result<AppendOutcome, AppError>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionManagerSnapshot {
    pub idle_timeout_ms: u64,
    pub active_session_count: usize,
    pub sessions: Vec<SessionWorkerSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionWorkerSnapshot {
    pub session_id: String,
    pub worker_id: u64,
}

impl SessionManager {
    pub fn new(
        pool: PgPool,
        fanout: SessionFanout,
        hot_store: HotEventStore,
        archive_queue: ArchiveQueue,
        pending_flush: PendingFlushQueue,
        session_store: HotSessionStore,
        ownership: OwnershipManager,
        replication: ReplicationCoordinator,
        commit_mode: CommitMode,
        instance_id: Arc<str>,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            pool,
            fanout,
            hot_store,
            archive_queue,
            pending_flush,
            session_store,
            ownership,
            replication,
            commit_mode,
            instance_id,
            idle_timeout,
            next_worker_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn append(
        &self,
        session_id: &str,
        tenant_id: &str,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let mut command = AppendCommand {
            tenant_id: tenant_id.to_string(),
            input,
            reply_tx,
        };

        for _attempt in 0..2 {
            let handle = self.worker_for(session_id).await;

            match handle.sender.send(command).await {
                Ok(()) => {
                    return reply_rx.await.unwrap_or(Err(AppError::Internal));
                }
                Err(error) => {
                    self.prune_worker(session_id, handle.worker_id).await;
                    command = error.0;
                }
            }
        }

        Err(AppError::Internal)
    }

    pub async fn snapshot(&self) -> SessionManagerSnapshot {
        let workers = self.workers.lock().await;
        let mut sessions = workers
            .iter()
            .map(|(session_id, handle)| SessionWorkerSnapshot {
                session_id: session_id.clone(),
                worker_id: handle.worker_id,
            })
            .collect::<Vec<_>>();

        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        SessionManagerSnapshot {
            idle_timeout_ms: self.idle_timeout.as_millis().min(u64::MAX as u128) as u64,
            active_session_count: sessions.len(),
            sessions,
        }
    }

    async fn worker_for(&self, session_id: &str) -> SessionWorkerHandle {
        let mut workers = self.workers.lock().await;

        if let Some(handle) = workers.get(session_id) {
            return handle.clone();
        }

        let worker_id = self.next_worker_id.fetch_add(1, Ordering::Relaxed);
        let (sender, receiver) = mpsc::channel(APPEND_QUEUE_CAPACITY);
        let handle = SessionWorkerHandle { worker_id, sender };
        workers.insert(session_id.to_string(), handle.clone());
        drop(workers);

        self.spawn_worker(session_id.to_string(), worker_id, receiver);
        handle
    }

    fn spawn_worker(
        &self,
        session_id: String,
        worker_id: u64,
        receiver: mpsc::Receiver<AppendCommand>,
    ) {
        let manager = self.clone();

        tokio::spawn(async move {
            manager.run_worker(session_id, worker_id, receiver).await;
        });
    }

    async fn run_worker(
        &self,
        session_id: String,
        worker_id: u64,
        mut receiver: mpsc::Receiver<AppendCommand>,
    ) {
        loop {
            let command = match timeout(self.idle_timeout, receiver.recv()).await {
                Ok(Some(command)) => command,
                Ok(None) | Err(_) => {
                    self.prune_worker(&session_id, worker_id).await;
                    return;
                }
            };

            let result = self
                .handle_append(&session_id, &command.tenant_id, command.input)
                .await;
            let _ = command.reply_tx.send(result);
        }
    }

    async fn handle_append(
        &self,
        session_id: &str,
        tenant_id: &str,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        match self.commit_mode {
            CommitMode::SyncPostgres => self.append_sync(session_id, input).await,
            CommitMode::LocalAsync => self.append_local_async(session_id, tenant_id, input).await,
        }
    }

    async fn append_sync(
        &self,
        session_id: &str,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        let outcome =
            repository::append_event(&self.pool, session_id, input, &self.instance_id).await?;

        self.session_store
            .bump_last_seq(session_id, &outcome.tenant_id, outcome.reply.last_seq)
            .await;

        if let Some(event) = outcome.event.clone() {
            self.hot_store.put_event(event.clone()).await;
            self.archive_queue.enqueue(session_id).await;
            self.fanout.broadcast(event).await;
        }

        Ok(outcome)
    }

    async fn append_local_async(
        &self,
        session_id: &str,
        tenant_id: &str,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        let lease = self.ownership.ensure_owned(session_id).await?;
        let last_seq =
            resolve_session_last_seq(&self.session_store, &self.pool, session_id).await?;

        if let Some(expected_seq) = input.expected_seq
            && last_seq != expected_seq
        {
            return Err(AppError::ExpectedSeqConflict {
                expected: expected_seq,
                current: last_seq,
            });
        }

        let last_producer_seq = self
            .resolve_producer_cursor(session_id, &input.producer_id, last_seq)
            .await?;

        match repository::classify_producer_sequence(last_producer_seq, input.producer_seq) {
            ProducerSequenceCheck::AcceptFirst | ProducerSequenceCheck::AcceptNext => {}
            ProducerSequenceCheck::ReplayOrConflict { expected } => {
                let producer_id = input.producer_id.clone();
                let current = input.producer_seq;
                let existing = self
                    .resolve_existing_event(session_id, &producer_id, current)
                    .await?;

                return match existing {
                    Some(existing) if matches_local_event(&existing, &input, tenant_id) => {
                        Ok(AppendOutcome {
                            reply: AppendReply {
                                seq: existing.seq,
                                last_seq,
                                deduped: true,
                                cursor: existing.cursor,
                                committed_cursor: last_seq,
                            },
                            event: None,
                            tenant_id: tenant_id.to_string(),
                        })
                    }
                    Some(_) => Err(AppError::ProducerReplayConflict),
                    None => Err(AppError::ProducerSeqConflict {
                        producer_id,
                        expected,
                        current,
                    }),
                };
            }
            ProducerSequenceCheck::Gap { expected } => {
                return Err(AppError::ProducerSeqConflict {
                    producer_id: input.producer_id,
                    expected,
                    current: input.producer_seq,
                });
            }
        }

        let next_seq = last_seq + 1;
        let inserted_at = iso8601(Utc::now());
        let event = EventResponse {
            session_id: session_id.to_string(),
            seq: next_seq,
            event_type: input.event_type,
            payload: input.payload,
            actor: input.actor,
            source: input.source,
            metadata: input.metadata,
            refs: input.refs,
            idempotency_key: input.idempotency_key,
            producer_id: input.producer_id,
            producer_seq: input.producer_seq,
            tenant_id: tenant_id.to_string(),
            inserted_at,
            cursor: next_seq,
        };

        self.replication
            .replicate(lease.epoch, &event, lease.standby.as_ref())
            .await?;
        self.apply_local_async_commit(event.clone()).await;

        Ok(AppendOutcome {
            reply: AppendReply {
                seq: next_seq,
                last_seq: next_seq,
                deduped: false,
                cursor: next_seq,
                committed_cursor: next_seq,
            },
            event: Some(event),
            tenant_id: tenant_id.to_string(),
        })
    }

    async fn resolve_producer_cursor(
        &self,
        session_id: &str,
        producer_id: &str,
        last_seq: i64,
    ) -> Result<Option<i64>, AppError> {
        if let Some(last_producer_seq) = self
            .hot_store
            .last_producer_seq(session_id, producer_id)
            .await
        {
            return Ok(Some(last_producer_seq));
        }

        if last_seq == 0 {
            return Ok(None);
        }

        repository::load_producer_cursor(&self.pool, session_id, producer_id).await
    }

    async fn resolve_existing_event(
        &self,
        session_id: &str,
        producer_id: &str,
        producer_seq: i64,
    ) -> Result<Option<EventResponse>, AppError> {
        if let Some(event) = self
            .hot_store
            .event_for_producer_seq(session_id, producer_id, producer_seq)
            .await
        {
            return Ok(Some(event));
        }

        repository::load_event_for_producer_seq(&self.pool, session_id, producer_id, producer_seq)
            .await
    }

    pub async fn apply_local_async_commit(&self, event: EventResponse) {
        self.session_store
            .put_tenant(&event.session_id, &event.tenant_id)
            .await;
        self.session_store
            .bump_last_seq(&event.session_id, &event.tenant_id, event.seq)
            .await;
        self.hot_store.put_event(event.clone()).await;
        self.pending_flush.enqueue(event.clone()).await;
        self.fanout.broadcast(event).await;
    }

    async fn prune_worker(&self, session_id: &str, worker_id: u64) {
        let removed = {
            let mut workers = self.workers.lock().await;

            if workers
                .get(session_id)
                .is_some_and(|handle| handle.worker_id == worker_id)
            {
                workers.remove(session_id).is_some()
            } else {
                false
            }
        };

        if removed && self.commit_mode == CommitMode::LocalAsync {
            self.ownership.release(session_id).await;
        }
    }
}

fn matches_local_event(
    existing: &EventResponse,
    input: &ValidatedAppendEvent,
    tenant_id: &str,
) -> bool {
    existing.event_type == input.event_type
        && existing.payload == input.payload
        && existing.actor == input.actor
        && existing.source == input.source
        && existing.metadata == input.metadata
        && existing.refs == input.refs
        && existing.idempotency_key == input.idempotency_key
        && existing.tenant_id == tenant_id
}

#[cfg(test)]
mod tests {
    use super::{SessionManager, SessionWorkerHandle};
    use crate::{
        archive_queue::ArchiveQueue, config::CommitMode, fanout::SessionFanout,
        flush_queue::PendingFlushQueue, hot_store::HotEventStore, ownership::OwnershipManager,
        replication::ReplicationCoordinator, session_store::HotSessionStore,
    };
    use sqlx::postgres::PgPoolOptions;
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::mpsc, time::sleep};

    fn manager(idle_timeout: Duration) -> SessionManager {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");

        SessionManager::new(
            pool.clone(),
            SessionFanout::default(),
            HotEventStore::new(),
            ArchiveQueue::new(),
            PendingFlushQueue::new(),
            HotSessionStore::new(),
            OwnershipManager::new(
                pool.clone(),
                Arc::<str>::from("node-a"),
                Duration::from_secs(5),
            ),
            ReplicationCoordinator::new(
                Arc::<str>::from("node-a"),
                false,
                None,
                Duration::from_millis(500),
            )
            .expect("replication"),
            CommitMode::SyncPostgres,
            Arc::<str>::from("node-a"),
            idle_timeout,
        )
    }

    #[tokio::test]
    async fn snapshot_sorts_active_sessions() {
        let manager = manager(Duration::from_secs(1));

        {
            let mut workers = manager.workers.lock().await;
            let (sender_a, _receiver_a) = mpsc::channel(1);
            let (sender_b, _receiver_b) = mpsc::channel(1);
            workers.insert(
                "ses_b".to_string(),
                SessionWorkerHandle {
                    worker_id: 2,
                    sender: sender_b,
                },
            );
            workers.insert(
                "ses_a".to_string(),
                SessionWorkerHandle {
                    worker_id: 1,
                    sender: sender_a,
                },
            );
        }

        let snapshot = manager.snapshot().await;

        assert_eq!(snapshot.active_session_count, 2);
        assert_eq!(snapshot.sessions[0].session_id, "ses_a");
        assert_eq!(snapshot.sessions[1].session_id, "ses_b");
    }

    #[tokio::test]
    async fn prune_worker_ignores_newer_replacement() {
        let manager = manager(Duration::from_secs(1));

        {
            let mut workers = manager.workers.lock().await;
            let (sender, _receiver) = mpsc::channel(1);
            workers.insert(
                "ses_demo".to_string(),
                SessionWorkerHandle {
                    worker_id: 2,
                    sender,
                },
            );
        }

        manager.prune_worker("ses_demo", 1).await;
        assert_eq!(manager.snapshot().await.active_session_count, 1);

        manager.prune_worker("ses_demo", 2).await;
        assert_eq!(manager.snapshot().await.active_session_count, 0);
    }

    #[tokio::test]
    async fn idle_worker_prunes_itself_without_commands() {
        let manager = manager(Duration::from_millis(20));
        let sender = manager.worker_for("ses_idle").await.sender;

        assert_eq!(manager.snapshot().await.active_session_count, 1);

        drop(sender);
        sleep(Duration::from_millis(60)).await;

        assert_eq!(manager.snapshot().await.active_session_count, 0);
    }
}
