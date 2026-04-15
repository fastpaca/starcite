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
    time::{Instant as TokioInstant, MissedTickBehavior, interval_at, sleep},
};

use crate::{
    cluster::{
        OwnershipManager, ReplicationCoordinator,
        ownership::OwnedLease,
        replication::{ReplicaSessionState, ReplicaSnapshotResponse},
    },
    data_plane,
    data_plane::{
        HotEventStore, HotSessionStore, PendingFlushQueue,
        repository::{self, AppendOutcome, ProducerSequenceCheck},
    },
    error::AppError,
    model::{AppendReply, Cursor, EventResponse, ValidatedAppendEvent, iso8601},
    telemetry::{AppendBoundary, Telemetry},
};

use super::{fanout::SessionFanout, ops::OpsState};

const APPEND_QUEUE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct SessionManager {
    workers: Arc<Mutex<HashMap<String, SessionWorkerHandle>>>,
    pool: PgPool,
    fanout: SessionFanout,
    hot_store: HotEventStore,
    pending_flush: PendingFlushQueue,
    session_store: HotSessionStore,
    ownership: OwnershipManager,
    replication: ReplicationCoordinator,
    ops: OpsState,
    telemetry: Telemetry,
    idle_timeout: Duration,
    next_worker_id: Arc<AtomicU64>,
}

pub struct SessionManagerDeps {
    pub pool: PgPool,
    pub fanout: SessionFanout,
    pub hot_store: HotEventStore,
    pub pending_flush: PendingFlushQueue,
    pub session_store: HotSessionStore,
    pub ownership: OwnershipManager,
    pub replication: ReplicationCoordinator,
    pub ops: OpsState,
    pub telemetry: Telemetry,
    pub idle_timeout: Duration,
}

#[derive(Clone)]
struct SessionWorkerHandle {
    worker_id: u64,
    sender: mpsc::Sender<SessionCommand>,
}

enum SessionCommand {
    Append {
        tenant_id: String,
        input: Box<ValidatedAppendEvent>,
        reply_tx: oneshot::Sender<Result<AppendOutcome, AppError>>,
    },
    AckArchived {
        tenant_id: String,
        archived_seq: i64,
        reply_tx: oneshot::Sender<Result<(), AppError>>,
    },
}

#[derive(Debug, Default)]
struct SessionWorkerState {
    last_seq: Option<i64>,
    producer_cursors: HashMap<String, i64>,
    flush_seeded: bool,
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

impl SessionWorkerState {
    fn remember_last_seq(&mut self, last_seq: i64) {
        self.last_seq = Some(
            self.last_seq
                .map_or(last_seq, |current| current.max(last_seq)),
        );
    }

    fn producer_seq(&self, producer_id: &str) -> Option<i64> {
        self.producer_cursors.get(producer_id).copied()
    }

    fn remember_producer_seq(&mut self, producer_id: &str, producer_seq: i64) {
        self.producer_cursors
            .entry(producer_id.to_string())
            .and_modify(|current| *current = (*current).max(producer_seq))
            .or_insert(producer_seq);
    }

    fn mark_flush_seeded(&mut self) {
        self.flush_seeded = true;
    }
}

impl SessionManager {
    pub fn new(deps: SessionManagerDeps) -> Self {
        let SessionManagerDeps {
            pool,
            fanout,
            hot_store,
            pending_flush,
            session_store,
            ownership,
            replication,
            ops,
            telemetry,
            idle_timeout,
        } = deps;

        Self {
            workers: Arc::new(Mutex::new(HashMap::new())),
            pool,
            fanout,
            hot_store,
            pending_flush,
            session_store,
            ownership,
            replication,
            ops,
            telemetry,
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
        let tenant_id = tenant_id.to_string();

        for _attempt in 0..2 {
            let (reply_tx, reply_rx) = oneshot::channel();
            let handle = self.worker_for_append(session_id).await?;
            let command = SessionCommand::Append {
                tenant_id: tenant_id.clone(),
                input: Box::new(input.clone()),
                reply_tx,
            };

            match handle.sender.send(command).await {
                Ok(()) => match reply_rx.await {
                    Ok(result) => return result,
                    Err(_error) => {
                        self.prune_worker(session_id, handle.worker_id).await;
                    }
                },
                Err(_error) => {
                    self.prune_worker(session_id, handle.worker_id).await;
                }
            }
        }

        Err(AppError::Internal)
    }

    pub async fn ack_archived(
        &self,
        session_id: &str,
        tenant_id: &str,
        archived_seq: i64,
    ) -> Result<(), AppError> {
        for _attempt in 0..2 {
            let (reply_tx, reply_rx) = oneshot::channel();
            let handle = self.worker_for_archive(session_id).await?;
            let command = SessionCommand::AckArchived {
                tenant_id: tenant_id.to_string(),
                archived_seq,
                reply_tx,
            };

            match handle.sender.send(command).await {
                Ok(()) => match reply_rx.await {
                    Ok(result) => return result,
                    Err(_error) => {
                        self.prune_worker(session_id, handle.worker_id).await;
                    }
                },
                Err(_error) => {
                    self.prune_worker(session_id, handle.worker_id).await;
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

    pub async fn drop_worker_handle(&self, session_id: &str) {
        self.workers.lock().await.remove(session_id);
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

    async fn worker_for_append(&self, session_id: &str) -> Result<SessionWorkerHandle, AppError> {
        if let Some(handle) = self.existing_worker(session_id).await {
            return Ok(handle);
        }

        let lease = self.ownership.live_or_renew_owned(session_id).await?;
        self.bootstrap_owned_session(session_id, &lease).await?;
        Ok(self.worker_for(session_id).await)
    }

    async fn worker_for_archive(&self, session_id: &str) -> Result<SessionWorkerHandle, AppError> {
        if let Some(handle) = self.existing_worker(session_id).await {
            return Ok(handle);
        }

        let lease = self.ownership.live_or_renew_owned(session_id).await?;
        self.bootstrap_owned_session(session_id, &lease).await?;
        Ok(self.worker_for(session_id).await)
    }

    async fn existing_worker(&self, session_id: &str) -> Option<SessionWorkerHandle> {
        self.workers.lock().await.get(session_id).cloned()
    }

    async fn bootstrap_owned_session(
        &self,
        session_id: &str,
        lease: &OwnedLease,
    ) -> Result<(), AppError> {
        if lease.replicas.is_empty() {
            return Ok(());
        }

        let local_last_seq = self
            .session_store
            .get_last_seq(session_id)
            .await
            .unwrap_or(0)
            .max(self.hot_store.max_seq(session_id).await.unwrap_or(0));
        let local_archived_seq = self
            .session_store
            .get_archived_seq(session_id)
            .await
            .unwrap_or(0);
        let mut best_snapshot = None::<ReplicaSnapshotResponse>;

        for replica in &lease.replicas {
            match self
                .replication
                .fetch_session_snapshot(lease.epoch, session_id, replica)
                .await
            {
                Ok(snapshot) => {
                    if best_snapshot
                        .as_ref()
                        .is_none_or(|current| snapshot_is_fresher(&snapshot, current))
                    {
                        best_snapshot = Some(snapshot);
                    }
                }
                Err(error) => {
                    tracing::warn!(error = ?error, session_id, replica = replica.node_id, "failed to fetch replica snapshot during ownership bootstrap");
                }
            }
        }

        let Some(snapshot) = best_snapshot else {
            if local_last_seq == 0 && local_archived_seq == 0 {
                return Err(AppError::QuorumUnavailable {
                    required: 2,
                    acknowledged: 0,
                });
            }

            return Ok(());
        };

        if snapshot.state.last_seq <= local_last_seq
            && snapshot.state.archived_seq <= local_archived_seq
        {
            return Ok(());
        }

        self.apply_bootstrap_snapshot(snapshot).await
    }

    fn spawn_worker(
        &self,
        session_id: String,
        worker_id: u64,
        receiver: mpsc::Receiver<SessionCommand>,
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
        mut receiver: mpsc::Receiver<SessionCommand>,
    ) {
        let mut idle = Box::pin(sleep(self.idle_timeout));
        let mut draining = self.ops.subscribe_draining();
        let mut state = SessionWorkerState::default();
        let mut renew_tick = interval_at(
            TokioInstant::now() + self.ownership.renew_interval(),
            self.ownership.renew_interval(),
        );
        renew_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = &mut idle => {
                    self.prune_worker(&session_id, worker_id).await;
                    return;
                }
                changed = draining.changed() => {
                    if changed.is_err() || *draining.borrow() {
                        tracing::info!(session_id, worker_id, "session worker exiting because node is draining");
                        self.prune_worker(&session_id, worker_id).await;
                        return;
                    }
                }
                _ = renew_tick.tick() => {
                    if let Err(error) = self.ownership.ensure_owned(&session_id).await {
                        tracing::warn!(error = ?error, session_id, "session worker failed to renew ownership");
                        self.prune_worker(&session_id, worker_id).await;
                        return;
                    }
                }
                command = receiver.recv() => {
                    let Some(command) = command else {
                        self.prune_worker(&session_id, worker_id).await;
                        return;
                    };

                    idle
                        .as_mut()
                        .reset(TokioInstant::now() + self.idle_timeout);

                    match command {
                        SessionCommand::Append {
                            tenant_id,
                            input,
                            reply_tx,
                        } => {
                            let result = self
                                .handle_append(&session_id, &tenant_id, &mut state, *input)
                                .await;
                            let _ = reply_tx.send(result);
                        }
                        SessionCommand::AckArchived {
                            tenant_id,
                            archived_seq,
                            reply_tx,
                        } => {
                            let result = self
                                .handle_archive_ack(&session_id, &tenant_id, &mut state, archived_seq)
                                .await;
                            let _ = reply_tx.send(result);
                        }
                    }
                }
            }
        }
    }

    async fn handle_append(
        &self,
        session_id: &str,
        tenant_id: &str,
        state: &mut SessionWorkerState,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        self.append_local_async(session_id, tenant_id, state, input)
            .await
    }

    async fn handle_archive_ack(
        &self,
        session_id: &str,
        tenant_id: &str,
        state: &mut SessionWorkerState,
        archived_seq: i64,
    ) -> Result<(), AppError> {
        let lease = self.ownership.live_or_renew_owned(session_id).await?;
        let current_archived_seq = self
            .session_store
            .get_archived_seq(session_id)
            .await
            .unwrap_or(0);

        if archived_seq <= current_archived_seq {
            return Ok(());
        }

        let last_seq = match state.last_seq {
            Some(last_seq) => last_seq,
            None => {
                let last_seq = data_plane::session_store::resolve_session_last_seq(
                    &self.session_store,
                    &self.pool,
                    session_id,
                )
                .await?;
                state.remember_last_seq(last_seq);
                last_seq
            }
        };
        let archived_seq = archived_seq.min(last_seq);

        if archived_seq <= current_archived_seq {
            return Ok(());
        }

        let frontier = ReplicaSessionState {
            session_id: session_id.to_string(),
            tenant_id: tenant_id.to_string(),
            last_seq,
            archived_seq,
        };

        self.replication
            .replicate_session_state(lease.epoch, frontier.clone(), &[], &lease.replicas)
            .await?;
        self.apply_frontier(&frontier).await;
        Ok(())
    }

    async fn append_local_async(
        &self,
        session_id: &str,
        tenant_id: &str,
        state: &mut SessionWorkerState,
        input: ValidatedAppendEvent,
    ) -> Result<AppendOutcome, AppError> {
        let started_at = std::time::Instant::now();
        let lease = self.ownership.live_or_renew_owned(session_id).await?;
        if !state.flush_seeded {
            self.seed_pending_flush(session_id).await;
            state.mark_flush_seeded();
        }
        let committed_seq = data_plane::session_store::resolve_session_archived_seq(
            &self.session_store,
            &self.pool,
            session_id,
        )
        .await?;
        let last_seq = match state.last_seq {
            Some(last_seq) => last_seq,
            None => {
                let last_seq = data_plane::session_store::resolve_session_last_seq(
                    &self.session_store,
                    &self.pool,
                    session_id,
                )
                .await?;
                state.remember_last_seq(last_seq);
                last_seq
            }
        };

        if let Some(expected_seq) = input.expected_seq
            && last_seq != expected_seq
        {
            return Err(AppError::ExpectedSeqConflict {
                expected: expected_seq,
                current: last_seq,
            });
        }

        let last_producer_seq = self
            .resolve_producer_cursor(state, session_id, &input.producer_id, last_seq)
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
                        state.remember_producer_seq(&producer_id, current);
                        Ok(deduped_append_outcome(
                            &existing,
                            lease.epoch,
                            last_seq,
                            committed_seq,
                            tenant_id,
                        ))
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
            epoch: Some(lease.epoch),
            cursor: next_seq,
        };

        self.telemetry.record_append_boundary(
            AppendBoundary::BeforeQuorumReplicate,
            started_at.elapsed().as_millis() as u64,
        );
        self.replication
            .replicate_session_state(
                lease.epoch,
                ReplicaSessionState {
                    session_id: session_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    last_seq: next_seq,
                    archived_seq: committed_seq,
                },
                std::slice::from_ref(&event),
                &lease.replicas,
            )
            .await?;
        state.remember_last_seq(next_seq);
        state.remember_producer_seq(&event.producer_id, event.producer_seq);
        self.apply_owner_commit(event.clone()).await;
        self.telemetry.record_append_boundary(
            AppendBoundary::AfterCommitBeforeReply,
            started_at.elapsed().as_millis() as u64,
        );

        Ok(AppendOutcome {
            reply: AppendReply {
                seq: next_seq,
                last_seq: next_seq,
                deduped: false,
                epoch: Some(lease.epoch),
                cursor: Cursor::new(Some(lease.epoch), next_seq),
                committed_cursor: Cursor::new(Some(lease.epoch), committed_seq),
            },
            event: Some(event),
            tenant_id: tenant_id.to_string(),
        })
    }

    async fn resolve_producer_cursor(
        &self,
        state: &mut SessionWorkerState,
        session_id: &str,
        producer_id: &str,
        last_seq: i64,
    ) -> Result<Option<i64>, AppError> {
        if let Some(last_producer_seq) = state.producer_seq(producer_id) {
            return Ok(Some(last_producer_seq));
        }

        if let Some(last_producer_seq) = self
            .session_store
            .get_last_producer_seq(session_id, producer_id)
            .await
        {
            state.remember_producer_seq(producer_id, last_producer_seq);
            return Ok(Some(last_producer_seq));
        }

        if let Some(last_producer_seq) = self
            .hot_store
            .last_producer_seq(session_id, producer_id)
            .await
        {
            state.remember_producer_seq(producer_id, last_producer_seq);
            return Ok(Some(last_producer_seq));
        }

        if last_seq == 0 {
            return Ok(None);
        }

        let last_producer_seq =
            repository::load_producer_cursor(&self.pool, session_id, producer_id).await?;
        if let Some(last_producer_seq) = last_producer_seq {
            state.remember_producer_seq(producer_id, last_producer_seq);
        }
        Ok(last_producer_seq)
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

    async fn seed_pending_flush(&self, session_id: &str) {
        let events = self
            .hot_store
            .events_after_cursor(session_id, 0, u32::MAX)
            .await;

        for event in events {
            self.pending_flush.enqueue(event).await;
        }
    }

    pub async fn apply_owner_commit(&self, event: EventResponse) {
        self.apply_commit(event, true).await;
    }

    pub async fn apply_replica_state(
        &self,
        state: ReplicaSessionState,
        events: Vec<EventResponse>,
    ) -> ReplicaApplyDisposition {
        let current_last_seq = self
            .session_store
            .get_last_seq(&state.session_id)
            .await
            .unwrap_or(0);
        let current_archived_seq = self
            .session_store
            .get_archived_seq(&state.session_id)
            .await
            .unwrap_or(0);

        if state.last_seq <= current_last_seq && state.archived_seq <= current_archived_seq {
            return ReplicaApplyDisposition::AlreadyCommitted;
        }

        let unapplied_events = events
            .into_iter()
            .filter(|event| event.seq > current_last_seq)
            .collect::<Vec<_>>();

        if let Some(first_event) = unapplied_events.first() {
            let expected_seq = current_last_seq + 1;
            if first_event.seq != expected_seq {
                return ReplicaApplyDisposition::SeqGap { expected_seq };
            }

            for (offset, event) in unapplied_events.iter().enumerate() {
                let expected_seq = current_last_seq + offset as i64 + 1;
                if event.seq != expected_seq {
                    return ReplicaApplyDisposition::SeqGap { expected_seq };
                }
            }
        } else if state.last_seq > current_last_seq {
            return ReplicaApplyDisposition::SeqGap {
                expected_seq: current_last_seq + 1,
            };
        }

        for event in unapplied_events {
            self.apply_commit(event, false).await;
        }

        self.apply_frontier(&state).await;
        ReplicaApplyDisposition::Applied
    }

    async fn apply_commit(&self, event: EventResponse, enqueue_flush: bool) {
        self.cache_commit(&event).await;

        if enqueue_flush {
            self.pending_flush.enqueue(event.clone()).await;
        }

        self.publish_commit(event).await;
    }

    async fn cache_commit(&self, event: &EventResponse) {
        self.session_store
            .put_tenant(&event.session_id, &event.tenant_id)
            .await;
        self.session_store
            .bump_last_seq(&event.session_id, &event.tenant_id, event.seq)
            .await;
        self.session_store
            .bump_producer_seq(
                &event.session_id,
                &event.tenant_id,
                &event.producer_id,
                event.producer_seq,
            )
            .await;
    }

    async fn apply_frontier(&self, state: &ReplicaSessionState) {
        self.session_store
            .put_tenant(&state.session_id, &state.tenant_id)
            .await;
        self.session_store
            .bump_last_seq(&state.session_id, &state.tenant_id, state.last_seq)
            .await;
        self.session_store
            .update_archived_seq(&state.session_id, state.archived_seq)
            .await;
        self.hot_store
            .delete_below(&state.session_id, state.archived_seq.saturating_add(1))
            .await;
    }

    async fn apply_bootstrap_snapshot(
        &self,
        snapshot: ReplicaSnapshotResponse,
    ) -> Result<(), AppError> {
        let expected_count = snapshot
            .state
            .last_seq
            .saturating_sub(snapshot.state.archived_seq);
        if expected_count != snapshot.events.len() as i64 {
            return Err(AppError::Internal);
        }

        let mut expected_seq = snapshot.state.archived_seq + 1;
        for event in &snapshot.events {
            if event.seq != expected_seq {
                return Err(AppError::Internal);
            }
            expected_seq += 1;
        }

        self.session_store
            .put_tenant(&snapshot.state.session_id, &snapshot.state.tenant_id)
            .await;
        self.hot_store
            .delete_below(&snapshot.state.session_id, i64::MAX)
            .await;

        for event in &snapshot.events {
            self.session_store
                .bump_producer_seq(
                    &snapshot.state.session_id,
                    &snapshot.state.tenant_id,
                    &event.producer_id,
                    event.producer_seq,
                )
                .await;
        }

        self.hot_store.put_events(snapshot.events).await;
        self.apply_frontier(&snapshot.state).await;
        Ok(())
    }

    async fn publish_commit(&self, event: EventResponse) {
        self.hot_store.put_event(event.clone()).await;
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

        if removed {
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

fn deduped_append_outcome(
    existing: &EventResponse,
    epoch: i64,
    last_seq: i64,
    committed_seq: i64,
    tenant_id: &str,
) -> AppendOutcome {
    AppendOutcome {
        reply: AppendReply {
            seq: existing.seq,
            last_seq,
            deduped: true,
            epoch: Some(epoch),
            cursor: Cursor::new(Some(epoch), existing.cursor),
            committed_cursor: Cursor::new(Some(epoch), committed_seq),
        },
        event: None,
        tenant_id: tenant_id.to_string(),
    }
}

fn snapshot_is_fresher(
    candidate: &ReplicaSnapshotResponse,
    current: &ReplicaSnapshotResponse,
) -> bool {
    candidate
        .state
        .last_seq
        .cmp(&current.state.last_seq)
        .then_with(|| {
            candidate
                .state
                .archived_seq
                .cmp(&current.state.archived_seq)
        })
        .is_gt()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaApplyDisposition {
    AlreadyCommitted,
    Applied,
    SeqGap { expected_seq: i64 },
}

#[cfg(test)]
mod tests {
    use super::{SessionManager, SessionManagerDeps, SessionWorkerHandle, SessionWorkerState};
    use crate::{
        cluster::{OwnershipManager, ReplicationCoordinator, replication::ReplicaSessionState},
        data_plane::{HotEventStore, HotSessionStore, PendingFlushQueue},
        error::AppError,
        model::{AppendEventRequest, Cursor, EventResponse, Principal, SessionResponse},
        runtime::{OpsState, fanout::SessionFanout},
        telemetry::Telemetry,
    };
    use serde_json::Map;
    use sqlx::postgres::PgPoolOptions;
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::mpsc, time::sleep};

    fn manager(idle_timeout: Duration) -> SessionManager {
        manager_with_replication(idle_timeout, false)
    }

    fn manager_with_replication(idle_timeout: Duration, require_standby: bool) -> SessionManager {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");

        SessionManager::new(SessionManagerDeps {
            pool: pool.clone(),
            fanout: SessionFanout::default(),
            hot_store: HotEventStore::new(),
            pending_flush: PendingFlushQueue::new(),
            session_store: HotSessionStore::new(),
            ownership: OwnershipManager::new(
                pool.clone(),
                Arc::<str>::from("node-a"),
                Duration::from_secs(5),
                3,
            ),
            replication: ReplicationCoordinator::new(
                Arc::<str>::from("node-a"),
                require_standby,
                Duration::from_millis(500),
            )
            .expect("replication"),
            ops: OpsState::new(30_000),
            telemetry: Telemetry::new(true),
            idle_timeout,
        })
    }

    #[test]
    fn worker_state_tracks_monotonic_cursors() {
        let mut state = SessionWorkerState::default();

        state.remember_last_seq(3);
        state.remember_last_seq(2);
        state.remember_producer_seq("writer-1", 4);
        state.remember_producer_seq("writer-1", 3);

        assert_eq!(state.last_seq, Some(3));
        assert_eq!(state.producer_seq("writer-1"), Some(4));
    }

    fn sample_event(session_id: &str, seq: i64, producer_seq: i64) -> EventResponse {
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
            producer_seq,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-13T00:00:00Z".to_string(),
            epoch: None,
            cursor: seq,
        }
    }

    fn sample_session(last_seq: i64) -> SessionResponse {
        SessionResponse {
            id: "ses_demo".to_string(),
            title: Some("Draft".to_string()),
            creator_principal: Some(Principal {
                tenant_id: "acme".to_string(),
                id: "user-42".to_string(),
                principal_type: "user".to_string(),
            }),
            metadata: Map::new(),
            last_seq,
            created_at: "2026-04-13T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-13T00:00:00.000000Z".to_string(),
            version: 1,
            archived: false,
        }
    }

    fn sample_append_input(
        producer_seq: i64,
        expected_seq: Option<i64>,
    ) -> crate::model::ValidatedAppendEvent {
        AppendEventRequest {
            event_type: "content".to_string(),
            payload: serde_json::json!({"text": "hello"}),
            actor: Some("service:test".to_string()),
            source: Some("test".to_string()),
            metadata: None,
            refs: None,
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq,
            expected_seq,
        }
        .validate()
        .expect("validated append input")
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

    #[tokio::test]
    async fn owner_commit_tracks_producer_cursor_and_enqueues_flush() {
        let manager = manager(Duration::from_secs(1));

        manager
            .apply_owner_commit(sample_event("ses_demo", 4, 9))
            .await;

        assert_eq!(
            manager
                .session_store
                .get_last_producer_seq("ses_demo", "writer-1")
                .await,
            Some(9)
        );
        assert_eq!(
            manager.pending_flush.snapshot().await.pending_event_count,
            1
        );
    }

    #[tokio::test]
    async fn replica_commit_tracks_producer_cursor_without_enqueuing_flush() {
        let manager = manager(Duration::from_secs(1));
        manager
            .session_store
            .put_session("acme", sample_session(3), Some(0))
            .await;

        manager
            .apply_replica_state(
                ReplicaSessionState {
                    session_id: "ses_demo".to_string(),
                    tenant_id: "acme".to_string(),
                    last_seq: 4,
                    archived_seq: 0,
                },
                vec![sample_event("ses_demo", 4, 9)],
            )
            .await;

        assert_eq!(
            manager
                .session_store
                .get_last_producer_seq("ses_demo", "writer-1")
                .await,
            Some(9)
        );
        assert_eq!(
            manager.pending_flush.snapshot().await.pending_event_count,
            0
        );
    }

    #[tokio::test]
    async fn append_local_async_does_not_publish_before_quorum() {
        let manager = manager_with_replication(Duration::from_secs(1), true);
        manager
            .ownership
            .insert_test_lease("ses_demo", 4, Vec::new())
            .await;
        manager
            .session_store
            .put_session("acme", sample_session(0), Some(0))
            .await;

        let result = manager
            .append_local_async(
                "ses_demo",
                "acme",
                &mut SessionWorkerState::default(),
                sample_append_input(1, Some(0)),
            )
            .await;

        assert!(matches!(
            result,
            Err(AppError::QuorumUnavailable {
                required: 2,
                acknowledged: 1
            })
        ));
        assert!(
            manager
                .hot_store
                .events_after_cursor("ses_demo", 0, 10)
                .await
                .is_empty()
        );
        assert_eq!(
            manager.pending_flush.snapshot().await.pending_event_count,
            0
        );
        assert_eq!(
            manager.session_store.get_last_seq("ses_demo").await,
            Some(0)
        );
        assert_eq!(
            manager
                .session_store
                .get_last_producer_seq("ses_demo", "writer-1")
                .await,
            None
        );
    }

    #[tokio::test]
    async fn append_local_async_keeps_committed_cursor_on_archived_frontier() {
        let manager = manager(Duration::from_secs(1));
        manager
            .ownership
            .insert_test_lease("ses_demo", 7, Vec::new())
            .await;
        manager
            .session_store
            .put_session("acme", sample_session(2), Some(2))
            .await;
        manager
            .session_store
            .bump_producer_seq("ses_demo", "acme", "writer-1", 2)
            .await;

        let outcome = manager
            .append_local_async(
                "ses_demo",
                "acme",
                &mut SessionWorkerState::default(),
                sample_append_input(3, Some(2)),
            )
            .await
            .expect("append should succeed");

        assert_eq!(outcome.reply.seq, 3);
        assert_eq!(outcome.reply.last_seq, 3);
        assert_eq!(outcome.reply.epoch, Some(7));
        assert_eq!(outcome.reply.cursor, Cursor::new(Some(7), 3));
        assert_eq!(outcome.reply.committed_cursor, Cursor::new(Some(7), 2));
        assert_eq!(
            manager
                .hot_store
                .events_after_cursor("ses_demo", 0, 10)
                .await
                .into_iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(
            manager.pending_flush.snapshot().await.pending_event_count,
            1
        );
        assert_eq!(
            manager.session_store.get_last_seq("ses_demo").await,
            Some(3)
        );
        assert_eq!(
            manager.session_store.get_archived_seq("ses_demo").await,
            Some(2)
        );
    }

    #[tokio::test]
    async fn ack_archived_updates_frontier_and_prunes_hot_tail() {
        let manager = manager(Duration::from_secs(1));
        manager
            .ownership
            .insert_test_lease("ses_demo", 7, Vec::new())
            .await;
        manager
            .session_store
            .put_session("acme", sample_session(4), Some(0))
            .await;
        manager
            .hot_store
            .put_event(sample_event("ses_demo", 1, 1))
            .await;
        manager
            .hot_store
            .put_event(sample_event("ses_demo", 2, 2))
            .await;
        manager
            .hot_store
            .put_event(sample_event("ses_demo", 3, 3))
            .await;
        manager
            .hot_store
            .put_event(sample_event("ses_demo", 4, 4))
            .await;

        manager
            .ack_archived("ses_demo", "acme", 3)
            .await
            .expect("archive frontier should advance");

        assert_eq!(
            manager.session_store.get_archived_seq("ses_demo").await,
            Some(3)
        );
        assert_eq!(
            manager
                .hot_store
                .events_after_cursor("ses_demo", 0, 10)
                .await
                .into_iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![4]
        );
    }
}
