use std::time::{Duration, Instant};

use super::{
    phoenix_protocol::{
        LifecycleOptions, PhoenixFrame, build_gap_payload, lifecycle_payload, push_frame,
    },
    query_options::TailOptions,
    socket_cursor::{CursorSnapshot, build_gap, replay_gap_reason},
    socket_support::record_read_result,
};
use serde_json::json;
use tokio::sync::{broadcast, mpsc};

use crate::{
    AppState,
    data_plane::{
        read_path::{self, ReadEventsError},
        repository,
        session_store::{resolve_session_archived_seq, resolve_session_last_seq},
    },
    error::AppError,
    model::{Cursor, EventResponse, EventsOptions, LifecycleResponse},
    telemetry::{ReadOperation, SocketSurface, SocketTransport},
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;

struct TailReplayContext<'a> {
    topic: &'a str,
    session_id: &'a str,
    batch_size: u32,
    join_ref: Option<String>,
    epoch: Option<i64>,
    replay_until_seq: i64,
}

pub(crate) async fn run_lifecycle_topic(
    topic: String,
    tenant_id: String,
    lifecycle: LifecycleOptions,
    join_ref: Option<String>,
    state: AppState,
    mut receiver: broadcast::Receiver<LifecycleResponse>,
    outbound_tx: mpsc::UnboundedSender<PhoenixFrame>,
) {
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Phoenix, SocketSurface::TenantLifecycle);
    let _fanout_guard = state.lifecycle.tenant_guard(tenant_id.to_string());
    let mut cursor = lifecycle.cursor;

    match sync_lifecycle(
        &topic,
        &tenant_id,
        join_ref.clone(),
        &state,
        &outbound_tx,
        cursor,
    )
    .await
    {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(error = ?error, tenant_id, "phoenix lifecycle replay failed");
            return;
        }
    }

    loop {
        match receiver.recv().await {
            Ok(event) if event.cursor <= cursor.seq => continue,
            Ok(event) => {
                cursor = Cursor::new(None, event.cursor);
                let payload = match lifecycle_payload(&event) {
                    Ok(payload) => payload,
                    Err(error) => {
                        tracing::warn!(error = ?error, topic, "phoenix lifecycle payload failed");
                        return;
                    }
                };

                let started_at = Instant::now();
                let result = outbound_tx.send(push_frame(
                    join_ref.clone(),
                    topic.clone(),
                    "lifecycle",
                    payload,
                ));
                record_read_result(
                    &state,
                    ReadOperation::LifecycleLive,
                    started_at,
                    result.as_ref().map(|_| ()).map_err(|_| ()),
                );

                if result.is_err() {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    topic,
                    cursor = ?cursor,
                    "phoenix lifecycle broadcast lagged, replaying from store"
                );

                match sync_lifecycle(
                    &topic,
                    &tenant_id,
                    join_ref.clone(),
                    &state,
                    &outbound_tx,
                    cursor,
                )
                .await
                {
                    Ok(next_cursor) => cursor = next_cursor,
                    Err(error) => {
                        tracing::warn!(
                            error = ?error,
                            tenant_id,
                            "phoenix lifecycle replay after lag failed"
                        );
                        return;
                    }
                }
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

pub(crate) async fn run_tail_topic(
    topic: String,
    session_id: String,
    tail: TailOptions,
    join_ref: Option<String>,
    state: AppState,
    mut receiver: broadcast::Receiver<EventResponse>,
    outbound_tx: mpsc::UnboundedSender<PhoenixFrame>,
) {
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Phoenix, SocketSurface::Tail);
    let _fanout_guard = state.fanout.session_guard(session_id.clone());
    let mut cursor = tail.cursor;

    match sync_tail(
        &topic,
        &session_id,
        tail.batch_size,
        join_ref.clone(),
        &state,
        &outbound_tx,
        cursor,
    )
    .await
    {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(error = ?error, session_id, "phoenix tail replay failed");
            return;
        }
    }

    let mut catchup_tick = tokio::time::interval(tail_catchup_interval());
    catchup_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    catchup_tick.tick().await;

    loop {
        tokio::select! {
            message = receiver.recv() => match message {
                Ok(event) if event.seq <= cursor.seq => continue,
                Ok(event) => {
                    cursor = event.cursor_token();
                    let started_at = Instant::now();
                    let result = outbound_tx.send(push_frame(
                        join_ref.clone(),
                        topic.clone(),
                        "events",
                        json!({"events": [event]}),
                    ));
                    record_read_result(
                        &state,
                        ReadOperation::TailLive,
                        started_at,
                        result.as_ref().map(|_| ()).map_err(|_| ()),
                    );

                    if result.is_err() {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        session_id,
                        skipped,
                        cursor = ?cursor,
                        "phoenix tail broadcast lagged, replaying from store"
                    );

                    match sync_tail(
                        &topic,
                        &session_id,
                        tail.batch_size,
                        join_ref.clone(),
                        &state,
                        &outbound_tx,
                        cursor,
                    )
                    .await
                    {
                        Ok(next_cursor) => cursor = next_cursor,
                        Err(error) => {
                            tracing::warn!(
                                error = ?error,
                                session_id,
                                "phoenix tail replay after lag failed"
                            );
                            return;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = catchup_tick.tick() => {
                if !tail_requires_catchup(state.hot_store.max_seq(&session_id).await, cursor.seq) {
                    continue;
                }

                tracing::warn!(
                    session_id,
                    cursor = ?cursor,
                    "phoenix tail hot store advanced without live fanout, replaying from store"
                );

                match sync_tail(
                    &topic,
                    &session_id,
                    tail.batch_size,
                    join_ref.clone(),
                    &state,
                    &outbound_tx,
                    cursor,
                )
                .await
                {
                    Ok(next_cursor) => cursor = next_cursor,
                    Err(error) => {
                        tracing::warn!(
                            error = ?error,
                            session_id,
                            "phoenix tail replay after catchup check failed"
                        );
                        return;
                    }
                }
            }
        }
    }
}

async fn sync_lifecycle(
    topic: &str,
    tenant_id: &str,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    cursor: Cursor,
) -> Result<Cursor, AppError> {
    let snapshot = resolve_lifecycle_cursor_snapshot(state, tenant_id).await?;
    let earliest_available_seq = earliest_available_lifecycle_seq(snapshot);

    if let Some(reason) = replay_gap_reason(cursor, snapshot, earliest_available_seq) {
        let gap = build_gap(reason, cursor, snapshot, earliest_available_seq);
        outbound_tx
            .send(push_frame(
                join_ref,
                topic.to_string(),
                "gap",
                build_gap_payload(&gap),
            ))
            .map_err(|_| AppError::Internal)?;
        return Ok(gap.next_cursor);
    }

    replay_lifecycle(
        topic,
        tenant_id,
        join_ref.clone(),
        state,
        outbound_tx,
        cursor,
    )
    .await
}

async fn replay_lifecycle(
    topic: &str,
    tenant_id: &str,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    mut cursor: Cursor,
) -> Result<Cursor, AppError> {
    loop {
        let page = repository::read_lifecycle_events(
            &state.pool,
            tenant_id,
            None,
            EventsOptions {
                cursor: cursor.seq,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for event in page.events {
            cursor = Cursor::new(None, event.cursor);
            let started_at = Instant::now();
            let result = outbound_tx.send(push_frame(
                join_ref.clone(),
                topic.to_string(),
                "lifecycle",
                lifecycle_payload(&event)?,
            ));
            record_read_result(
                state,
                ReadOperation::LifecycleCatchup,
                started_at,
                result.as_ref().map(|_| ()).map_err(|_| ()),
            );
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

async fn sync_tail(
    topic: &str,
    session_id: &str,
    batch_size: u32,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    cursor: Cursor,
) -> Result<Cursor, AppError> {
    let snapshot = resolve_session_cursor_snapshot(state, session_id).await?;
    let earliest_available_seq =
        resolve_tail_earliest_available_seq(state, session_id, snapshot.committed_seq).await;

    if let Some(reason) = replay_gap_reason(cursor, snapshot, earliest_available_seq) {
        let gap = build_gap(reason, cursor, snapshot, earliest_available_seq);
        outbound_tx
            .send(push_frame(
                join_ref,
                topic.to_string(),
                "gap",
                build_gap_payload(&gap),
            ))
            .map_err(|_| AppError::Internal)?;
        return Ok(gap.next_cursor);
    }

    replay_tail(
        TailReplayContext {
            topic,
            session_id,
            batch_size,
            join_ref: join_ref.clone(),
            epoch: snapshot.epoch,
            replay_until_seq: snapshot.last_seq,
        },
        state,
        outbound_tx,
        cursor,
    )
    .await
}

async fn replay_tail(
    context: TailReplayContext<'_>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    mut cursor: Cursor,
) -> Result<Cursor, AppError> {
    loop {
        if cursor.seq >= context.replay_until_seq {
            return Ok(cursor);
        }

        let page = match read_path::read_events(
            &state.hot_store,
            &state.pool,
            context.session_id,
            EventsOptions {
                cursor: cursor.seq,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await
        {
            Ok(page) => page,
            Err(ReadEventsError::ContinuityUnavailable) => {
                return emit_tail_gap(
                    context.topic,
                    context.session_id,
                    context.join_ref.clone(),
                    state,
                    outbound_tx,
                    cursor,
                )
                .await;
            }
            Err(ReadEventsError::App(error)) => return Err(error),
        };

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for events in page.events.chunks(context.batch_size as usize) {
            let events = events
                .iter()
                .cloned()
                .map(|event| attach_event_epoch(event, context.epoch))
                .collect::<Vec<_>>();
            cursor = events
                .last()
                .map(EventResponse::cursor_token)
                .unwrap_or(cursor);
            let started_at = Instant::now();
            let result = outbound_tx.send(push_frame(
                context.join_ref.clone(),
                context.topic.to_string(),
                "events",
                json!({"events": events}),
            ));
            record_read_result(
                state,
                ReadOperation::TailCatchup,
                started_at,
                result.as_ref().map(|_| ()).map_err(|_| ()),
            );
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

async fn emit_tail_gap(
    topic: &str,
    session_id: &str,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    from_cursor: Cursor,
) -> Result<Cursor, AppError> {
    let snapshot = resolve_session_cursor_snapshot(state, session_id).await?;
    let earliest_available_seq =
        resolve_tail_earliest_available_seq(state, session_id, snapshot.committed_seq).await;
    let gap = build_gap(
        super::socket_cursor::GapReason::CursorExpired,
        from_cursor,
        snapshot,
        earliest_available_seq,
    );
    outbound_tx
        .send(push_frame(
            join_ref,
            topic.to_string(),
            "gap",
            build_gap_payload(&gap),
        ))
        .map_err(|_| AppError::Internal)?;
    Ok(gap.next_cursor)
}

async fn resolve_session_cursor_snapshot(
    state: &AppState,
    session_id: &str,
) -> Result<CursorSnapshot, AppError> {
    let lease = state.ownership.live_or_renew_owned(session_id).await?;
    let last_seq = resolve_session_last_seq(&state.session_store, &state.pool, session_id).await?;
    let committed_seq =
        resolve_session_archived_seq(&state.session_store, &state.pool, session_id).await?;

    Ok(CursorSnapshot {
        epoch: Some(lease.epoch),
        last_seq,
        committed_seq,
    })
}

async fn resolve_lifecycle_cursor_snapshot(
    state: &AppState,
    tenant_id: &str,
) -> Result<CursorSnapshot, AppError> {
    let last_seq = repository::lifecycle_head_seq(&state.pool, tenant_id, None).await?;

    Ok(CursorSnapshot {
        epoch: None,
        last_seq,
        committed_seq: last_seq,
    })
}

fn earliest_available_lifecycle_seq(snapshot: CursorSnapshot) -> Option<i64> {
    (snapshot.last_seq > 0).then_some(1)
}

fn tail_requires_catchup(max_hot_seq: Option<i64>, cursor_seq: i64) -> bool {
    max_hot_seq.is_some_and(|max_hot_seq| max_hot_seq > cursor_seq)
}

fn tail_catchup_interval() -> Duration {
    if cfg!(test) {
        Duration::from_millis(25)
    } else {
        Duration::from_secs(5)
    }
}

async fn resolve_tail_earliest_available_seq(
    state: &AppState,
    session_id: &str,
    committed_seq: i64,
) -> Option<i64> {
    let hot_first_seq = state.hot_store.first_seq(session_id).await;

    match hot_first_seq {
        Some(1) => Some(1),
        Some(hot_first_seq) if hot_first_seq > 1 => {
            Some(resolve_archive_floor_or_hot_floor(state, session_id, hot_first_seq).await)
        }
        None if committed_seq > 0 => {
            Some(resolve_archive_floor_or_hot_floor(state, session_id, committed_seq + 1).await)
        }
        _ => None,
    }
}

async fn resolve_archive_floor_or_hot_floor(
    state: &AppState,
    session_id: &str,
    hot_floor_seq: i64,
) -> i64 {
    archive_floor_or_hot_floor(
        repository::load_first_event_seq(&state.pool, session_id)
            .await
            .ok()
            .flatten(),
        hot_floor_seq,
    )
}

fn archive_floor_or_hot_floor(archived_floor_seq: Option<i64>, hot_floor_seq: i64) -> i64 {
    archived_floor_seq
        .filter(|seq| *seq > 0)
        .unwrap_or(hot_floor_seq)
}

fn attach_event_epoch(event: EventResponse, epoch: Option<i64>) -> EventResponse {
    match epoch {
        Some(epoch) => event.with_epoch(epoch),
        None => event,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        archive_floor_or_hot_floor, earliest_available_lifecycle_seq, run_tail_topic, sync_tail,
        tail_requires_catchup,
    };
    use crate::{
        AppState,
        api::socket_cursor::{CursorSnapshot, GapReason, replay_gap_reason},
        auth::AuthService,
        cluster::{ControlPlaneState, OwnerProxy, OwnershipManager, ReplicationCoordinator},
        config::{AuthMode, Config},
        data_plane::{ArchiveQueue, HotEventStore, HotSessionStore, PendingFlushQueue},
        model::{Cursor, EventResponse, SessionResponse},
        runtime::{
            LifecycleFanout, OpsState, SessionFanout, SessionManager, SessionManagerDeps,
            SessionRuntime,
        },
        telemetry::Telemetry,
    };
    use serde_json::{Map, Value};
    use sqlx::postgres::PgPoolOptions;
    use std::{sync::Arc, time::Duration};
    use tokio::{sync::mpsc, task::yield_now, time::timeout};

    fn test_state() -> AppState {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");
        let fanout = SessionFanout::default();
        let lifecycle = LifecycleFanout::default();
        let hot_store = HotEventStore::new();
        let archive_queue = ArchiveQueue::new();
        let pending_flush = PendingFlushQueue::new();
        let session_store = HotSessionStore::new();
        let telemetry = Telemetry::new(true);
        let ops = OpsState::new(30_000);
        let instance_id = Arc::<str>::from("node-a");
        let ownership =
            OwnershipManager::new(pool.clone(), instance_id.clone(), Duration::from_secs(5));
        let control_plane = ControlPlaneState::new(None, None, Duration::from_secs(5));
        let owner_proxy = OwnerProxy::new(Duration::from_millis(100), None);
        let replication = ReplicationCoordinator::new(
            instance_id.clone(),
            false,
            None,
            Duration::from_millis(100),
        )
        .expect("replication");
        let session_manager = SessionManager::new(SessionManagerDeps {
            pool: pool.clone(),
            fanout: fanout.clone(),
            hot_store: hot_store.clone(),
            pending_flush: pending_flush.clone(),
            session_store: session_store.clone(),
            ownership: ownership.clone(),
            replication: replication.clone(),
            ops: ops.clone(),
            telemetry: telemetry.clone(),
            idle_timeout: Duration::from_secs(30),
        });
        let runtime = SessionRuntime::new(
            None,
            lifecycle.clone(),
            telemetry.clone(),
            instance_id.clone(),
            Duration::from_secs(30),
        );
        let auth = AuthService::new(&test_config()).expect("auth");

        AppState {
            pool,
            fanout,
            lifecycle,
            hot_store,
            archive_queue,
            pending_flush,
            session_store,
            session_manager,
            ownership,
            control_plane,
            owner_proxy,
            replication,
            runtime,
            ops,
            auth_mode: AuthMode::None,
            auth,
            telemetry,
            instance_id,
        }
    }

    fn test_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:4001".parse().expect("listen addr"),
            ops_listen_addr: "127.0.0.1:4101".parse().expect("ops listen addr"),
            database_url: "postgres://postgres:postgres@localhost/starcite_test".to_string(),
            max_connections: 1,
            archive_flush_interval_ms: 5_000,
            migrate_on_boot: false,
            auth_mode: AuthMode::None,
            auth_issuer: None,
            auth_audience: None,
            auth_jwks_url: None,
            auth_jwt_leeway_seconds: 1,
            auth_jwks_refresh_ms: 60_000,
            auth_jwks_hard_expiry_ms: 60_000,
            telemetry_enabled: true,
            shutdown_drain_timeout_ms: 30_000,
            session_runtime_idle_timeout_ms: 30_000,
            commit_flush_interval_ms: 100,
            local_async_lease_ttl_ms: 5_000,
            local_async_node_public_url: None,
            local_async_node_ops_url: None,
            local_async_node_ttl_ms: 2_000,
            local_async_owner_proxy_timeout_ms: 100,
            local_async_standby_url: None,
            local_async_replication_timeout_ms: 100,
        }
    }

    fn sample_session(last_seq: i64) -> SessionResponse {
        SessionResponse {
            id: "ses_demo".to_string(),
            title: Some("Draft".to_string()),
            creator_principal: None,
            metadata: Map::new(),
            last_seq,
            created_at: "2026-04-13T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-13T00:00:00.000000Z".to_string(),
            version: 1,
            archived: false,
        }
    }

    fn sample_event(session_id: &str, seq: i64) -> EventResponse {
        EventResponse {
            session_id: session_id.to_string(),
            seq,
            event_type: "content".to_string(),
            payload: serde_json::json!({"text": format!("m{seq}")})
                .as_object()
                .expect("object payload")
                .clone(),
            actor: "service:test".to_string(),
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

    async fn seed_session(
        state: &AppState,
        session_id: &str,
        epoch: i64,
        last_seq: i64,
        archived_seq: i64,
    ) {
        let mut session = sample_session(last_seq);
        session.id = session_id.to_string();
        state
            .session_store
            .put_session("acme", session, Some(archived_seq))
            .await;
        state
            .ownership
            .insert_test_lease(session_id, epoch, None)
            .await;
    }

    fn drain_frames(
        receiver: &mut mpsc::UnboundedReceiver<crate::api::phoenix_protocol::PhoenixFrame>,
    ) -> Vec<crate::api::phoenix_protocol::PhoenixFrame> {
        let mut frames = Vec::new();

        while let Ok(frame) = receiver.try_recv() {
            frames.push(frame);
        }

        frames
    }

    fn payload_event_seqs(frame: &crate::api::phoenix_protocol::PhoenixFrame) -> Vec<i64> {
        frame.payload["events"]
            .as_array()
            .expect("events array")
            .iter()
            .map(|event| event["seq"].as_i64().expect("seq"))
            .collect()
    }

    #[tokio::test]
    async fn sync_tail_batches_hot_replay_and_attaches_epoch() {
        let state = test_state();
        seed_session(&state, "ses_demo", 7, 3, 0).await;
        state.hot_store.put_event(sample_event("ses_demo", 1)).await;
        state.hot_store.put_event(sample_event("ses_demo", 2)).await;
        state.hot_store.put_event(sample_event("ses_demo", 3)).await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        let cursor = sync_tail(
            "tail:ses_demo",
            "ses_demo",
            2,
            Some("1".to_string()),
            &state,
            &outbound_tx,
            Cursor::zero(),
        )
        .await
        .expect("tail sync should succeed");

        let frames = drain_frames(&mut outbound_rx);
        assert_eq!(cursor, Cursor::new(Some(7), 3));
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].event, "events");
        assert_eq!(payload_event_seqs(&frames[0]), vec![1, 2]);
        assert_eq!(frames[0].payload["events"][0]["epoch"], Value::from(7));
        assert_eq!(frames[0].payload["events"][1]["epoch"], Value::from(7));
        assert_eq!(frames[1].event, "events");
        assert_eq!(payload_event_seqs(&frames[1]), vec![3]);
        assert_eq!(frames[1].payload["events"][0]["epoch"], Value::from(7));
    }

    #[tokio::test]
    async fn sync_tail_emits_gap_when_replay_continuity_is_unavailable() {
        let state = test_state();
        seed_session(&state, "ses_demo", 7, 3, 0).await;
        state.hot_store.put_event(sample_event("ses_demo", 1)).await;
        state.hot_store.put_event(sample_event("ses_demo", 3)).await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        let cursor = sync_tail(
            "tail:ses_demo",
            "ses_demo",
            1,
            Some("1".to_string()),
            &state,
            &outbound_tx,
            Cursor::zero(),
        )
        .await
        .expect("continuity gap should be reported");

        let frames = drain_frames(&mut outbound_rx);
        assert_eq!(cursor, Cursor::new(Some(7), 0));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event, "gap");
        assert_eq!(frames[0].payload["reason"], "cursor_expired");
        assert_eq!(frames[0].payload["from_cursor"], Value::from(0));
        assert_eq!(frames[0].payload["next_cursor"], Value::from(0));
        assert_eq!(frames[0].payload["next_cursor_epoch"], Value::from(7));
        assert_eq!(
            frames[0].payload["earliest_available_cursor"],
            Value::from(1)
        );
        assert_eq!(
            frames[0].payload["earliest_available_cursor_epoch"],
            Value::from(7)
        );
    }

    #[tokio::test]
    async fn sync_tail_expires_cursor_before_hot_floor() {
        let state = test_state();
        seed_session(&state, "ses_demo", 7, 6, 0).await;
        state.hot_store.put_event(sample_event("ses_demo", 6)).await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        let cursor = sync_tail(
            "tail:ses_demo",
            "ses_demo",
            1,
            Some("1".to_string()),
            &state,
            &outbound_tx,
            Cursor::zero(),
        )
        .await
        .expect("cursor before hot floor should expire");

        let frames = drain_frames(&mut outbound_rx);
        assert_eq!(cursor, Cursor::new(Some(7), 5));
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event, "gap");
        assert_eq!(frames[0].payload["reason"], "cursor_expired");
        assert_eq!(frames[0].payload["next_cursor"], Value::from(5));
        assert_eq!(
            frames[0].payload["earliest_available_cursor"],
            Value::from(6)
        );
    }

    #[tokio::test]
    async fn run_tail_topic_hands_off_from_replay_to_live_without_duplicate_frames() {
        let state = test_state();
        seed_session(&state, "ses_demo", 7, 1, 0).await;
        state.hot_store.put_event(sample_event("ses_demo", 1)).await;
        let receiver = state.fanout.subscribe("ses_demo").await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(run_tail_topic(
            "tail:ses_demo".to_string(),
            "ses_demo".to_string(),
            crate::api::query_options::TailOptions {
                cursor: Cursor::zero(),
                batch_size: 8,
            },
            Some("1".to_string()),
            state.clone(),
            receiver,
            outbound_tx,
        ));

        let replay = timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("replay frame should arrive")
            .expect("replay frame");
        assert_eq!(replay.event, "events");
        assert_eq!(payload_event_seqs(&replay), vec![1]);

        yield_now().await;

        state
            .fanout
            .broadcast(sample_event("ses_demo", 2).with_epoch(7))
            .await;

        let live = timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("live frame should arrive")
            .expect("live frame");
        assert_eq!(live.event, "events");
        assert_eq!(payload_event_seqs(&live), vec![2]);
        assert_eq!(live.payload["events"].as_array().expect("events").len(), 1);
        assert_eq!(live.payload["events"][0]["epoch"], Value::from(7));

        let no_duplicate = timeout(Duration::from_millis(50), outbound_rx.recv()).await;
        assert!(
            no_duplicate.is_err(),
            "unexpected duplicate replay/live frame"
        );

        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn run_tail_topic_periodically_catches_up_without_broadcast_signal() {
        let state = test_state();
        seed_session(&state, "ses_demo", 7, 1, 0).await;
        state.hot_store.put_event(sample_event("ses_demo", 1)).await;
        let receiver = state.fanout.subscribe("ses_demo").await;
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        let task = tokio::spawn(run_tail_topic(
            "tail:ses_demo".to_string(),
            "ses_demo".to_string(),
            crate::api::query_options::TailOptions {
                cursor: Cursor::zero(),
                batch_size: 8,
            },
            Some("1".to_string()),
            state.clone(),
            receiver,
            outbound_tx,
        ));

        let replay = timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("replay frame should arrive")
            .expect("replay frame");
        assert_eq!(payload_event_seqs(&replay), vec![1]);

        state.hot_store.put_event(sample_event("ses_demo", 2)).await;
        state
            .session_store
            .bump_last_seq("ses_demo", "acme", 2)
            .await;

        let catchup = timeout(Duration::from_millis(250), outbound_rx.recv())
            .await
            .expect("catchup frame should arrive")
            .expect("catchup frame");
        assert_eq!(catchup.event, "events");
        assert_eq!(payload_event_seqs(&catchup), vec![2]);
        assert_eq!(catchup.payload["events"][0]["epoch"], Value::from(7));

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn lifecycle_floor_starts_at_one_when_history_exists() {
        assert_eq!(
            earliest_available_lifecycle_seq(CursorSnapshot {
                epoch: None,
                last_seq: 3,
                committed_seq: 3,
            }),
            Some(1)
        );
        assert_eq!(
            earliest_available_lifecycle_seq(CursorSnapshot {
                epoch: None,
                last_seq: 0,
                committed_seq: 0,
            }),
            None
        );
    }

    #[test]
    fn archive_floor_prefers_archived_floor_when_present() {
        assert_eq!(archive_floor_or_hot_floor(Some(2), 6), 2);
        assert_eq!(archive_floor_or_hot_floor(None, 6), 6);
    }

    #[test]
    fn tail_requires_catchup_only_when_hot_store_is_ahead() {
        assert!(tail_requires_catchup(Some(6), 5));
        assert!(!tail_requires_catchup(Some(5), 5));
        assert!(!tail_requires_catchup(None, 5));
    }

    #[test]
    fn replay_gap_reason_keeps_same_epoch_cursor_ahead_of_head_recoverable() {
        let reason = replay_gap_reason(
            Cursor::new(Some(7), 9),
            CursorSnapshot {
                epoch: Some(7),
                last_seq: 6,
                committed_seq: 4,
            },
            Some(2),
        );

        assert_eq!(reason, None);
    }

    #[test]
    fn replay_gap_reason_rolls_back_only_across_epoch_change() {
        let reason = replay_gap_reason(
            Cursor::new(Some(6), 9),
            CursorSnapshot {
                epoch: Some(7),
                last_seq: 6,
                committed_seq: 4,
            },
            Some(2),
        );

        assert_eq!(reason, Some(GapReason::Rollback));
    }
}
