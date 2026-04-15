use std::time::Instant;

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
    data_plane::{read_path, repository, session_store::resolve_session_last_seq},
    error::AppError,
    model::{Cursor, EventResponse, EventsOptions, LifecycleResponse},
    telemetry::{ReadOperation, SocketSurface, SocketTransport},
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;

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

    loop {
        match receiver.recv().await {
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
    let earliest_available_seq = earliest_available_seq(snapshot);

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
    let earliest_available_seq = earliest_available_seq(snapshot);

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
        topic,
        session_id,
        batch_size,
        join_ref.clone(),
        state,
        outbound_tx,
        cursor,
        snapshot.epoch,
    )
    .await
}

async fn replay_tail(
    topic: &str,
    session_id: &str,
    batch_size: u32,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    mut cursor: Cursor,
    epoch: Option<i64>,
) -> Result<Cursor, AppError> {
    loop {
        let page = read_path::read_events(
            &state.hot_store,
            &state.pool,
            session_id,
            EventsOptions {
                cursor: cursor.seq,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for events in page.events.chunks(batch_size as usize) {
            let events = events
                .iter()
                .cloned()
                .map(|event| attach_event_epoch(event, epoch))
                .collect::<Vec<_>>();
            cursor = events
                .last()
                .map(EventResponse::cursor_token)
                .unwrap_or(cursor);
            let started_at = Instant::now();
            let result = outbound_tx.send(push_frame(
                join_ref.clone(),
                topic.to_string(),
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

async fn resolve_session_cursor_snapshot(
    state: &AppState,
    session_id: &str,
) -> Result<CursorSnapshot, AppError> {
    let lease = state.ownership.live_or_renew_owned(session_id).await?;
    let last_seq = resolve_session_last_seq(&state.session_store, &state.pool, session_id).await?;
    let committed_seq = crate::data_plane::session_store::resolve_session_archived_seq(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await?;

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

fn earliest_available_seq(snapshot: CursorSnapshot) -> Option<i64> {
    (snapshot.last_seq > 0).then_some(1)
}

fn attach_event_epoch(event: EventResponse, epoch: Option<i64>) -> EventResponse {
    match epoch {
        Some(epoch) => event.with_epoch(epoch),
        None => event,
    }
}
