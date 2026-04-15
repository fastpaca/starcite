use std::{
    future::pending,
    pin::Pin,
    time::{Duration, Instant},
};

use super::{
    query_options::TailOptions,
    raw_socket::{build_gap_frame, send_events, send_gap, send_node_draining, send_token_expired},
    socket_cursor::{CursorSnapshot, build_gap, replay_gap_reason},
    socket_support::{record_read_result, wait_for_drain},
};
use axum::extract::ws::{Message, WebSocket};
use tokio::time::{Sleep, sleep};

use crate::{
    AppState, data_plane,
    error::AppError,
    model::{Cursor, EventResponse, EventsOptions},
    telemetry::{ReadOperation, SocketSurface, SocketTransport},
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;
type SocketExpiry = Pin<Box<Sleep>>;

pub(crate) async fn require_local_owner_for_event_path(
    state: &AppState,
    session_id: &str,
) -> Result<(), AppError> {
    state.ownership.live_or_renew_owned(session_id).await?;
    Ok(())
}

pub(crate) async fn run_tail_session(
    mut socket: WebSocket,
    state: AppState,
    session_id: String,
    tail: TailOptions,
    mut receiver: tokio::sync::broadcast::Receiver<EventResponse>,
    expiry_delay: Option<Duration>,
) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Raw, SocketSurface::Tail);
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Raw, SocketSurface::Tail);
    let _fanout_guard = state.fanout.session_guard(session_id.clone());
    let mut cursor = tail.cursor;
    let mut expiry = expiry_delay.map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining(&mut socket, &state.ops).await;
        return;
    }

    match sync_tail(&mut socket, &state, &session_id, cursor, tail.batch_size).await {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(error = ?error, session_id, "tail replay failed");
            return;
        }
    }

    loop {
        tokio::select! {
            _ = wait_for_optional_expiry(expiry.as_mut()) => {
                let _ = send_token_expired(&mut socket).await;
                return;
            }
            _ = wait_for_drain(&state.ops) => {
                tracing::info!(
                    session_id = %session_id,
                    "closing raw tail socket because node is draining"
                );
                let _ = send_node_draining(&mut socket, &state.ops).await;
                return;
            }
            incoming = socket.recv() => {
                if !handle_socket_message(&mut socket, incoming).await {
                    return;
                }
            }
            received = receiver.recv() => match received {
                Ok(event) if event.seq <= cursor.seq => continue,
                Ok(event) => {
                    cursor = event.cursor_token();
                    let started_at = Instant::now();
                    let result = send_events(&mut socket, &[event]).await;
                    record_read_result(&state, ReadOperation::TailLive, started_at, result);

                    if result.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(
                        session_id,
                        skipped,
                        cursor = ?cursor,
                        "tail broadcast lagged, replaying from store"
                    );

                    match sync_tail(&mut socket, &state, &session_id, cursor, tail.batch_size).await {
                        Ok(next_cursor) => cursor = next_cursor,
                        Err(error) => {
                            tracing::warn!(error = ?error, session_id, "tail replay after lag failed");
                            return;
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    }
}

async fn sync_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    cursor: Cursor,
    batch_size: u32,
) -> Result<Cursor, AppError> {
    let snapshot = resolve_session_cursor_snapshot(state, session_id).await?;
    let earliest_available_seq = earliest_available_seq(snapshot);

    if let Some(reason) = replay_gap_reason(cursor, snapshot, earliest_available_seq) {
        let gap = build_gap(reason, cursor, snapshot, earliest_available_seq);
        send_gap(socket, &build_gap_frame(&gap))
            .await
            .map_err(|_| AppError::Internal)?;
        return Ok(gap.next_cursor);
    }

    replay_tail(
        socket,
        state,
        session_id,
        cursor,
        batch_size,
        snapshot.epoch,
    )
    .await
}

async fn replay_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    mut cursor: Cursor,
    batch_size: u32,
    epoch: Option<i64>,
) -> Result<Cursor, AppError> {
    loop {
        let page = data_plane::read_path::read_events(
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
            let result = send_events(socket, &events).await;
            record_read_result(state, ReadOperation::TailCatchup, started_at, result);
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

async fn resolve_session_cursor_snapshot(
    state: &AppState,
    session_id: &str,
) -> Result<CursorSnapshot, AppError> {
    let lease = state.ownership.live_or_renew_owned(session_id).await?;
    let last_seq = data_plane::session_store::resolve_session_last_seq(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await?;
    let committed_seq = data_plane::session_store::resolve_session_archived_seq(
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

fn earliest_available_seq(snapshot: CursorSnapshot) -> Option<i64> {
    (snapshot.last_seq > 0).then_some(1)
}

fn attach_event_epoch(event: EventResponse, epoch: Option<i64>) -> EventResponse {
    match epoch {
        Some(epoch) => event.with_epoch(epoch),
        None => event,
    }
}

async fn handle_socket_message(
    socket: &mut WebSocket,
    incoming: Option<Result<Message, axum::Error>>,
) -> bool {
    match incoming {
        Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await.is_ok(),
        Some(Ok(Message::Close(_))) | None => false,
        Some(Ok(_)) => true,
        Some(Err(error)) => {
            tracing::warn!(error = ?error, "raw websocket receive failed");
            false
        }
    }
}

async fn wait_for_optional_expiry(expiry: Option<&mut SocketExpiry>) {
    match expiry {
        Some(expiry) => expiry.await,
        None => pending::<()>().await,
    }
}
