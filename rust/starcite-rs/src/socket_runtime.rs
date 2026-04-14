use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket};
use tokio::{sync::broadcast, time::sleep};

use crate::{
    AppState,
    config::CommitMode,
    error::AppError,
    model::{EventResponse, EventsOptions, LifecycleResponse},
    query_options::{LifecycleOptions, TailOptions},
    raw_socket::{
        build_resume_invalidated_gap, build_resume_invalidated_gap_with_earliest, send_events,
        send_gap, send_lifecycle, send_node_draining, send_token_expired,
    },
    read_path, repository,
    session_store::resolve_session_last_seq,
    telemetry::{ReadOperation, ReadOutcome, ReadPhase, SocketSurface, SocketTransport},
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;

pub(crate) async fn require_local_owner_for_event_path(
    state: &AppState,
    session_id: &str,
) -> Result<(), AppError> {
    if state.commit_mode == CommitMode::LocalAsync {
        state.ownership.live_or_renew_owned(session_id).await?;
    }

    Ok(())
}

pub(crate) async fn run_tail_session(
    mut socket: WebSocket,
    state: AppState,
    session_id: String,
    tail: TailOptions,
    mut receiver: broadcast::Receiver<EventResponse>,
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
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
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
                    Ok(event) if event.seq <= cursor => continue,
                    Ok(event) => {
                        cursor = event.seq;
                        let started_at = Instant::now();
                        let result = send_events(&mut socket, &[event]).await;
                        record_read_result(&state, ReadOperation::TailLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            session_id,
                            skipped,
                            cursor,
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
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        } else {
            tokio::select! {
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
                    Ok(event) if event.seq <= cursor => continue,
                    Ok(event) => {
                        cursor = event.seq;
                        let started_at = Instant::now();
                        let result = send_events(&mut socket, &[event]).await;
                        record_read_result(&state, ReadOperation::TailLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            session_id,
                            skipped,
                            cursor,
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
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

pub(crate) async fn run_lifecycle_session(
    mut socket: WebSocket,
    state: AppState,
    lifecycle: LifecycleOptions,
    mut receiver: broadcast::Receiver<LifecycleResponse>,
    expiry_delay: Option<Duration>,
) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Raw, lifecycle_socket_surface(&lifecycle));
    let _subscription = state
        .telemetry
        .track_socket_subscription(SocketTransport::Raw, lifecycle_socket_surface(&lifecycle));
    let _fanout_guard = match lifecycle.session_id.as_ref() {
        Some(session_id) => state.lifecycle.session_guard(session_id.clone()),
        None => state.lifecycle.tenant_guard(lifecycle.tenant_id.clone()),
    };
    let mut cursor = lifecycle.cursor;
    let mut expiry = expiry_delay.map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining(&mut socket, &state.ops).await;
        return;
    }

    match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
        Ok(next_cursor) => cursor = next_cursor,
        Err(error) => {
            tracing::warn!(
                error = ?error,
                tenant_id = lifecycle.tenant_id,
                "lifecycle replay failed"
            );
            return;
        }
    }

    loop {
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
                    let _ = send_token_expired(&mut socket).await;
                    return;
                }
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        tenant_id = %lifecycle.tenant_id,
                        session_id = ?lifecycle.session_id,
                        "closing raw lifecycle socket because node is draining"
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
                    Ok(event) if event.cursor <= cursor => continue,
                    Ok(event)
                        if lifecycle
                            .session_id
                            .as_ref()
                            .is_some_and(|session_id| event.event.session_id() != session_id) =>
                    {
                        continue;
                    }
                    Ok(event) => {
                        cursor = event.cursor;
                        let started_at = Instant::now();
                        let result = send_lifecycle(&mut socket, &event).await;
                        record_read_result(&state, ReadOperation::LifecycleLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            tenant_id = lifecycle.tenant_id,
                            cursor,
                            "lifecycle broadcast lagged, replaying from store"
                        );

                        match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(
                                    error = ?error,
                                    tenant_id = lifecycle.tenant_id,
                                    "lifecycle replay after lag failed"
                                );
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        } else {
            tokio::select! {
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        tenant_id = %lifecycle.tenant_id,
                        session_id = ?lifecycle.session_id,
                        "closing raw lifecycle socket because node is draining"
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
                    Ok(event) if event.cursor <= cursor => continue,
                    Ok(event)
                        if lifecycle
                            .session_id
                            .as_ref()
                            .is_some_and(|session_id| event.event.session_id() != session_id) =>
                    {
                        continue;
                    }
                    Ok(event) => {
                        cursor = event.cursor;
                        let started_at = Instant::now();
                        let result = send_lifecycle(&mut socket, &event).await;
                        record_read_result(&state, ReadOperation::LifecycleLive, started_at, result);

                        if result.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            tenant_id = lifecycle.tenant_id,
                            cursor,
                            "lifecycle broadcast lagged, replaying from store"
                        );

                        match sync_lifecycle(&mut socket, &state, &lifecycle, cursor).await {
                            Ok(next_cursor) => cursor = next_cursor,
                            Err(error) => {
                                tracing::warn!(
                                    error = ?error,
                                    tenant_id = lifecycle.tenant_id,
                                    "lifecycle replay after lag failed"
                                );
                                return;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

async fn sync_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    cursor: i64,
    batch_size: u32,
) -> Result<i64, AppError> {
    require_local_owner_for_event_path(state, session_id).await?;
    let next_cursor = replay_tail(socket, state, session_id, cursor, batch_size).await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let last_seq = resolve_session_last_seq(&state.session_store, &state.pool, session_id).await?;

    if cursor > last_seq {
        let gap = build_resume_invalidated_gap(cursor, last_seq);
        send_gap(socket, &gap)
            .await
            .map_err(|_| AppError::Internal)?;
        Ok(last_seq)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_tail(
    socket: &mut WebSocket,
    state: &AppState,
    session_id: &str,
    mut cursor: i64,
    batch_size: u32,
) -> Result<i64, AppError> {
    loop {
        let page = read_path::read_events(
            &state.hot_store,
            &state.pool,
            session_id,
            EventsOptions {
                cursor,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for events in page.events.chunks(batch_size as usize) {
            cursor = events.last().map(|event| event.seq).unwrap_or(cursor);
            let started_at = Instant::now();
            let result = send_events(socket, events).await;
            record_read_result(state, ReadOperation::TailCatchup, started_at, result);
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

async fn sync_lifecycle(
    socket: &mut WebSocket,
    state: &AppState,
    lifecycle: &LifecycleOptions,
    cursor: i64,
) -> Result<i64, AppError> {
    let next_cursor = replay_lifecycle(socket, state, lifecycle, cursor).await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let head = repository::lifecycle_head_seq(
        &state.pool,
        &lifecycle.tenant_id,
        lifecycle.session_id.as_deref(),
    )
    .await?;

    if cursor > head {
        let earliest_available_cursor = if head == 0 { 0 } else { 1 };
        let gap =
            build_resume_invalidated_gap_with_earliest(cursor, head, earliest_available_cursor);
        send_gap(socket, &gap)
            .await
            .map_err(|_| AppError::Internal)?;
        Ok(head)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_lifecycle(
    socket: &mut WebSocket,
    state: &AppState,
    lifecycle: &LifecycleOptions,
    mut cursor: i64,
) -> Result<i64, AppError> {
    loop {
        let page = repository::read_lifecycle_events(
            &state.pool,
            &lifecycle.tenant_id,
            lifecycle.session_id.as_deref(),
            EventsOptions {
                cursor,
                limit: TAIL_REPLAY_LIMIT,
            },
        )
        .await?;

        if page.events.is_empty() {
            return Ok(cursor);
        }

        for event in page.events {
            cursor = event.cursor;
            let started_at = Instant::now();
            let result = send_lifecycle(socket, &event).await;
            record_read_result(state, ReadOperation::LifecycleCatchup, started_at, result);
            result.map_err(|_| AppError::Internal)?;
        }
    }
}

fn record_read_result(
    state: &AppState,
    operation: ReadOperation,
    started_at: Instant,
    result: Result<(), ()>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(()) => {
            state
                .telemetry
                .record_read(operation, ReadPhase::Deliver, ReadOutcome::Ok, duration_ms)
        }
        Err(()) => state.telemetry.record_read(
            operation,
            ReadPhase::Deliver,
            ReadOutcome::Error,
            duration_ms,
        ),
    }
}

fn lifecycle_socket_surface(lifecycle: &LifecycleOptions) -> SocketSurface {
    if lifecycle.session_id.is_some() {
        SocketSurface::SessionLifecycle
    } else {
        SocketSurface::TenantLifecycle
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

async fn wait_for_drain(ops: &crate::ops::OpsState) {
    if ops.is_draining() {
        return;
    }

    loop {
        sleep(Duration::from_millis(100)).await;

        if ops.is_draining() {
            return;
        }
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}
