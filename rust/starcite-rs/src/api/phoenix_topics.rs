use std::time::Instant;

use super::{
    phoenix_protocol::{
        LifecycleOptions, PhoenixFrame, build_resume_invalidated_gap, lifecycle_payload, push_frame,
    },
    query_options::TailOptions,
    socket_support::record_read_result,
};
use serde_json::json;
use tokio::sync::{broadcast, mpsc};

use crate::{
    AppState,
    data_plane::{read_path, repository, session_store::resolve_session_last_seq},
    error::AppError,
    model::{EventResponse, EventsOptions, LifecycleResponse},
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
    let _subscription = state.telemetry.track_socket_subscription(
        SocketTransport::Phoenix,
        lifecycle_socket_surface(&lifecycle),
    );
    let _fanout_guard = match lifecycle.session_id.as_ref() {
        Some(session_id) => state.lifecycle.session_guard(session_id.clone()),
        None => state.lifecycle.tenant_guard(tenant_id.to_string()),
    };
    let mut cursor = lifecycle.cursor;

    match sync_lifecycle(
        &topic,
        &tenant_id,
        &lifecycle,
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
                    cursor,
                    "phoenix lifecycle broadcast lagged, replaying from store"
                );

                match sync_lifecycle(
                    &topic,
                    &tenant_id,
                    &lifecycle,
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
            Ok(event) if event.seq <= cursor => continue,
            Ok(event) => {
                cursor = event.seq;
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
                    cursor,
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

fn lifecycle_socket_surface(lifecycle: &LifecycleOptions) -> SocketSurface {
    if lifecycle.session_id.is_some() {
        SocketSurface::SessionLifecycle
    } else {
        SocketSurface::TenantLifecycle
    }
}

async fn sync_lifecycle(
    topic: &str,
    tenant_id: &str,
    lifecycle: &LifecycleOptions,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    cursor: i64,
) -> Result<i64, AppError> {
    let next_cursor = replay_lifecycle(
        topic,
        tenant_id,
        lifecycle,
        join_ref.clone(),
        state,
        outbound_tx,
        cursor,
    )
    .await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let head =
        repository::lifecycle_head_seq(&state.pool, tenant_id, lifecycle.session_id.as_deref())
            .await?;

    if cursor > head {
        outbound_tx
            .send(push_frame(
                join_ref,
                topic.to_string(),
                "gap",
                build_resume_invalidated_gap(cursor, head),
            ))
            .map_err(|_| AppError::Internal)?;
        Ok(head)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_lifecycle(
    topic: &str,
    tenant_id: &str,
    lifecycle: &LifecycleOptions,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    mut cursor: i64,
) -> Result<i64, AppError> {
    loop {
        let page = repository::read_lifecycle_events(
            &state.pool,
            tenant_id,
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
    cursor: i64,
) -> Result<i64, AppError> {
    let next_cursor = replay_tail(
        topic,
        session_id,
        batch_size,
        join_ref.clone(),
        state,
        outbound_tx,
        cursor,
    )
    .await?;

    if next_cursor != cursor {
        return Ok(next_cursor);
    }

    let last_seq = resolve_session_last_seq(&state.session_store, &state.pool, session_id).await?;

    if cursor > last_seq {
        let gap = build_resume_invalidated_gap(cursor, last_seq);
        outbound_tx
            .send(push_frame(join_ref, topic.to_string(), "gap", gap))
            .map_err(|_| AppError::Internal)?;
        Ok(last_seq)
    } else {
        Ok(next_cursor)
    }
}

async fn replay_tail(
    topic: &str,
    session_id: &str,
    batch_size: u32,
    join_ref: Option<String>,
    state: &AppState,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    mut cursor: i64,
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
