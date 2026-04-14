use std::collections::HashMap;
use std::time::Instant;

use axum::{
    extract::{
        Query, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    response::IntoResponse,
};
use serde_json::{Value, json};
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time::sleep,
};

use crate::{
    AppState,
    auth::{self, AuthContext},
    config::{AuthMode, CommitMode},
    error::AppError,
    model::{EventResponse, EventsOptions, LifecycleResponse},
    owner_proxy::build_phoenix_socket_ws_url,
    phoenix_protocol::{
        LifecycleOptions, PhoenixFrame, build_resume_invalidated_gap, lifecycle_payload,
        parse_client_frame, parse_lifecycle_join_payload, parse_session_lifecycle_join_payload,
        parse_tail_join_payload, push_frame, reply_frame,
    },
    query_options::TailOptions,
    read_path, repository,
    request_metrics::authenticate_socket,
    runtime::RuntimeTouchReason,
    session_store::{resolve_session_last_seq, resolve_session_tenant_id},
    socket_support::{record_read_result, wait_for_drain},
    telemetry::{ReadOperation, SocketSurface, SocketTransport},
};

const TAIL_REPLAY_LIMIT: u32 = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SocketContext {
    auth: AuthContext,
    tenant_id: Option<String>,
    connect_params: HashMap<String, String>,
}

#[derive(Debug)]
struct TopicSubscription {
    join_ref: Option<String>,
    task: JoinHandle<()>,
}

pub async fn socket(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    websocket: WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    let auth = authenticate_socket(&state, &params)?;
    let context = SocketContext {
        auth,
        tenant_id: params
            .get("tenant_id")
            .filter(|value| !value.is_empty())
            .cloned(),
        connect_params: params,
    };

    Ok(websocket.on_upgrade(move |socket| async move {
        run_socket(socket, state, context).await;
    }))
}

async fn run_socket(mut socket: WebSocket, state: AppState, context: SocketContext) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Phoenix, SocketSurface::Socket);
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<PhoenixFrame>();
    let mut subscriptions = HashMap::<String, TopicSubscription>::new();
    let mut expiry = context
        .auth
        .expiry_delay()
        .map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining_frames(&mut socket, &subscriptions, &state.ops).await;
        return;
    }

    loop {
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
                    let _ = send_token_expired_frames(&mut socket, &subscriptions).await;
                    break;
                }
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        topic_count = subscriptions.len(),
                        "closing phoenix socket because node is draining"
                    );
                    let _ = send_node_draining_frames(&mut socket, &subscriptions, &state.ops).await;
                    break;
                }
                outbound = outbound_rx.recv() => {
                    match outbound {
                        Some(frame) => {
                            if send_frame(&mut socket, &frame).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                incoming = socket.recv() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            if !handle_text_frame(
                                text.as_str(),
                                &state,
                                &context,
                                &outbound_tx,
                                &mut subscriptions,
                            ).await {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(error = ?error, "phoenix socket receive failed");
                            break;
                        }
                    }
                }
            }
        } else {
            tokio::select! {
                _ = wait_for_drain(&state.ops) => {
                    tracing::info!(
                        topic_count = subscriptions.len(),
                        "closing phoenix socket because node is draining"
                    );
                    let _ = send_node_draining_frames(&mut socket, &subscriptions, &state.ops).await;
                    break;
                }
                outbound = outbound_rx.recv() => {
                    match outbound {
                        Some(frame) => {
                            if send_frame(&mut socket, &frame).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                incoming = socket.recv() => {
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            if !handle_text_frame(
                                text.as_str(),
                                &state,
                                &context,
                                &outbound_tx,
                                &mut subscriptions,
                            ).await {
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            if socket.send(Message::Pong(payload)).await.is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            tracing::warn!(error = ?error, "phoenix socket receive failed");
                            break;
                        }
                    }
                }
            }
        }
    }

    for subscription in subscriptions.into_values() {
        subscription.task.abort();
    }
}

async fn handle_text_frame(
    raw: &str,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) -> bool {
    let frame = match parse_client_frame(raw) {
        Ok(frame) => frame,
        Err(error) => {
            tracing::warn!(error = ?error, "invalid phoenix frame");
            return false;
        }
    };

    match frame.event.as_str() {
        "heartbeat" if frame.topic == "phoenix" => outbound_tx
            .send(reply_frame(
                frame.join_ref,
                frame.ref_id,
                frame.topic,
                true,
                json!({}),
            ))
            .is_ok(),
        "phx_join" => {
            handle_join(frame, state, context, outbound_tx, subscriptions).await;
            true
        }
        "phx_leave" => {
            handle_leave(frame, outbound_tx, subscriptions);
            true
        }
        _ => true,
    }
}

async fn handle_join(
    frame: PhoenixFrame,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    if subscriptions.contains_key(&frame.topic) {
        let _ = outbound_tx.send(reply_frame(
            frame.join_ref,
            frame.ref_id,
            frame.topic,
            false,
            json!({"reason": "already_joined"}),
        ));
        return;
    }

    match frame.topic.as_str() {
        topic if topic.starts_with("lifecycle:") => {
            let session_id = match topic.strip_prefix("lifecycle:") {
                Some(session_id) if !session_id.is_empty() => session_id.to_string(),
                _ => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": "invalid_session_id"}),
                    ));
                    return;
                }
            };

            let cursor = match parse_session_lifecycle_join_payload(&frame.payload) {
                Ok(cursor) => cursor,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": reason_for_error(&error)}),
                    ));
                    return;
                }
            };

            let tenant_id =
                match resolve_session_tenant_id(&state.session_store, &state.pool, &session_id)
                    .await
                {
                    Ok(tenant_id) => tenant_id,
                    Err(error) => {
                        let _ = outbound_tx.send(reply_frame(
                            frame.join_ref,
                            frame.ref_id,
                            frame.topic,
                            false,
                            json!({"reason": reason_for_error(&error)}),
                        ));
                        return;
                    }
                };

            if let Err(error) = auth::allow_read_session(&context.auth, &session_id, &tenant_id) {
                let _ = outbound_tx.send(reply_frame(
                    frame.join_ref,
                    frame.ref_id,
                    frame.topic,
                    false,
                    json!({"reason": reason_for_error(&error)}),
                ));
                return;
            }

            let lifecycle = LifecycleOptions {
                cursor,
                session_id: Some(session_id.clone()),
            };
            state
                .runtime
                .touch_existing(
                    &session_id,
                    &tenant_id,
                    RuntimeTouchReason::PhoenixLifecycle,
                )
                .await;
            let receiver = state.lifecycle.subscribe_session(&session_id).await;
            let join_ref = frame.join_ref.clone();
            let topic = frame.topic.clone();
            let state = state.clone();

            let _ = outbound_tx.send(reply_frame(
                join_ref.clone(),
                frame.ref_id,
                frame.topic.clone(),
                true,
                json!({}),
            ));

            let task = tokio::spawn(run_lifecycle_topic(
                topic,
                tenant_id,
                lifecycle,
                join_ref.clone(),
                state,
                receiver,
                outbound_tx.clone(),
            ));

            subscriptions.insert(frame.topic, TopicSubscription { join_ref, task });
        }
        "lifecycle" => {
            let tenant_id = match resolve_lifecycle_tenant_id(context, &frame.payload) {
                Ok(tenant_id) => tenant_id,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": reason_for_error(&error)}),
                    ));
                    return;
                }
            };
            let lifecycle = match parse_lifecycle_join_payload(&frame.payload) {
                Ok(lifecycle) => lifecycle,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": reason_for_error(&error)}),
                    ));
                    return;
                }
            };
            if let Err(error) =
                validate_lifecycle_scope(state, &context.auth, &tenant_id, &lifecycle).await
            {
                let _ = outbound_tx.send(reply_frame(
                    frame.join_ref,
                    frame.ref_id,
                    frame.topic,
                    false,
                    json!({"reason": reason_for_error(&error)}),
                ));
                return;
            }

            if let Some(session_id) = lifecycle.session_id.as_deref() {
                state
                    .runtime
                    .touch_existing(session_id, &tenant_id, RuntimeTouchReason::PhoenixLifecycle)
                    .await;
            }

            let receiver = state.lifecycle.subscribe_tenant(&tenant_id).await;
            let join_ref = frame.join_ref.clone();
            let topic = frame.topic.clone();
            let state = state.clone();

            let _ = outbound_tx.send(reply_frame(
                join_ref.clone(),
                frame.ref_id,
                frame.topic.clone(),
                true,
                json!({}),
            ));

            let task = tokio::spawn(run_lifecycle_topic(
                topic,
                tenant_id,
                lifecycle,
                join_ref.clone(),
                state,
                receiver,
                outbound_tx.clone(),
            ));

            subscriptions.insert(frame.topic, TopicSubscription { join_ref, task });
        }
        topic if topic.starts_with("tail:") => {
            let session_id = match topic.strip_prefix("tail:") {
                Some(session_id) if !session_id.is_empty() => session_id.to_string(),
                _ => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": "invalid_session_id"}),
                    ));
                    return;
                }
            };

            if let Err(error) = auth::allowed_to_access_session(&context.auth, &session_id) {
                let _ = outbound_tx.send(reply_frame(
                    frame.join_ref,
                    frame.ref_id,
                    frame.topic,
                    false,
                    json!({"reason": reason_for_error(&error)}),
                ));
                return;
            }

            let tail = match parse_tail_join_payload(&frame.payload) {
                Ok(tail) => tail,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": reason_for_error(&error)}),
                    ));
                    return;
                }
            };

            let tenant_id =
                match resolve_session_tenant_id(&state.session_store, &state.pool, &session_id)
                    .await
                {
                    Ok(tenant_id) => tenant_id,
                    Err(error) => {
                        let _ = outbound_tx.send(reply_frame(
                            frame.join_ref,
                            frame.ref_id,
                            frame.topic,
                            false,
                            json!({"reason": reason_for_error(&error)}),
                        ));
                        return;
                    }
                };

            if let Err(error) = auth::allow_read_session(&context.auth, &session_id, &tenant_id) {
                let _ = outbound_tx.send(reply_frame(
                    frame.join_ref,
                    frame.ref_id,
                    frame.topic,
                    false,
                    tail_join_error_payload(&error, context),
                ));
                return;
            }

            if state.commit_mode == CommitMode::LocalAsync
                && let Err(error) = state.ownership.live_or_renew_owned(&session_id).await
            {
                let _ = outbound_tx.send(reply_frame(
                    frame.join_ref,
                    frame.ref_id,
                    frame.topic,
                    false,
                    tail_join_error_payload(&error, context),
                ));
                return;
            }

            state
                .runtime
                .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::PhoenixTail)
                .await;
            let receiver = state.fanout.subscribe(&session_id).await;
            let join_ref = frame.join_ref.clone();
            let topic = frame.topic.clone();
            let state = state.clone();

            let _ = outbound_tx.send(reply_frame(
                join_ref.clone(),
                frame.ref_id,
                frame.topic.clone(),
                true,
                json!({}),
            ));

            let task = tokio::spawn(run_tail_topic(
                topic,
                session_id,
                tail,
                join_ref.clone(),
                state,
                receiver,
                outbound_tx.clone(),
            ));

            subscriptions.insert(frame.topic, TopicSubscription { join_ref, task });
        }
        _ => {
            let _ = outbound_tx.send(reply_frame(
                frame.join_ref,
                frame.ref_id,
                frame.topic,
                false,
                json!({"reason": "invalid_topic"}),
            ));
        }
    }
}

fn handle_leave(
    frame: PhoenixFrame,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    if let Some(subscription) = subscriptions.remove(&frame.topic) {
        subscription.task.abort();
    }

    let _ = outbound_tx.send(reply_frame(
        frame.join_ref,
        frame.ref_id,
        frame.topic,
        true,
        json!({}),
    ));
}

async fn run_lifecycle_topic(
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

async fn run_tail_topic(
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

fn resolve_lifecycle_tenant_id(
    context: &SocketContext,
    payload: &Value,
) -> Result<String, AppError> {
    if context.auth.kind == AuthMode::UnsafeJwt {
        auth::can_subscribe_lifecycle(&context.auth)?;
        return Ok(context.auth.principal.tenant_id.clone());
    }

    if let Some(tenant_id) = context.tenant_id.as_ref() {
        return Ok(tenant_id.clone());
    }

    match payload.get("tenant_id").and_then(Value::as_str) {
        Some(tenant_id) if !tenant_id.is_empty() => Ok(tenant_id.to_string()),
        _ => Err(AppError::InvalidTenantId),
    }
}

fn lifecycle_socket_surface(lifecycle: &LifecycleOptions) -> SocketSurface {
    if lifecycle.session_id.is_some() {
        SocketSurface::SessionLifecycle
    } else {
        SocketSurface::TenantLifecycle
    }
}

async fn validate_lifecycle_scope(
    state: &AppState,
    auth: &AuthContext,
    tenant_id: &str,
    lifecycle: &LifecycleOptions,
) -> Result<(), AppError> {
    let Some(session_id) = lifecycle.session_id.as_ref() else {
        return Ok(());
    };

    let scoped_tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, session_id).await?;

    if scoped_tenant_id != tenant_id {
        return Err(AppError::ForbiddenTenant);
    }

    auth::allow_read_session(auth, session_id, &scoped_tenant_id)
}

async fn send_token_expired_frames(
    socket: &mut WebSocket,
    subscriptions: &HashMap<String, TopicSubscription>,
) -> Result<(), ()> {
    for (topic, subscription) in subscriptions {
        send_frame(
            socket,
            &push_frame(
                subscription.join_ref.clone(),
                topic.clone(),
                "token_expired",
                json!({"reason": "token_expired"}),
            ),
        )
        .await?;
    }

    send_socket_close(socket, close_code::POLICY, "token_expired").await
}

async fn send_node_draining_frames(
    socket: &mut WebSocket,
    subscriptions: &HashMap<String, TopicSubscription>,
    ops: &crate::ops::OpsState,
) -> Result<(), ()> {
    let snapshot = ops.snapshot();

    for (topic, subscription) in subscriptions {
        send_frame(
            socket,
            &push_frame(
                subscription.join_ref.clone(),
                topic.clone(),
                "node_draining",
                build_node_draining_payload(&snapshot),
            ),
        )
        .await?;
    }

    send_socket_close(socket, close_code::RESTART, "node_draining").await
}

fn build_node_draining_payload(ops: &crate::ops::OpsSnapshot) -> Value {
    let mut payload = serde_json::Map::from_iter([(
        "reason".to_string(),
        Value::String("node_draining".to_string()),
    )]);

    if let Some(drain_source) = ops.drain_source {
        payload.insert(
            "drain_source".to_string(),
            Value::String(drain_source.to_string()),
        );
    }

    if let Some(retry_after_ms) = ops.retry_after_ms {
        payload.insert(
            "retry_after_ms".to_string(),
            Value::Number(retry_after_ms.into()),
        );
    }

    Value::Object(payload)
}

async fn send_socket_close(
    socket: &mut WebSocket,
    code: u16,
    reason: &'static str,
) -> Result<(), ()> {
    socket
        .send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await
        .map_err(|_| ())
}

async fn send_frame(socket: &mut WebSocket, frame: &PhoenixFrame) -> Result<(), ()> {
    let message = serde_json::to_string(&(
        frame.join_ref.clone(),
        frame.ref_id.clone(),
        frame.topic.clone(),
        frame.event.clone(),
        frame.payload.clone(),
    ))
    .map_err(|_| ())?;

    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

fn tail_join_error_payload(error: &AppError, context: &SocketContext) -> Value {
    match error {
        AppError::SessionNotOwned {
            owner_id,
            owner_public_url,
            epoch,
        } => json!({
            "reason": reason_for_error(error),
            "owner_id": owner_id,
            "owner_url": owner_public_url,
            "owner_socket_url": owner_public_url
                .as_deref()
                .and_then(|owner_url| build_phoenix_socket_ws_url(owner_url, &context.connect_params)),
            "epoch": epoch
        }),
        _ => json!({"reason": reason_for_error(error)}),
    }
}

fn reason_for_error(error: &AppError) -> &'static str {
    match error {
        AppError::MissingBearerToken => "missing_bearer_token",
        AppError::InvalidBearerToken => "invalid_bearer_token",
        AppError::TokenExpired => "token_expired",
        AppError::Forbidden => "forbidden",
        AppError::ForbiddenScope => "forbidden_scope",
        AppError::ForbiddenSession => "forbidden_session",
        AppError::ForbiddenTenant => "forbidden_tenant",
        AppError::InvalidSessionId => "invalid_session_id",
        AppError::InvalidTenantId => "invalid_tenant_id",
        AppError::InvalidCursor => "invalid_cursor",
        AppError::InvalidTailBatchSize => "invalid_tail_batch_size",
        AppError::SessionNotFound => "session_not_found",
        AppError::SessionNotOwned { .. } => "session_not_owned",
        _ => "internal_error",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{
        SocketContext, build_node_draining_payload, resolve_lifecycle_tenant_id,
        tail_join_error_payload,
    };
    use crate::{auth::AuthContext, config::AuthMode, error::AppError, ops::OpsSnapshot};

    #[test]
    fn tail_join_error_payload_includes_owner_socket_url() {
        let payload = tail_join_error_payload(
            &AppError::SessionNotOwned {
                owner_id: "node-a".to_string(),
                owner_public_url: Some("https://owner.example:4443".to_string()),
                epoch: 9,
            },
            &SocketContext {
                auth: AuthContext::none(),
                tenant_id: Some("acme".to_string()),
                connect_params: HashMap::from([
                    ("token".to_string(), "jwt-token".to_string()),
                    ("vsn".to_string(), "2.0.0".to_string()),
                ]),
            },
        );

        assert_eq!(payload["reason"], "session_not_owned");
        assert_eq!(payload["owner_url"], "https://owner.example:4443");
        assert_eq!(
            payload["owner_socket_url"],
            "wss://owner.example:4443/v1/socket/websocket?token=jwt-token&vsn=2.0.0"
        );
        assert_eq!(payload["epoch"], 9);
    }

    #[test]
    fn lifecycle_tenant_id_uses_socket_context_then_join_payload() {
        let context = SocketContext {
            auth: AuthContext::none(),
            tenant_id: Some("acme".to_string()),
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "other"}))
                .expect("context tenant id"),
            "acme"
        );

        let context = SocketContext {
            auth: AuthContext::none(),
            tenant_id: None,
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "acme"}))
                .expect("join tenant id"),
            "acme"
        );
    }

    #[test]
    fn lifecycle_tenant_id_uses_authenticated_service_tenant_in_unsafe_mode() {
        let context = SocketContext {
            auth: AuthContext {
                kind: AuthMode::UnsafeJwt,
                principal: crate::model::Principal {
                    tenant_id: "acme".to_string(),
                    id: "svc".to_string(),
                    principal_type: "service".to_string(),
                },
                scopes: vec!["session:read".to_string()],
                session_id: None,
                expires_at: Some(4_102_444_800_i64),
            },
            tenant_id: Some("other".to_string()),
            connect_params: HashMap::new(),
        };

        assert_eq!(
            resolve_lifecycle_tenant_id(&context, &json!({"tenant_id": "other"}))
                .expect("auth tenant id"),
            "acme"
        );
    }

    #[test]
    fn node_draining_payload_includes_shutdown_retry_hint() {
        let payload = build_node_draining_payload(&OpsSnapshot {
            mode: "draining",
            draining: true,
            drain_source: Some("shutdown"),
            retry_after_ms: Some(2_400),
            shutdown_drain_timeout_ms: 5_000,
        });

        assert_eq!(
            payload,
            json!({
                "reason": "node_draining",
                "drain_source": "shutdown",
                "retry_after_ms": 2400
            })
        );
    }
}
