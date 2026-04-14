use std::{collections::HashMap, future::pending, pin::Pin};

use super::socket_support;
use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use serde_json::{Value, json};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Sleep, sleep},
};

use crate::{
    AppState, api,
    api::phoenix_context::{
        SocketContext, error_reason, resolve_lifecycle_tenant_id, tail_join_error_payload,
    },
    api::phoenix_protocol::{
        LifecycleOptions, PhoenixFrame, parse_client_frame, parse_lifecycle_join_payload,
        parse_session_lifecycle_join_payload, parse_tail_join_payload, reply_frame,
    },
    api::phoenix_socket::{send_frame, send_node_draining, send_token_expired},
    api::phoenix_topics::{run_lifecycle_topic, run_tail_topic},
    auth::{self, AuthContext},
    config::CommitMode,
    data_plane,
    error::AppError,
    runtime::RuntimeTouchReason,
    telemetry::{SocketSurface, SocketTransport},
};

type SocketExpiry = Pin<Box<Sleep>>;

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
    let auth = api::request_metrics::authenticate_socket(&state, &params)?;
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
        let _ = send_node_draining(
            &mut socket,
            subscription_targets(&subscriptions),
            &state.ops,
        )
        .await;
        return;
    }

    loop {
        tokio::select! {
            _ = wait_for_optional_expiry(expiry.as_mut()) => {
                let _ = send_token_expired(&mut socket, subscription_targets(&subscriptions))
                    .await;
                break;
            }
            _ = socket_support::wait_for_drain(&state.ops) => {
                tracing::info!(
                    topic_count = subscriptions.len(),
                    "closing phoenix socket because node is draining"
                );
                let _ = send_node_draining(
                    &mut socket,
                    subscription_targets(&subscriptions),
                    &state.ops,
                )
                .await;
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
                if !handle_socket_message(
                    &mut socket,
                    incoming,
                    &state,
                    &context,
                    &outbound_tx,
                    &mut subscriptions,
                )
                .await
                {
                    break;
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
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            json!({"reason": "already_joined"}),
        );
        return;
    }

    if frame.topic.starts_with("lifecycle:") {
        handle_session_lifecycle_join(frame, state, context, outbound_tx, subscriptions).await;
    } else if frame.topic == "lifecycle" {
        handle_lifecycle_join(frame, state, context, outbound_tx, subscriptions).await;
    } else if frame.topic.starts_with("tail:") {
        handle_tail_join(frame, state, context, outbound_tx, subscriptions).await;
    } else {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            json!({"reason": "invalid_topic"}),
        );
    }
}

async fn handle_session_lifecycle_join(
    frame: PhoenixFrame,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    let session_id = match parse_topic_session_id(&frame.topic, "lifecycle:") {
        Some(session_id) => session_id,
        None => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": "invalid_session_id"}),
            );
            return;
        }
    };

    let cursor = match parse_session_lifecycle_join_payload(&frame.payload) {
        Ok(cursor) => cursor,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };

    let tenant_id = match resolve_session_tenant_id(state, &session_id).await {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };

    if let Err(error) = auth::allow_read_session(&context.auth, &session_id, &tenant_id) {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            json!({"reason": error_reason(&error)}),
        );
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
    spawn_lifecycle_subscription(
        frame,
        outbound_tx,
        subscriptions,
        tenant_id,
        lifecycle,
        state.clone(),
        receiver,
    );
}

async fn handle_lifecycle_join(
    frame: PhoenixFrame,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    let tenant_id = match resolve_lifecycle_tenant_id(context, &frame.payload) {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };
    let lifecycle = match parse_lifecycle_join_payload(&frame.payload) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };

    if let Err(error) = validate_lifecycle_scope(state, &context.auth, &tenant_id, &lifecycle).await
    {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            json!({"reason": error_reason(&error)}),
        );
        return;
    }

    if let Some(session_id) = lifecycle.session_id.as_deref() {
        state
            .runtime
            .touch_existing(session_id, &tenant_id, RuntimeTouchReason::PhoenixLifecycle)
            .await;
    }

    let receiver = state.lifecycle.subscribe_tenant(&tenant_id).await;
    spawn_lifecycle_subscription(
        frame,
        outbound_tx,
        subscriptions,
        tenant_id,
        lifecycle,
        state.clone(),
        receiver,
    );
}

async fn handle_tail_join(
    frame: PhoenixFrame,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    let session_id = match parse_topic_session_id(&frame.topic, "tail:") {
        Some(session_id) => session_id,
        None => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": "invalid_session_id"}),
            );
            return;
        }
    };

    if let Err(error) = auth::allowed_to_access_session(&context.auth, &session_id) {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            json!({"reason": error_reason(&error)}),
        );
        return;
    }

    let tail = match parse_tail_join_payload(&frame.payload) {
        Ok(tail) => tail,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };

    let tenant_id = match resolve_session_tenant_id(state, &session_id).await {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            let _ = send_reply(
                outbound_tx,
                &frame,
                false,
                json!({"reason": error_reason(&error)}),
            );
            return;
        }
    };

    if let Err(error) = auth::allow_read_session(&context.auth, &session_id, &tenant_id) {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            tail_join_error_payload(&error, context),
        );
        return;
    }

    if state.commit_mode == CommitMode::LocalAsync
        && let Err(error) = state.ownership.live_or_renew_owned(&session_id).await
    {
        let _ = send_reply(
            outbound_tx,
            &frame,
            false,
            tail_join_error_payload(&error, context),
        );
        return;
    }

    state
        .runtime
        .touch_existing(&session_id, &tenant_id, RuntimeTouchReason::PhoenixTail)
        .await;
    let receiver = state.fanout.subscribe(&session_id).await;
    spawn_tail_subscription(
        frame,
        outbound_tx,
        subscriptions,
        session_id,
        tail,
        state.clone(),
        receiver,
    );
}

fn handle_leave(
    frame: PhoenixFrame,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) {
    if let Some(subscription) = subscriptions.remove(&frame.topic) {
        subscription.task.abort();
    }

    let _ = send_reply(outbound_tx, &frame, true, json!({}));
}

fn subscription_targets(
    subscriptions: &HashMap<String, TopicSubscription>,
) -> impl Iterator<Item = (&str, Option<String>)> + '_ {
    subscriptions
        .iter()
        .map(|(topic, subscription)| (topic.as_str(), subscription.join_ref.clone()))
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

    let scoped_tenant_id = data_plane::session_store::resolve_session_tenant_id(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await?;

    if scoped_tenant_id != tenant_id {
        return Err(AppError::ForbiddenTenant);
    }

    auth::allow_read_session(auth, session_id, &scoped_tenant_id)
}

async fn handle_socket_message(
    socket: &mut WebSocket,
    incoming: Option<Result<Message, axum::Error>>,
    state: &AppState,
    context: &SocketContext,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
) -> bool {
    match incoming {
        Some(Ok(Message::Text(text))) => {
            handle_text_frame(text.as_str(), state, context, outbound_tx, subscriptions).await
        }
        Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await.is_ok(),
        Some(Ok(Message::Close(_))) | None => false,
        Some(Ok(_)) => true,
        Some(Err(error)) => {
            tracing::warn!(error = ?error, "phoenix socket receive failed");
            false
        }
    }
}

fn send_reply(
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    frame: &PhoenixFrame,
    success: bool,
    payload: Value,
) -> bool {
    outbound_tx
        .send(reply_frame(
            frame.join_ref.clone(),
            frame.ref_id.clone(),
            frame.topic.clone(),
            success,
            payload,
        ))
        .is_ok()
}

fn parse_topic_session_id(topic: &str, prefix: &str) -> Option<String> {
    topic
        .strip_prefix(prefix)
        .filter(|session_id| !session_id.is_empty())
        .map(str::to_string)
}

async fn resolve_session_tenant_id(state: &AppState, session_id: &str) -> Result<String, AppError> {
    data_plane::session_store::resolve_session_tenant_id(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await
}

fn spawn_lifecycle_subscription(
    frame: PhoenixFrame,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    tenant_id: String,
    lifecycle: LifecycleOptions,
    state: AppState,
    receiver: tokio::sync::broadcast::Receiver<crate::model::LifecycleResponse>,
) {
    let join_ref = frame.join_ref.clone();
    let topic = frame.topic.clone();

    let _ = send_reply(outbound_tx, &frame, true, json!({}));

    let task = tokio::spawn(run_lifecycle_topic(
        topic.clone(),
        tenant_id,
        lifecycle,
        join_ref.clone(),
        state,
        receiver,
        outbound_tx.clone(),
    ));

    subscriptions.insert(topic, TopicSubscription { join_ref, task });
}

fn spawn_tail_subscription(
    frame: PhoenixFrame,
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    subscriptions: &mut HashMap<String, TopicSubscription>,
    session_id: String,
    tail: api::query_options::TailOptions,
    state: AppState,
    receiver: tokio::sync::broadcast::Receiver<crate::model::EventResponse>,
) {
    let join_ref = frame.join_ref.clone();
    let topic = frame.topic.clone();

    let _ = send_reply(outbound_tx, &frame, true, json!({}));

    let task = tokio::spawn(run_tail_topic(
        topic.clone(),
        session_id,
        tail,
        join_ref.clone(),
        state,
        receiver,
        outbound_tx.clone(),
    ));

    subscriptions.insert(topic, TopicSubscription { join_ref, task });
}

async fn wait_for_optional_expiry(expiry: Option<&mut SocketExpiry>) {
    match expiry {
        Some(expiry) => expiry.await,
        None => pending::<()>().await,
    }
}
