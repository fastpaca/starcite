use std::collections::HashMap;

use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use serde_json::json;
use tokio::{sync::mpsc, task::JoinHandle, time::sleep};

use crate::{
    AppState,
    api::phoenix_context::{
        SocketContext, error_reason, resolve_lifecycle_tenant_id, tail_join_error_payload,
    },
    api::phoenix_protocol::{
        LifecycleOptions, PhoenixFrame, parse_client_frame, parse_lifecycle_join_payload,
        parse_session_lifecycle_join_payload, parse_tail_join_payload, reply_frame,
    },
    api::phoenix_socket::{send_frame, send_node_draining, send_token_expired},
    api::phoenix_topics::{run_lifecycle_topic, run_tail_topic},
    app::{api, data_plane, runtime},
    auth::{self, AuthContext},
    config::CommitMode,
    error::AppError,
    runtime::RuntimeTouchReason,
    telemetry::{SocketSurface, SocketTransport},
};

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
        if let Some(expires_at) = expiry.as_mut() {
            tokio::select! {
                _ = expires_at => {
                    let _ = send_token_expired(&mut socket, subscription_targets(&subscriptions))
                        .await;
                    break;
                }
                _ = runtime::socket_support::wait_for_drain(&state.ops) => {
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
                _ = runtime::socket_support::wait_for_drain(&state.ops) => {
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
                        json!({"reason": error_reason(&error)}),
                    ));
                    return;
                }
            };

            let tenant_id = match data_plane::session_store::resolve_session_tenant_id(
                &state.session_store,
                &state.pool,
                &session_id,
            )
            .await
            {
                Ok(tenant_id) => tenant_id,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": error_reason(&error)}),
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
                    json!({"reason": error_reason(&error)}),
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
                        json!({"reason": error_reason(&error)}),
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
                        json!({"reason": error_reason(&error)}),
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
                    json!({"reason": error_reason(&error)}),
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
                    json!({"reason": error_reason(&error)}),
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
                        json!({"reason": error_reason(&error)}),
                    ));
                    return;
                }
            };

            let tenant_id = match data_plane::session_store::resolve_session_tenant_id(
                &state.session_store,
                &state.pool,
                &session_id,
            )
            .await
            {
                Ok(tenant_id) => tenant_id,
                Err(error) => {
                    let _ = outbound_tx.send(reply_frame(
                        frame.join_ref,
                        frame.ref_id,
                        frame.topic,
                        false,
                        json!({"reason": error_reason(&error)}),
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
