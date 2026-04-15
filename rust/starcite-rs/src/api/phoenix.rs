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
        parse_tail_join_payload, reply_frame,
    },
    api::phoenix_socket::{send_frame, send_node_draining, send_token_expired},
    api::phoenix_topics::{run_lifecycle_topic, run_tail_topic},
    auth, data_plane,
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
    let auth = api::request_metrics::authenticate_socket(&state, &params).await?;
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
        reject_join_reason(outbound_tx, &frame, "already_joined");
        return;
    }

    if frame.topic == "lifecycle" {
        handle_lifecycle_join(frame, state, context, outbound_tx, subscriptions).await;
    } else if frame.topic.starts_with("tail:") {
        handle_tail_join(frame, state, context, outbound_tx, subscriptions).await;
    } else {
        reject_join_reason(outbound_tx, &frame, "invalid_topic");
    }
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
            reject_join_error(outbound_tx, &frame, &error);
            return;
        }
    };
    let lifecycle = match parse_lifecycle_join_payload(&frame.payload) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            reject_join_error(outbound_tx, &frame, &error);
            return;
        }
    };

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
            reject_join_reason(outbound_tx, &frame, "invalid_session_id");
            return;
        }
    };

    if let Err(error) = auth::allowed_to_access_session(&context.auth, &session_id) {
        reject_join_error(outbound_tx, &frame, &error);
        return;
    }

    let tail = match parse_tail_join_payload(&frame.payload) {
        Ok(tail) => tail,
        Err(error) => {
            reject_join_error(outbound_tx, &frame, &error);
            return;
        }
    };

    let tenant_id = match resolve_session_tenant_id(state, &session_id).await {
        Ok(tenant_id) => tenant_id,
        Err(error) => {
            reject_join_error(outbound_tx, &frame, &error);
            return;
        }
    };

    if let Err(error) = auth::allow_read_session(&context.auth, &session_id, &tenant_id) {
        reject_tail_join(outbound_tx, &frame, &error, context);
        return;
    }

    if let Err(error) = state.ownership.live_or_renew_owned(&session_id).await {
        reject_tail_join(outbound_tx, &frame, &error, context);
        return;
    }

    touch_existing_session(
        state,
        &session_id,
        &tenant_id,
        RuntimeTouchReason::PhoenixTail,
    )
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

async fn touch_existing_session(
    state: &AppState,
    session_id: &str,
    tenant_id: &str,
    reason: RuntimeTouchReason,
) {
    state
        .runtime
        .touch_existing(session_id, tenant_id, reason)
        .await;
}

fn reject_join(
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    frame: &PhoenixFrame,
    payload: Value,
) {
    let _ = send_reply(outbound_tx, frame, false, payload);
}

fn reject_join_reason(
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    frame: &PhoenixFrame,
    reason: &str,
) {
    reject_join(outbound_tx, frame, json!({ "reason": reason }));
}

fn reject_join_error(
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    frame: &PhoenixFrame,
    error: &AppError,
) {
    reject_join_reason(outbound_tx, frame, error_reason(error));
}

fn reject_tail_join(
    outbound_tx: &mpsc::UnboundedSender<PhoenixFrame>,
    frame: &PhoenixFrame,
    error: &AppError,
    context: &SocketContext,
) {
    reject_join(outbound_tx, frame, tail_join_error_payload(error, context));
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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc, time::Duration};

    use serde_json::json;
    use sqlx::postgres::PgPoolOptions;
    use tokio::sync::mpsc;

    use super::{TopicSubscription, handle_tail_join};
    use crate::{
        AppState,
        api::{
            phoenix_context::SocketContext, phoenix_protocol::PhoenixFrame,
            query_options::TailOptions,
        },
        auth::{AuthContext, AuthService},
        cluster::{
            ControlPlaneState, DirectEventRelay, OwnerProxy, OwnershipManager,
            ReplicationCoordinator,
        },
        config::{AuthMode, Config},
        data_plane::{ArchiveQueue, HotEventStore, HotSessionStore, PendingFlushQueue},
        model::{Cursor, Principal, SessionResponse},
        runtime::{
            LifecycleFanout, OpsState, SessionFanout, SessionManager, SessionManagerDeps,
            SessionRuntime,
        },
        telemetry::Telemetry,
    };

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
            OwnershipManager::new(pool.clone(), instance_id.clone(), Duration::from_secs(5), 3);
        let control_plane = ControlPlaneState::new(None, None, Duration::from_secs(5));
        let owner_proxy = OwnerProxy::new(Duration::from_millis(100), None);
        let replication =
            ReplicationCoordinator::new(instance_id.clone(), false, Duration::from_millis(100))
                .expect("replication");
        let direct_event_relay = DirectEventRelay::disabled();
        let session_manager = SessionManager::new(SessionManagerDeps {
            pool: pool.clone(),
            fanout: fanout.clone(),
            hot_store: hot_store.clone(),
            pending_flush: pending_flush.clone(),
            session_store: session_store.clone(),
            ownership: ownership.clone(),
            replication: replication.clone(),
            direct_relay: direct_event_relay.clone(),
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
            direct_event_relay,
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
            local_async_replication_timeout_ms: 100,
            local_async_replication_factor: 3,
        }
    }

    fn auth_context(tenant_id: &str, session_id: Option<&str>, scopes: &[&str]) -> AuthContext {
        AuthContext {
            kind: AuthMode::Jwt,
            principal: Principal {
                tenant_id: tenant_id.to_string(),
                id: "user-42".to_string(),
                principal_type: "user".to_string(),
            },
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            session_id: session_id.map(str::to_string),
            expires_at: Some(4_102_444_800_i64),
        }
    }

    fn tail_join_frame(session_id: &str) -> PhoenixFrame {
        PhoenixFrame {
            join_ref: Some("1".to_string()),
            ref_id: Some("2".to_string()),
            topic: format!("tail:{session_id}"),
            event: "phx_join".to_string(),
            payload: json!({
                "cursor": Cursor::zero().seq,
                "batch_size": TailOptions {
                    cursor: Cursor::zero(),
                    batch_size: 1,
                }.batch_size,
            }),
        }
    }

    fn sample_session(session_id: &str) -> SessionResponse {
        SessionResponse {
            id: session_id.to_string(),
            title: Some("Draft".to_string()),
            creator_principal: None,
            metadata: serde_json::Map::new(),
            last_seq: 0,
            created_at: "2026-04-13T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-13T00:00:00.000000Z".to_string(),
            version: 1,
            archived: false,
        }
    }

    #[tokio::test]
    async fn tail_join_rejects_session_locked_token_on_other_session() {
        let state = test_state();
        let session_id = "ses_demo";
        let frame = tail_join_frame(session_id);
        let context = SocketContext {
            auth: auth_context("acme", Some("ses_locked"), &["session:read"]),
            tenant_id: None,
            connect_params: HashMap::from([("token".to_string(), "jwt-token".to_string())]),
        };
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        let mut subscriptions = HashMap::<String, TopicSubscription>::new();

        handle_tail_join(frame, &state, &context, &outbound_tx, &mut subscriptions).await;

        let reply = outbound_rx.recv().await.expect("join reply");
        assert_eq!(reply.topic, "tail:ses_demo");
        assert_eq!(reply.event, "phx_reply");
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(reply.payload["response"]["reason"], "forbidden_session");
        assert!(subscriptions.is_empty());
    }

    #[tokio::test]
    async fn tail_join_rejects_cross_tenant_session() {
        let state = test_state();
        let session_id = "ses_demo";
        state
            .session_store
            .put_session("beta", sample_session(session_id), Some(0))
            .await;
        let frame = tail_join_frame(session_id);
        let context = SocketContext {
            auth: auth_context("acme", None, &["session:read"]),
            tenant_id: None,
            connect_params: HashMap::from([("token".to_string(), "jwt-token".to_string())]),
        };
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
        let mut subscriptions = HashMap::<String, TopicSubscription>::new();

        handle_tail_join(frame, &state, &context, &outbound_tx, &mut subscriptions).await;

        let reply = outbound_rx.recv().await.expect("join reply");
        assert_eq!(reply.topic, "tail:ses_demo");
        assert_eq!(reply.event, "phx_reply");
        assert_eq!(reply.payload["status"], "error");
        assert_eq!(reply.payload["response"]["reason"], "forbidden_tenant");
        assert!(subscriptions.is_empty());
    }
}
