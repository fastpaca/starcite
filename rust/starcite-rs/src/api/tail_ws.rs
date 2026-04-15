use std::{collections::HashMap, future::pending, pin::Pin, time::Instant};

use axum::{
    extract::{
        Path, Query, State,
        ws::{
            Message, WebSocket, WebSocketUpgrade, close_code, rejection::WebSocketUpgradeRejection,
        },
    },
    http::HeaderMap,
    response::IntoResponse,
};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{Sleep, sleep},
};

use crate::{
    AppState, api,
    api::{
        phoenix_socket::build_node_draining_payload,
        phoenix_topics::{TailStreamFrame, run_tail_stream},
        socket_support::{record_read_result, wait_for_drain},
    },
    auth, data_plane,
    error::AppError,
    runtime::RuntimeTouchReason,
    telemetry::{SocketSurface, SocketTransport},
};

type SocketExpiry = Pin<Box<Sleep>>;

pub async fn tail(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    websocket: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Result<impl IntoResponse, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let websocket = require_websocket_upgrade(websocket)?;
    let tail = api::query_options::parse_tail_options(&params)?;
    let auth = api::request_metrics::authenticate_raw_socket(&state, &headers, &params).await?;

    auth::allowed_to_access_session(&auth, &session_id)?;

    let tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    state.ownership.live_or_renew_owned(&session_id).await?;

    touch_existing_session(&state, &session_id, &tenant_id, RuntimeTouchReason::RawTail).await;

    Ok(websocket.on_upgrade(move |socket| async move {
        run_tail_socket(socket, state, session_id, tail, auth).await;
    }))
}

async fn run_tail_socket(
    mut socket: WebSocket,
    state: AppState,
    session_id: String,
    tail: api::query_options::TailOptions,
    auth: auth::AuthContext,
) {
    let _connection = state
        .telemetry
        .track_socket_connection(SocketTransport::Raw, SocketSurface::Tail);
    let receiver = state.fanout.subscribe(&session_id).await;
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<String>();
    let stream_task = spawn_tail_stream(
        state.clone(),
        session_id.clone(),
        tail,
        receiver,
        outbound_tx,
    );
    let mut expiry = auth.expiry_delay().map(|delay| Box::pin(sleep(delay)));

    if state.ops.is_draining() {
        let _ = send_node_draining(&mut socket, &state.ops.snapshot()).await;
        stream_task.abort();
        let _ = stream_task.await;
        return;
    }

    loop {
        tokio::select! {
            _ = wait_for_optional_expiry(expiry.as_mut()) => {
                let _ = send_token_expired_close(&mut socket).await;
                break;
            }
            _ = wait_for_drain(&state.ops) => {
                tracing::info!(session_id, "closing raw tail socket because node is draining");
                let _ = send_node_draining(&mut socket, &state.ops.snapshot()).await;
                break;
            }
            outbound = outbound_rx.recv() => match outbound {
                Some(payload) => {
                    if socket.send(Message::Text(payload.into())).await.is_err() {
                        break;
                    }
                }
                None => break,
            },
            incoming = socket.recv() => {
                if !handle_socket_message(&mut socket, incoming).await {
                    break;
                }
            }
        }
    }

    stream_task.abort();
    let _ = stream_task.await;
}

fn spawn_tail_stream(
    state: AppState,
    session_id: String,
    tail: api::query_options::TailOptions,
    receiver: tokio::sync::broadcast::Receiver<crate::model::EventResponse>,
    outbound_tx: mpsc::UnboundedSender<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let emit_state = state.clone();

        run_tail_stream(
            session_id.clone(),
            tail,
            state,
            receiver,
            move |frame, operation| match frame {
                TailStreamFrame::Events(events) => {
                    let started_at = Instant::now();
                    let payload = api::public_payload::tail_events_text(&events, tail.batch_size)?;
                    let result = outbound_tx.send(payload);

                    if let Some(operation) = operation {
                        record_read_result(
                            &emit_state,
                            operation,
                            started_at,
                            result.as_ref().map(|_| ()).map_err(|_| ()),
                        );
                    }

                    result.map_err(|_| AppError::Internal)
                }
                TailStreamFrame::Gap(gap) => outbound_tx
                    .send(api::public_payload::raw_tail_gap_text(&gap)?)
                    .map_err(|_| AppError::Internal),
            },
        )
        .await;
    })
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
            tracing::warn!(error = ?error, "raw tail socket receive failed");
            false
        }
    }
}

async fn send_node_draining(
    socket: &mut WebSocket,
    ops: &crate::runtime::OpsSnapshot,
) -> Result<(), ()> {
    let payload = serde_json::to_string(&build_node_draining_payload(ops)).map_err(|_| ())?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| ())?;
    send_close(socket, close_code::RESTART, "node_draining").await
}

async fn send_token_expired_close(socket: &mut WebSocket) -> Result<(), ()> {
    send_close(socket, 4001, "token_expired").await
}

async fn send_close(socket: &mut WebSocket, code: u16, reason: &'static str) -> Result<(), ()> {
    socket
        .send(Message::Close(Some(axum::extract::ws::CloseFrame {
            code,
            reason: reason.into(),
        })))
        .await
        .map_err(|_| ())
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

fn require_websocket_upgrade(
    websocket: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Result<WebSocketUpgrade, AppError> {
    websocket.map_err(|_| AppError::InvalidWebsocketUpgrade)
}

async fn wait_for_optional_expiry(expiry: Option<&mut SocketExpiry>) {
    match expiry {
        Some(expiry) => expiry.await,
        None => pending::<()>().await,
    }
}
