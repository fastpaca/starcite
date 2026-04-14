use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use serde_json::{Value, json};

use crate::{
    ops::{OpsSnapshot, OpsState},
    phoenix_protocol::{PhoenixFrame, push_frame},
};

pub(crate) async fn send_frame(socket: &mut WebSocket, frame: &PhoenixFrame) -> Result<(), ()> {
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

pub(crate) async fn send_token_expired<'a, I>(
    socket: &mut WebSocket,
    subscriptions: I,
) -> Result<(), ()>
where
    I: IntoIterator<Item = (&'a str, Option<String>)>,
{
    let payload = json!({"reason": "token_expired"});
    send_terminal_notice(
        socket,
        subscriptions,
        "token_expired",
        &payload,
        close_code::POLICY,
        "token_expired",
    )
    .await
}

pub(crate) async fn send_node_draining<'a, I>(
    socket: &mut WebSocket,
    subscriptions: I,
    ops: &OpsState,
) -> Result<(), ()>
where
    I: IntoIterator<Item = (&'a str, Option<String>)>,
{
    let payload = build_node_draining_payload(&ops.snapshot());
    send_terminal_notice(
        socket,
        subscriptions,
        "node_draining",
        &payload,
        close_code::RESTART,
        "node_draining",
    )
    .await
}

async fn send_terminal_notice<'a, I>(
    socket: &mut WebSocket,
    subscriptions: I,
    event: &'static str,
    payload: &Value,
    code: u16,
    reason: &'static str,
) -> Result<(), ()>
where
    I: IntoIterator<Item = (&'a str, Option<String>)>,
{
    for (topic, join_ref) in subscriptions {
        send_frame(
            socket,
            &push_frame(join_ref, topic.to_string(), event, payload.clone()),
        )
        .await?;
    }

    send_socket_close(socket, code, reason).await
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

fn build_node_draining_payload(ops: &OpsSnapshot) -> Value {
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::build_node_draining_payload;
    use crate::ops::OpsSnapshot;

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
