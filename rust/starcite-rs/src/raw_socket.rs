use axum::extract::ws::{CloseFrame, Message, WebSocket, close_code};
use serde::Serialize;

use crate::{
    model::{EventResponse, LifecycleResponse},
    ops::{OpsSnapshot, OpsState},
};

#[derive(Debug, Clone, Serialize, PartialEq)]
struct TailEventsFrame {
    events: Vec<EventResponse>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct TokenExpiredFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct NodeDrainingFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain_source: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_after_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct TailGapFrame {
    #[serde(rename = "type")]
    frame_type: &'static str,
    reason: &'static str,
    from_cursor: i64,
    next_cursor: i64,
    committed_cursor: i64,
    earliest_available_cursor: i64,
}

pub(crate) fn build_resume_invalidated_gap(from_cursor: i64, last_seq: i64) -> TailGapFrame {
    build_resume_invalidated_gap_with_earliest(from_cursor, last_seq, 1)
}

pub(crate) fn build_resume_invalidated_gap_with_earliest(
    from_cursor: i64,
    last_seq: i64,
    earliest_available_cursor: i64,
) -> TailGapFrame {
    TailGapFrame {
        frame_type: "gap",
        reason: "resume_invalidated",
        from_cursor,
        next_cursor: last_seq,
        committed_cursor: last_seq,
        earliest_available_cursor,
    }
}

pub(crate) async fn send_events(
    socket: &mut WebSocket,
    events: &[EventResponse],
) -> Result<(), ()> {
    let message = serde_json::to_string(&TailEventsFrame {
        events: events.to_vec(),
    })
    .map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

pub(crate) async fn send_lifecycle(
    socket: &mut WebSocket,
    event: &LifecycleResponse,
) -> Result<(), ()> {
    let message = serde_json::to_string(event).map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

pub(crate) async fn send_gap(socket: &mut WebSocket, gap: &TailGapFrame) -> Result<(), ()> {
    let message = serde_json::to_string(gap).map_err(|_| ())?;
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())
}

pub(crate) async fn send_token_expired(socket: &mut WebSocket) -> Result<(), ()> {
    let message = serde_json::to_string(&build_token_expired_frame()).map_err(|_| ())?;
    send_terminal_message(socket, message, close_code::POLICY, "token_expired").await
}

pub(crate) async fn send_node_draining(socket: &mut WebSocket, ops: &OpsState) -> Result<(), ()> {
    let message =
        serde_json::to_string(&build_node_draining_frame(&ops.snapshot())).map_err(|_| ())?;
    send_terminal_message(socket, message, close_code::RESTART, "node_draining").await
}

fn build_token_expired_frame() -> TokenExpiredFrame {
    TokenExpiredFrame {
        frame_type: "token_expired",
        reason: "token_expired",
    }
}

fn build_node_draining_frame(ops: &OpsSnapshot) -> NodeDrainingFrame {
    NodeDrainingFrame {
        frame_type: "node_draining",
        reason: "node_draining",
        drain_source: ops.drain_source,
        retry_after_ms: ops.retry_after_ms,
    }
}

async fn send_terminal_message(
    socket: &mut WebSocket,
    message: String,
    code: u16,
    reason: &'static str,
) -> Result<(), ()> {
    socket
        .send(Message::Text(message.into()))
        .await
        .map_err(|_| ())?;
    socket
        .send(Message::Close(Some(CloseFrame {
            code: code.into(),
            reason: reason.into(),
        })))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::{
        build_node_draining_frame, build_resume_invalidated_gap, build_token_expired_frame,
    };
    use crate::ops::OpsState;

    #[test]
    fn resume_invalidated_gap_uses_public_shape() {
        let gap = build_resume_invalidated_gap(10, 2);

        assert_eq!(gap.frame_type, "gap");
        assert_eq!(gap.reason, "resume_invalidated");
        assert_eq!(gap.from_cursor, 10);
        assert_eq!(gap.next_cursor, 2);
        assert_eq!(gap.committed_cursor, 2);
        assert_eq!(gap.earliest_available_cursor, 1);
    }

    #[test]
    fn token_expired_frame_uses_public_shape() {
        let frame = build_token_expired_frame();

        assert_eq!(frame.frame_type, "token_expired");
        assert_eq!(frame.reason, "token_expired");
    }

    #[test]
    fn node_draining_frame_uses_public_shape() {
        let ops_state = OpsState::new(5_000);
        ops_state.begin_shutdown_drain();
        let frame = build_node_draining_frame(&ops_state.snapshot());

        assert_eq!(frame.frame_type, "node_draining");
        assert_eq!(frame.reason, "node_draining");
        assert_eq!(frame.drain_source, Some("shutdown"));
        assert!(frame.retry_after_ms.is_some());
    }
}
