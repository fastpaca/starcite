use serde::Serialize;
use serde_json::Value;

use crate::{
    api::socket_cursor::{GapReason, ReplayGap, public_gap_reason},
    error::AppError,
    model::{AppendReply, Cursor, EventResponse},
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicAppendReply {
    pub(crate) seq: i64,
    pub(crate) last_seq: i64,
    pub(crate) deduped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) epoch: Option<i64>,
    pub(crate) cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) cursor_epoch: Option<i64>,
    pub(crate) committed_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) committed_cursor_epoch: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicGapPayload {
    #[serde(rename = "type")]
    pub(crate) frame_type: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) from_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) from_cursor_epoch: Option<i64>,
    pub(crate) next_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) next_cursor_epoch: Option<i64>,
    pub(crate) committed_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) committed_cursor_epoch: Option<i64>,
    pub(crate) earliest_available_cursor: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) earliest_available_cursor_epoch: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PublicTailEvent {
    pub(crate) session_id: String,
    pub(crate) seq: i64,
    #[serde(rename = "type")]
    pub(crate) event_type: String,
    pub(crate) payload: crate::model::JsonMap,
    pub(crate) actor: String,
    pub(crate) source: Option<String>,
    pub(crate) metadata: crate::model::JsonMap,
    pub(crate) refs: crate::model::JsonMap,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) producer_id: String,
    pub(crate) producer_seq: i64,
    pub(crate) tenant_id: String,
    pub(crate) inserted_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) epoch: Option<i64>,
    pub(crate) cursor: Cursor,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct RawTailGapPayload {
    #[serde(rename = "type")]
    pub(crate) frame_type: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) from_cursor: Cursor,
    pub(crate) next_cursor: Cursor,
    pub(crate) committed_cursor: Cursor,
    pub(crate) earliest_available_cursor: Cursor,
}

pub(crate) fn append_reply(reply: &AppendReply) -> PublicAppendReply {
    PublicAppendReply {
        seq: reply.seq,
        last_seq: reply.last_seq,
        deduped: reply.deduped,
        epoch: reply.epoch,
        cursor: cursor_seq(reply.cursor),
        cursor_epoch: cursor_epoch(reply.cursor),
        committed_cursor: cursor_seq(reply.committed_cursor),
        committed_cursor_epoch: cursor_epoch(reply.committed_cursor),
    }
}

pub(crate) fn gap(gap: &ReplayGap) -> PublicGapPayload {
    PublicGapPayload {
        frame_type: "gap",
        reason: public_gap_reason(gap.reason),
        from_cursor: cursor_seq(gap.from_cursor),
        from_cursor_epoch: cursor_epoch(gap.from_cursor),
        next_cursor: cursor_seq(gap.next_cursor),
        next_cursor_epoch: cursor_epoch(gap.next_cursor),
        committed_cursor: cursor_seq(gap.committed_cursor),
        committed_cursor_epoch: cursor_epoch(gap.committed_cursor),
        earliest_available_cursor: cursor_seq(gap.earliest_available_cursor),
        earliest_available_cursor_epoch: cursor_epoch(gap.earliest_available_cursor),
    }
}

pub(crate) fn gap_value(replay_gap: &ReplayGap) -> Result<Value, AppError> {
    serde_json::to_value(gap(replay_gap)).map_err(|_| AppError::Internal)
}

pub(crate) fn tail_event(event: &EventResponse) -> PublicTailEvent {
    PublicTailEvent {
        session_id: event.session_id.clone(),
        seq: event.seq,
        event_type: event.event_type.clone(),
        payload: event.payload.clone(),
        actor: event.actor.clone(),
        source: event.source.clone(),
        metadata: event.metadata.clone(),
        refs: event.refs.clone(),
        idempotency_key: event.idempotency_key.clone(),
        producer_id: event.producer_id.clone(),
        producer_seq: event.producer_seq,
        tenant_id: event.tenant_id.clone(),
        inserted_at: event.inserted_at.clone(),
        epoch: event.epoch,
        cursor: event.cursor_token(),
    }
}

pub(crate) fn tail_events_text(
    events: &[EventResponse],
    batch_size: u32,
) -> Result<String, AppError> {
    let rendered = events.iter().map(tail_event).collect::<Vec<_>>();

    if batch_size > 1 || rendered.len() > 1 {
        serde_json::to_string(&rendered).map_err(|_| AppError::Internal)
    } else {
        let event = rendered.into_iter().next().ok_or(AppError::Internal)?;
        serde_json::to_string(&event).map_err(|_| AppError::Internal)
    }
}

pub(crate) fn raw_tail_gap(gap: &ReplayGap) -> RawTailGapPayload {
    RawTailGapPayload {
        frame_type: "gap",
        reason: raw_gap_reason(gap.reason),
        from_cursor: gap.from_cursor,
        next_cursor: gap.next_cursor,
        committed_cursor: gap.committed_cursor,
        earliest_available_cursor: gap.earliest_available_cursor,
    }
}

pub(crate) fn raw_tail_gap_text(gap: &ReplayGap) -> Result<String, AppError> {
    serde_json::to_string(&raw_tail_gap(gap)).map_err(|_| AppError::Internal)
}

fn cursor_seq(cursor: Cursor) -> i64 {
    cursor.seq
}

fn cursor_epoch(cursor: Cursor) -> Option<i64> {
    cursor.epoch
}

fn raw_gap_reason(reason: GapReason) -> &'static str {
    match reason {
        GapReason::CursorExpired => "cursor_expired",
        GapReason::EpochStale => "epoch_stale",
        GapReason::Rollback => "rollback",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        append_reply, gap, gap_value, raw_tail_gap, raw_tail_gap_text, tail_event, tail_events_text,
    };
    use crate::{
        api::socket_cursor::{GapReason, ReplayGap},
        model::{AppendReply, Cursor, EventResponse},
    };

    #[test]
    fn append_reply_preserves_epoch_metadata() {
        let payload = serde_json::to_value(append_reply(&AppendReply {
            seq: 4,
            last_seq: 4,
            deduped: false,
            epoch: Some(9),
            cursor: Cursor::new(Some(9), 4),
            committed_cursor: Cursor::new(Some(9), 2),
        }))
        .expect("serialize append reply");

        assert_eq!(
            payload,
            json!({
                "seq": 4,
                "last_seq": 4,
                "deduped": false,
                "epoch": 9,
                "cursor": 4,
                "cursor_epoch": 9,
                "committed_cursor": 2,
                "committed_cursor_epoch": 9
            })
        );
    }

    #[test]
    fn gap_payload_preserves_epoch_metadata() {
        let payload = serde_json::to_value(gap(&ReplayGap {
            reason: GapReason::EpochStale,
            from_cursor: Cursor::new(Some(3), 8),
            next_cursor: Cursor::new(Some(4), 6),
            committed_cursor: Cursor::new(Some(4), 5),
            earliest_available_cursor: Cursor::new(Some(4), 1),
        }))
        .expect("serialize gap");

        assert_eq!(
            payload,
            json!({
                "type": "gap",
                "reason": "resume_invalidated",
                "from_cursor": 8,
                "from_cursor_epoch": 3,
                "next_cursor": 6,
                "next_cursor_epoch": 4,
                "committed_cursor": 5,
                "committed_cursor_epoch": 4,
                "earliest_available_cursor": 1,
                "earliest_available_cursor_epoch": 4
            })
        );
    }

    #[test]
    fn gap_value_reuses_public_gap_shape() {
        let gap = ReplayGap {
            reason: GapReason::CursorExpired,
            from_cursor: Cursor::new(None, 0),
            next_cursor: Cursor::new(Some(4), 2),
            committed_cursor: Cursor::new(Some(4), 7),
            earliest_available_cursor: Cursor::new(Some(4), 3),
        };

        assert_eq!(
            gap_value(&gap).expect("serialize gap"),
            json!({
                "type": "gap",
                "reason": "cursor_expired",
                "from_cursor": 0,
                "next_cursor": 2,
                "next_cursor_epoch": 4,
                "committed_cursor": 7,
                "committed_cursor_epoch": 4,
                "earliest_available_cursor": 3,
                "earliest_available_cursor_epoch": 4
            })
        );
    }

    fn sample_event() -> EventResponse {
        EventResponse {
            session_id: "ses_demo".to_string(),
            seq: 4,
            event_type: "content".to_string(),
            payload: json!({"text": "hello"})
                .as_object()
                .expect("payload object")
                .clone(),
            actor: "service:test".to_string(),
            source: Some("test".to_string()),
            metadata: serde_json::Map::new(),
            refs: serde_json::Map::new(),
            idempotency_key: Some("idem-1".to_string()),
            producer_id: "writer-1".to_string(),
            producer_seq: 9,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-15T00:00:00Z".to_string(),
            epoch: Some(12),
            cursor: 4,
        }
    }

    #[test]
    fn tail_event_keeps_epoch_aware_cursor_object() {
        let payload = serde_json::to_value(tail_event(&sample_event())).expect("serialize event");

        assert_eq!(
            payload,
            json!({
                "session_id": "ses_demo",
                "seq": 4,
                "type": "content",
                "payload": { "text": "hello" },
                "actor": "service:test",
                "source": "test",
                "metadata": {},
                "refs": {},
                "idempotency_key": "idem-1",
                "producer_id": "writer-1",
                "producer_seq": 9,
                "tenant_id": "acme",
                "inserted_at": "2026-04-15T00:00:00Z",
                "epoch": 12,
                "cursor": { "epoch": 12, "seq": 4 }
            })
        );
    }

    #[test]
    fn tail_events_text_switches_between_object_and_array_shapes() {
        let event = sample_event();

        assert_eq!(
            serde_json::from_str::<Value>(
                &tail_events_text(std::slice::from_ref(&event), 1).expect("single text")
            )
            .expect("single payload"),
            json!({
                "session_id": "ses_demo",
                "seq": 4,
                "type": "content",
                "payload": { "text": "hello" },
                "actor": "service:test",
                "source": "test",
                "metadata": {},
                "refs": {},
                "idempotency_key": "idem-1",
                "producer_id": "writer-1",
                "producer_seq": 9,
                "tenant_id": "acme",
                "inserted_at": "2026-04-15T00:00:00Z",
                "epoch": 12,
                "cursor": { "epoch": 12, "seq": 4 }
            })
        );

        assert_eq!(
            serde_json::from_str::<Value>(&tail_events_text(&[event], 2).expect("batched text"))
                .expect("batched payload"),
            json!([{
                "session_id": "ses_demo",
                "seq": 4,
                "type": "content",
                "payload": { "text": "hello" },
                "actor": "service:test",
                "source": "test",
                "metadata": {},
                "refs": {},
                "idempotency_key": "idem-1",
                "producer_id": "writer-1",
                "producer_seq": 9,
                "tenant_id": "acme",
                "inserted_at": "2026-04-15T00:00:00Z",
                "epoch": 12,
                "cursor": { "epoch": 12, "seq": 4 }
            }])
        );
    }

    #[test]
    fn raw_tail_gap_keeps_internal_reason_and_cursor_objects() {
        let gap = ReplayGap {
            reason: GapReason::Rollback,
            from_cursor: Cursor::new(Some(3), 8),
            next_cursor: Cursor::new(Some(4), 6),
            committed_cursor: Cursor::new(Some(4), 5),
            earliest_available_cursor: Cursor::new(Some(4), 1),
        };

        assert_eq!(
            serde_json::to_value(raw_tail_gap(&gap)).expect("serialize raw gap"),
            json!({
                "type": "gap",
                "reason": "rollback",
                "from_cursor": { "epoch": 3, "seq": 8 },
                "next_cursor": { "epoch": 4, "seq": 6 },
                "committed_cursor": { "epoch": 4, "seq": 5 },
                "earliest_available_cursor": { "epoch": 4, "seq": 1 }
            })
        );

        assert_eq!(
            serde_json::from_str::<Value>(&raw_tail_gap_text(&gap).expect("raw gap text"))
                .expect("raw gap payload"),
            json!({
                "type": "gap",
                "reason": "rollback",
                "from_cursor": { "epoch": 3, "seq": 8 },
                "next_cursor": { "epoch": 4, "seq": 6 },
                "committed_cursor": { "epoch": 4, "seq": 5 },
                "earliest_available_cursor": { "epoch": 4, "seq": 1 }
            })
        );
    }
}
