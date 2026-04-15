use serde::Serialize;
use serde_json::Value;

use crate::{
    api::socket_cursor::{ReplayGap, public_gap_reason},
    error::AppError,
    model::{AppendReply, Cursor},
};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicAppendReply {
    pub(crate) seq: i64,
    pub(crate) last_seq: i64,
    pub(crate) deduped: bool,
    pub(crate) cursor: i64,
    pub(crate) committed_cursor: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(crate) struct PublicGapPayload {
    #[serde(rename = "type")]
    pub(crate) frame_type: &'static str,
    pub(crate) reason: &'static str,
    pub(crate) from_cursor: i64,
    pub(crate) next_cursor: i64,
    pub(crate) committed_cursor: i64,
    pub(crate) earliest_available_cursor: i64,
}

pub(crate) fn append_reply(reply: &AppendReply) -> PublicAppendReply {
    PublicAppendReply {
        seq: reply.seq,
        last_seq: reply.last_seq,
        deduped: reply.deduped,
        cursor: cursor_seq(reply.cursor),
        committed_cursor: cursor_seq(reply.committed_cursor),
    }
}

pub(crate) fn gap(gap: &ReplayGap) -> PublicGapPayload {
    PublicGapPayload {
        frame_type: "gap",
        reason: public_gap_reason(gap.reason),
        from_cursor: cursor_seq(gap.from_cursor),
        next_cursor: cursor_seq(gap.next_cursor),
        committed_cursor: cursor_seq(gap.committed_cursor),
        earliest_available_cursor: cursor_seq(gap.earliest_available_cursor),
    }
}

pub(crate) fn gap_value(replay_gap: &ReplayGap) -> Result<Value, AppError> {
    serde_json::to_value(gap(replay_gap)).map_err(|_| AppError::Internal)
}

fn cursor_seq(cursor: Cursor) -> i64 {
    cursor.seq
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{append_reply, gap, gap_value};
    use crate::{
        api::socket_cursor::{GapReason, ReplayGap},
        model::{AppendReply, Cursor},
    };

    #[test]
    fn append_reply_flattens_epoch_aware_cursors() {
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
                "cursor": 4,
                "committed_cursor": 2
            })
        );
        assert!(payload.get("epoch").is_none());
    }

    #[test]
    fn gap_payload_flattens_cursor_tokens() {
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
                "next_cursor": 6,
                "committed_cursor": 5,
                "earliest_available_cursor": 1
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
                "committed_cursor": 7,
                "earliest_available_cursor": 3
            })
        );
    }
}
