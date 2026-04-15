use serde_json::Value;

use crate::{error::AppError, model::Cursor};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GapReason {
    CursorExpired,
    EpochStale,
    Rollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CursorSnapshot {
    pub(crate) epoch: Option<i64>,
    pub(crate) last_seq: i64,
    pub(crate) committed_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayGap {
    pub(crate) reason: GapReason,
    pub(crate) from_cursor: Cursor,
    pub(crate) next_cursor: Cursor,
    pub(crate) committed_cursor: Cursor,
    pub(crate) earliest_available_cursor: Cursor,
}

pub(crate) fn parse_json_cursor(payload: &Value) -> Result<Cursor, AppError> {
    match payload {
        Value::Number(number) => number
            .as_i64()
            .filter(|seq| *seq >= 0)
            .map(|seq| Cursor::new(None, seq))
            .ok_or(AppError::InvalidCursor),
        Value::Object(object) => {
            let seq = object
                .get("seq")
                .and_then(Value::as_i64)
                .filter(|seq| *seq >= 0)
                .ok_or(AppError::InvalidCursor)?;
            let epoch = parse_optional_json_epoch(object.get("epoch"))?;
            Ok(Cursor::new(epoch, seq))
        }
        _ => Err(AppError::InvalidCursor),
    }
}

pub(crate) fn replay_gap_reason(
    cursor: Cursor,
    snapshot: CursorSnapshot,
    earliest_available_seq: Option<i64>,
) -> Option<GapReason> {
    if cursor.seq > snapshot.last_seq {
        return Some(GapReason::Rollback);
    }

    if cursor_epoch_mismatch(cursor.epoch, snapshot.epoch) {
        return Some(GapReason::EpochStale);
    }

    if earliest_available_seq.is_some_and(|earliest_available_seq| {
        earliest_available_seq > 0 && cursor.seq < earliest_available_seq - 1
    }) {
        return Some(GapReason::CursorExpired);
    }

    None
}

pub(crate) fn build_gap(
    reason: GapReason,
    from_cursor: Cursor,
    snapshot: CursorSnapshot,
    earliest_available_seq: Option<i64>,
) -> ReplayGap {
    let next_cursor = next_cursor_for_gap(reason, from_cursor, snapshot, earliest_available_seq);
    let committed_cursor = Cursor::new(snapshot.epoch, snapshot.committed_seq);
    let earliest_available_cursor = Cursor::new(
        snapshot.epoch,
        earliest_available_seq.unwrap_or(snapshot.last_seq + 1),
    );

    ReplayGap {
        reason,
        from_cursor,
        next_cursor,
        committed_cursor,
        earliest_available_cursor,
    }
}

pub(crate) fn public_gap_reason(reason: GapReason) -> &'static str {
    match reason {
        GapReason::CursorExpired => "cursor_expired",
        GapReason::EpochStale | GapReason::Rollback => "resume_invalidated",
    }
}

fn next_cursor_for_gap(
    reason: GapReason,
    from_cursor: Cursor,
    snapshot: CursorSnapshot,
    earliest_available_seq: Option<i64>,
) -> Cursor {
    let next_seq = match reason {
        GapReason::CursorExpired => earliest_available_seq
            .filter(|earliest_available_seq| *earliest_available_seq > 0)
            .map(|earliest_available_seq| earliest_available_seq - 1)
            .unwrap_or(snapshot.committed_seq),
        GapReason::EpochStale => {
            min_max(from_cursor.seq, snapshot.last_seq, snapshot.committed_seq)
        }
        GapReason::Rollback => snapshot.last_seq.max(snapshot.committed_seq),
    };

    Cursor::new(snapshot.epoch, next_seq)
}

fn min_max(value: i64, upper_bound: i64, floor: i64) -> i64 {
    value.min(upper_bound).max(floor)
}

fn cursor_epoch_mismatch(cursor_epoch: Option<i64>, active_epoch: Option<i64>) -> bool {
    match (cursor_epoch, active_epoch) {
        (None, _) | (_, None) => false,
        (Some(cursor_epoch), Some(active_epoch)) => cursor_epoch != active_epoch,
    }
}

fn parse_optional_json_epoch(value: Option<&Value>) -> Result<Option<i64>, AppError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(number)) => number
            .as_i64()
            .filter(|epoch| *epoch >= 0)
            .map(Some)
            .ok_or(AppError::InvalidCursor),
        _ => Err(AppError::InvalidCursor),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        CursorSnapshot, GapReason, build_gap, parse_json_cursor, public_gap_reason,
        replay_gap_reason,
    };
    use crate::model::Cursor;

    #[test]
    fn json_cursor_supports_integer_and_object_shapes() {
        assert_eq!(
            parse_json_cursor(&json!(12)).expect("integer cursor"),
            Cursor::new(None, 12)
        );
        assert_eq!(
            parse_json_cursor(&json!({"epoch": 4, "seq": 12})).expect("object cursor"),
            Cursor::new(Some(4), 12)
        );
    }

    #[test]
    fn stale_epoch_maps_to_internal_epoch_stale() {
        let reason = replay_gap_reason(
            Cursor::new(Some(9), 4),
            CursorSnapshot {
                epoch: Some(10),
                last_seq: 6,
                committed_seq: 2,
            },
            Some(1),
        );

        assert_eq!(reason, Some(GapReason::EpochStale));
        assert_eq!(
            public_gap_reason(GapReason::EpochStale),
            "resume_invalidated"
        );
    }

    #[test]
    fn rollback_maps_to_resume_invalidated_public_reason() {
        let gap = build_gap(
            GapReason::Rollback,
            Cursor::new(None, 10),
            CursorSnapshot {
                epoch: Some(4),
                last_seq: 3,
                committed_seq: 1,
            },
            Some(1),
        );

        assert_eq!(public_gap_reason(gap.reason), "resume_invalidated");
        assert_eq!(gap.next_cursor, Cursor::new(Some(4), 3));
        assert_eq!(gap.committed_cursor, Cursor::new(Some(4), 1));
    }

    #[test]
    fn cursor_expired_preserves_public_reason() {
        let gap = build_gap(
            GapReason::CursorExpired,
            Cursor::new(None, 0),
            CursorSnapshot {
                epoch: Some(4),
                last_seq: 9,
                committed_seq: 7,
            },
            Some(3),
        );

        assert_eq!(public_gap_reason(gap.reason), "cursor_expired");
        assert_eq!(gap.next_cursor, Cursor::new(Some(4), 2));
        assert_eq!(gap.earliest_available_cursor, Cursor::new(Some(4), 3));
    }
}
