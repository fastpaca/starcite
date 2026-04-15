use super::query_options::TailOptions;

use serde_json::{Value, json};

use crate::{
    api::{
        public_payload,
        socket_cursor::{ReplayGap, parse_json_cursor},
    },
    config::MAX_LIST_LIMIT,
    error::AppError,
    model::{Cursor, LifecycleResponse},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleOptions {
    pub(crate) cursor: Cursor,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PhoenixFrame {
    pub(crate) join_ref: Option<String>,
    pub(crate) ref_id: Option<String>,
    pub(crate) topic: String,
    pub(crate) event: String,
    pub(crate) payload: Value,
}

pub(crate) fn parse_lifecycle_join_payload(payload: &Value) -> Result<LifecycleOptions, AppError> {
    let cursor = parse_lifecycle_cursor(payload)?;
    Ok(LifecycleOptions { cursor })
}

pub(crate) fn parse_tail_join_payload(payload: &Value) -> Result<TailOptions, AppError> {
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;
    let cursor = parse_phoenix_cursor(object)?;
    let mut batch_size = 1_u32;

    if let Some(value) = object.get("batch_size") {
        let value = value.as_u64().ok_or(AppError::InvalidTailBatchSize)?;
        batch_size = u32::try_from(value).map_err(|_| AppError::InvalidTailBatchSize)?;

        if !(1..=MAX_LIST_LIMIT).contains(&batch_size) {
            return Err(AppError::InvalidTailBatchSize);
        }
    }

    Ok(TailOptions { cursor, batch_size })
}

pub(crate) fn parse_client_frame(raw: &str) -> Result<PhoenixFrame, serde_json::Error> {
    let (join_ref, ref_id, topic, event, payload): (
        Option<String>,
        Option<String>,
        String,
        String,
        Value,
    ) = serde_json::from_str(raw)?;

    Ok(PhoenixFrame {
        join_ref,
        ref_id,
        topic,
        event,
        payload,
    })
}

pub(crate) fn reply_frame(
    join_ref: Option<String>,
    ref_id: Option<String>,
    topic: String,
    ok: bool,
    response: Value,
) -> PhoenixFrame {
    PhoenixFrame {
        join_ref,
        ref_id,
        topic,
        event: "phx_reply".to_string(),
        payload: json!({
            "status": if ok { "ok" } else { "error" },
            "response": response
        }),
    }
}

pub(crate) fn push_frame(
    join_ref: Option<String>,
    topic: String,
    event: &str,
    payload: Value,
) -> PhoenixFrame {
    PhoenixFrame {
        join_ref,
        ref_id: None,
        topic,
        event: event.to_string(),
        payload,
    }
}

pub(crate) fn lifecycle_payload(event: &LifecycleResponse) -> Result<Value, AppError> {
    serde_json::to_value(event).map_err(|_| AppError::Internal)
}

pub(crate) fn build_gap_payload(gap: &ReplayGap) -> Value {
    public_payload::gap_value(gap).expect("public gap payload should serialize")
}

fn parse_lifecycle_cursor(payload: &Value) -> Result<Cursor, AppError> {
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;
    parse_phoenix_cursor(object)
}

fn parse_phoenix_cursor(object: &serde_json::Map<String, Value>) -> Result<Cursor, AppError> {
    let mut cursor = match object.get("cursor") {
        Some(value) => parse_json_cursor(value)?,
        None => Cursor::zero(),
    };

    if let Some(epoch) = parse_cursor_epoch(object.get("cursor_epoch"))? {
        if !object.contains_key("cursor") {
            return Err(AppError::InvalidCursor);
        }

        match cursor.epoch {
            Some(cursor_epoch) if cursor_epoch != epoch => return Err(AppError::InvalidCursor),
            _ => cursor.epoch = Some(epoch),
        }
    }

    Ok(cursor)
}

fn parse_cursor_epoch(value: Option<&Value>) -> Result<Option<i64>, AppError> {
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
        PhoenixFrame, parse_client_frame, parse_lifecycle_join_payload, parse_tail_join_payload,
        push_frame, reply_frame,
    };
    use crate::model::Cursor;

    #[test]
    fn parses_client_frame_from_protocol_array() {
        let frame = parse_client_frame(r#"["1","2","tail:ses_demo","phx_join",{"cursor":4}]"#)
            .expect("frame should parse");

        assert_eq!(
            frame,
            PhoenixFrame {
                join_ref: Some("1".to_string()),
                ref_id: Some("2".to_string()),
                topic: "tail:ses_demo".to_string(),
                event: "phx_join".to_string(),
                payload: json!({"cursor": 4}),
            }
        );
    }

    #[test]
    fn tail_join_payload_defaults_cursor_and_batch_size() {
        let options = parse_tail_join_payload(&json!({})).expect("defaults should parse");
        assert_eq!(options.cursor, Cursor::zero());
        assert_eq!(options.batch_size, 1);
    }

    #[test]
    fn tail_join_payload_accepts_cursor_epoch_pair() {
        let options = parse_tail_join_payload(&json!({"cursor": 4, "cursor_epoch": 9}))
            .expect("cursor epoch pair should parse");

        assert_eq!(options.cursor, Cursor::new(Some(9), 4));
    }

    #[test]
    fn tail_join_payload_accepts_object_cursor_shape() {
        let options = parse_tail_join_payload(&json!({"cursor": {"epoch": 4, "seq": 4}}))
            .expect("object cursor should parse");

        assert_eq!(options.cursor, Cursor::new(Some(4), 4));
    }

    #[test]
    fn tail_join_payload_rejects_invalid_cursor_shapes() {
        assert!(parse_tail_join_payload(&json!({"cursor": "4"})).is_err());
        assert!(parse_tail_join_payload(&json!({"cursor_epoch": 4})).is_err());
        assert!(parse_tail_join_payload(&json!({"cursor": 4, "cursor_epoch": "9"})).is_err());
        assert!(
            parse_tail_join_payload(&json!({
                "cursor": {"epoch": 4, "seq": 4},
                "cursor_epoch": 9
            }))
            .is_err()
        );
    }

    #[test]
    fn lifecycle_join_payload_defaults_cursor() {
        let options = parse_lifecycle_join_payload(&json!({})).expect("defaults should parse");
        assert_eq!(options.cursor, Cursor::zero());
    }

    #[test]
    fn lifecycle_join_payload_accepts_cursor_epoch_pair() {
        let options = parse_lifecycle_join_payload(&json!({"cursor": 4, "cursor_epoch": 9}))
            .expect("cursor epoch pair should parse");

        assert_eq!(options.cursor, Cursor::new(Some(9), 4));
    }

    #[test]
    fn lifecycle_join_payload_rejects_invalid_cursor_shapes() {
        assert!(parse_lifecycle_join_payload(&json!({"cursor": "4"})).is_err());
        assert!(parse_lifecycle_join_payload(&json!({"cursor_epoch": 4})).is_err());
    }

    #[test]
    fn reply_and_push_frames_keep_protocol_shape() {
        let reply = reply_frame(
            Some("1".to_string()),
            Some("2".to_string()),
            "tail:ses_demo".to_string(),
            true,
            json!({}),
        );

        let push = push_frame(
            Some("1".to_string()),
            "tail:ses_demo".to_string(),
            "events",
            json!({"events": []}),
        );

        let reply_value = serde_json::to_value(&(
            reply.join_ref,
            reply.ref_id,
            reply.topic,
            reply.event,
            reply.payload,
        ))
        .expect("reply should serialize");

        let push_value = serde_json::to_value(&(
            push.join_ref,
            push.ref_id,
            push.topic,
            push.event,
            push.payload,
        ))
        .expect("push should serialize");

        assert_eq!(
            reply_value,
            json!(["1", "2", "tail:ses_demo", "phx_reply", {"status": "ok", "response": {}}])
        );
        assert_eq!(
            push_value,
            json!(["1", null, "tail:ses_demo", "events", {"events": []}])
        );
    }
}
