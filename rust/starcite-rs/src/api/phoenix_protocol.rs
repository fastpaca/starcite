use super::query_options::TailOptions;

use serde_json::{Value, json};

use crate::{
    api::socket_cursor::{ReplayGap, parse_json_cursor, public_gap_reason},
    config::MAX_LIST_LIMIT,
    error::AppError,
    model::{Cursor, LifecycleResponse},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleOptions {
    pub(crate) cursor: Cursor,
    pub(crate) session_id: Option<String>,
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
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;
    let mut session_id = None;
    let cursor = parse_lifecycle_cursor(payload)?;

    if let Some(value) = object.get("session_id") {
        let value = value.as_str().ok_or(AppError::InvalidSessionId)?;

        if value.is_empty() {
            return Err(AppError::InvalidSessionId);
        }

        session_id = Some(value.to_string());
    }

    Ok(LifecycleOptions { cursor, session_id })
}

pub(crate) fn parse_session_lifecycle_join_payload(payload: &Value) -> Result<Cursor, AppError> {
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;

    if object.contains_key("session_id") {
        return Err(AppError::InvalidSessionId);
    }

    parse_lifecycle_cursor(payload)
}

pub(crate) fn parse_tail_join_payload(payload: &Value) -> Result<TailOptions, AppError> {
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;
    let mut cursor = Cursor::zero();
    let mut batch_size = 1_u32;

    if let Some(value) = object.get("cursor") {
        cursor = parse_json_cursor(value)?;
    }

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
    json!({
        "type": "gap",
        "reason": public_gap_reason(gap.reason),
        "from_cursor": gap.from_cursor.seq,
        "next_cursor": gap.next_cursor.seq,
        "committed_cursor": gap.committed_cursor.seq,
        "earliest_available_cursor": gap.earliest_available_cursor.seq
    })
}

fn parse_lifecycle_cursor(payload: &Value) -> Result<Cursor, AppError> {
    let object = payload.as_object().ok_or(AppError::InvalidEvent)?;
    let mut cursor = Cursor::zero();

    if let Some(value) = object.get("cursor") {
        cursor = parse_json_cursor(value)?;
    }

    Ok(cursor)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        PhoenixFrame, parse_client_frame, parse_lifecycle_join_payload,
        parse_session_lifecycle_join_payload, parse_tail_join_payload, push_frame, reply_frame,
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
    fn tail_join_payload_rejects_non_integer_cursor() {
        assert!(parse_tail_join_payload(&json!({"cursor": "4"})).is_err());
    }

    #[test]
    fn lifecycle_join_payload_defaults_cursor() {
        let options = parse_lifecycle_join_payload(&json!({})).expect("defaults should parse");
        assert_eq!(options.cursor, Cursor::zero());
        assert_eq!(options.session_id, None);
    }

    #[test]
    fn lifecycle_join_payload_rejects_non_integer_cursor() {
        assert!(parse_lifecycle_join_payload(&json!({"cursor": "4"})).is_err());
    }

    #[test]
    fn lifecycle_join_payload_parses_session_filter() {
        let options = parse_lifecycle_join_payload(&json!({"session_id": "ses_demo"}))
            .expect("session filter should parse");

        assert_eq!(options.session_id.as_deref(), Some("ses_demo"));
    }

    #[test]
    fn session_lifecycle_join_payload_parses_cursor() {
        let cursor = parse_session_lifecycle_join_payload(&json!({"cursor": 7}))
            .expect("cursor should parse");

        assert_eq!(cursor, Cursor::new(None, 7));
    }

    #[test]
    fn session_lifecycle_join_payload_rejects_session_filter() {
        assert!(parse_session_lifecycle_join_payload(&json!({"session_id": "ses_demo"})).is_err());
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
