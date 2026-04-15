use std::collections::BTreeMap;

use sqlx::PgPool;

use super::{hot_store::HotEventStore, repository};
use crate::{
    error::AppError,
    model::{EventResponse, EventsOptions, EventsPage},
};

#[derive(Debug)]
pub enum ReadEventsError {
    ContinuityUnavailable,
    App(AppError),
}

pub async fn read_events(
    hot_store: &HotEventStore,
    pool: &PgPool,
    session_id: &str,
    opts: EventsOptions,
) -> Result<EventsPage, ReadEventsError> {
    let hot_events = hot_store
        .events_after_cursor(session_id, opts.cursor, opts.limit)
        .await;
    let cold_events =
        maybe_read_cold_events(hot_store, pool, session_id, &opts, &hot_events).await?;
    let events = merge_events(cold_events, hot_events, opts.cursor, opts.limit)?;
    let next_cursor = events.last().map(|event| event.seq);

    Ok(EventsPage {
        events,
        next_cursor,
    })
}

async fn maybe_read_cold_events(
    hot_store: &HotEventStore,
    pool: &PgPool,
    session_id: &str,
    opts: &EventsOptions,
    hot_events: &[EventResponse],
) -> Result<Vec<EventResponse>, ReadEventsError> {
    if hot_events.is_empty() {
        return repository::read_events(pool, session_id, opts.clone())
            .await
            .map(|page| page.events)
            .map_err(map_cold_read_error);
    }

    if hot_events
        .first()
        .is_some_and(|event| event.seq == opts.cursor + 1)
    {
        return Ok(Vec::new());
    }

    let cold_limit = cold_limit(hot_store, session_id, opts, hot_events).await;

    repository::read_events(
        pool,
        session_id,
        EventsOptions {
            cursor: opts.cursor,
            limit: cold_limit,
        },
    )
    .await
    .map(|page| page.events)
    .map_err(map_cold_read_error)
}

async fn cold_limit(
    hot_store: &HotEventStore,
    session_id: &str,
    opts: &EventsOptions,
    hot_events: &[EventResponse],
) -> u32 {
    let Some(max_hot_seq) = hot_store.max_seq(session_id).await else {
        return opts.limit;
    };

    let Some(first_hot_seq) = hot_events.first().map(|event| event.seq) else {
        return opts.limit;
    };

    if max_hot_seq <= opts.cursor || first_hot_seq <= opts.cursor + 1 {
        return opts.limit;
    }

    let missing = first_hot_seq.saturating_sub(opts.cursor + 1);
    let missing = missing.clamp(0, i64::from(opts.limit));
    missing as u32
}

fn merge_events(
    cold_events: Vec<EventResponse>,
    hot_events: Vec<EventResponse>,
    cursor: i64,
    limit: u32,
) -> Result<Vec<EventResponse>, ReadEventsError> {
    let mut merged = BTreeMap::new();

    for event in cold_events.into_iter().chain(hot_events) {
        merged.entry(event.seq).or_insert(event);
    }

    let events = merged
        .into_values()
        .take(limit as usize)
        .collect::<Vec<_>>();

    ensure_gap_free(cursor, &events)?;
    Ok(events)
}

fn ensure_gap_free(cursor: i64, events: &[EventResponse]) -> Result<(), ReadEventsError> {
    let Some(first) = events.first() else {
        return Ok(());
    };

    if first.seq != cursor + 1 {
        return Err(ReadEventsError::ContinuityUnavailable);
    }

    let mut previous = first.seq;
    for event in events.iter().skip(1) {
        if event.seq != previous + 1 {
            return Err(ReadEventsError::ContinuityUnavailable);
        }

        previous = event.seq;
    }

    Ok(())
}

fn map_cold_read_error(error: AppError) -> ReadEventsError {
    match error {
        AppError::Sqlx(_) | AppError::DatabaseUnavailable => ReadEventsError::ContinuityUnavailable,
        error => ReadEventsError::App(error),
    }
}

#[cfg(test)]
mod tests {
    use super::{ReadEventsError, ensure_gap_free, map_cold_read_error, merge_events};
    use crate::{error::AppError, model::EventResponse};
    use serde_json::Map;

    fn event(session_id: &str, seq: i64) -> EventResponse {
        EventResponse {
            session_id: session_id.to_string(),
            seq,
            event_type: "content".to_string(),
            payload: Map::new(),
            actor: "service:bench".to_string(),
            source: Some("test".to_string()),
            metadata: Map::new(),
            refs: Map::new(),
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq: seq,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-13T00:00:00Z".to_string(),
            epoch: None,
            cursor: seq,
        }
    }

    #[test]
    fn merge_prefers_gap_free_union_of_cold_and_hot() {
        let events = merge_events(
            vec![event("ses_demo", 1), event("ses_demo", 2)],
            vec![
                event("ses_demo", 2),
                event("ses_demo", 3),
                event("ses_demo", 4),
            ],
            0,
            10,
        )
        .expect("merge");

        let seqs = events
            .into_iter()
            .map(|event| event.seq)
            .collect::<Vec<_>>();
        assert_eq!(seqs, vec![1, 2, 3, 4]);
    }

    #[test]
    fn gap_free_check_rejects_missing_first_event() {
        let error = ensure_gap_free(4, &[event("ses_demo", 6)]).expect_err("missing first event");
        assert!(matches!(error, ReadEventsError::ContinuityUnavailable));
    }

    #[test]
    fn gap_free_check_rejects_internal_hole() {
        let error = ensure_gap_free(0, &[event("ses_demo", 1), event("ses_demo", 3)])
            .expect_err("internal hole");
        assert!(matches!(error, ReadEventsError::ContinuityUnavailable));
    }

    #[test]
    fn cold_read_sqlx_error_maps_to_continuity_unavailable() {
        let error = map_cold_read_error(AppError::DatabaseUnavailable);
        assert!(matches!(error, ReadEventsError::ContinuityUnavailable));
    }

    #[test]
    fn cold_read_internal_error_is_preserved() {
        let error = map_cold_read_error(AppError::Internal);
        assert!(matches!(error, ReadEventsError::App(AppError::Internal)));
    }
}
