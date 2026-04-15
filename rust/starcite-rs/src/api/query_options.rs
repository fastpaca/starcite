use std::collections::HashMap;

use crate::{
    config::{DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT},
    error::AppError,
    model::{ArchivedFilter, Cursor, ListOptions, parse_query_scalar},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TailOptions {
    pub(crate) cursor: Cursor,
    pub(crate) batch_size: u32,
}

pub(crate) fn parse_tail_options(
    params: &HashMap<String, String>,
) -> Result<TailOptions, AppError> {
    let mut cursor = Cursor::zero();
    let mut batch_size = 1_u32;
    let mut saw_cursor = false;

    if let Some(raw_cursor) = params.get("cursor") {
        cursor = parse_tail_cursor(raw_cursor)?;
        saw_cursor = true;
    }

    if let Some(raw_epoch) = params.get("cursor_epoch") {
        if !saw_cursor {
            return Err(AppError::InvalidCursor);
        }

        let epoch = parse_tail_cursor_part(raw_epoch, AppError::InvalidCursor)?;
        match cursor.epoch {
            Some(cursor_epoch) if cursor_epoch != epoch => return Err(AppError::InvalidCursor),
            _ => cursor.epoch = Some(epoch),
        }
    }

    if let Some(raw_batch_size) = params.get("batch_size") {
        batch_size = parse_tail_batch_size(raw_batch_size)?;
    }

    Ok(TailOptions { cursor, batch_size })
}

pub(crate) fn parse_list_options(params: HashMap<String, String>) -> Result<ListOptions, AppError> {
    let mut metadata = serde_json::Map::new();
    let mut tenant_id = None;
    let mut limit = DEFAULT_LIST_LIMIT;
    let mut cursor = None;
    let mut archived = ArchivedFilter::Active;

    for (key, value) in params {
        match key.as_str() {
            "limit" => limit = parse_limit(&value)?,
            "cursor" => {
                if value.is_empty() {
                    return Err(AppError::InvalidCursor);
                }

                cursor = Some(value);
            }
            "archived" => archived = parse_archived_filter(&value)?,
            "tenant_id" => {
                if value.is_empty() {
                    return Err(AppError::InvalidListQuery);
                }

                tenant_id = Some(value);
            }
            _ => {
                if let Some(key) = metadata_key(&key) {
                    metadata.insert(key.to_string(), parse_query_scalar(&value));
                }
            }
        }
    }

    Ok(ListOptions {
        limit,
        cursor,
        archived,
        metadata,
        tenant_id,
        session_id: None,
    })
}

pub(crate) fn parse_archived_filter(raw: &str) -> Result<ArchivedFilter, AppError> {
    match raw {
        "false" => Ok(ArchivedFilter::Active),
        "true" => Ok(ArchivedFilter::Archived),
        "all" => Ok(ArchivedFilter::All),
        _ => Err(AppError::InvalidListQuery),
    }
}

fn parse_limit(raw: &str) -> Result<u32, AppError> {
    let parsed = raw.parse::<u32>().map_err(|_| AppError::InvalidLimit)?;

    if (1..=MAX_LIST_LIMIT).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(AppError::InvalidLimit)
    }
}

fn parse_tail_batch_size(raw: &str) -> Result<u32, AppError> {
    let parsed = raw
        .parse::<u32>()
        .map_err(|_| AppError::InvalidTailBatchSize)?;

    if (1..=MAX_LIST_LIMIT).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(AppError::InvalidTailBatchSize)
    }
}

fn parse_tail_cursor(raw: &str) -> Result<Cursor, AppError> {
    if raw.is_empty() {
        return Err(AppError::InvalidCursor);
    }

    let Some((epoch, seq)) = raw.split_once(':') else {
        let seq = parse_tail_cursor_part(raw, AppError::InvalidCursor)?;
        return Ok(Cursor::new(None, seq));
    };

    Ok(Cursor::new(
        Some(parse_tail_cursor_part(epoch, AppError::InvalidCursor)?),
        parse_tail_cursor_part(seq, AppError::InvalidCursor)?,
    ))
}

fn parse_tail_cursor_part(raw: &str, error: AppError) -> Result<i64, AppError> {
    raw.parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or(error)
}

fn metadata_key(raw: &str) -> Option<&str> {
    raw.strip_prefix("metadata.")
        .or_else(|| raw.strip_prefix("metadata[")?.strip_suffix(']'))
        .filter(|key| !key.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{parse_archived_filter, parse_list_options, parse_tail_options};
    use crate::model::{ArchivedFilter, Cursor};

    #[test]
    fn list_query_supports_bracket_metadata_filters() {
        let params = HashMap::from([
            ("metadata[marker]".to_string(), "hot".to_string()),
            ("limit".to_string(), "50".to_string()),
        ]);

        let opts = parse_list_options(params).expect("query should parse");

        assert_eq!(opts.limit, 50);
        assert_eq!(opts.metadata.get("marker"), Some(&json!("hot")));
    }

    #[test]
    fn archived_filter_matches_existing_api() {
        assert_eq!(
            parse_archived_filter("false").expect("false should parse"),
            ArchivedFilter::Active
        );
        assert_eq!(
            parse_archived_filter("true").expect("true should parse"),
            ArchivedFilter::Archived
        );
        assert_eq!(
            parse_archived_filter("all").expect("all should parse"),
            ArchivedFilter::All
        );
    }

    #[test]
    fn tail_query_defaults_cursor_and_batch_size() {
        let options = parse_tail_options(&HashMap::new()).expect("tail query should parse");

        assert_eq!(options.cursor, Cursor::zero());
        assert_eq!(options.batch_size, 1);
    }

    #[test]
    fn tail_query_accepts_epoch_cursor_string() {
        let options =
            parse_tail_options(&HashMap::from([("cursor".to_string(), "9:4".to_string())]))
                .expect("epoch cursor should parse");

        assert_eq!(options.cursor, Cursor::new(Some(9), 4));
    }

    #[test]
    fn tail_query_accepts_cursor_epoch_pair() {
        let options = parse_tail_options(&HashMap::from([
            ("cursor".to_string(), "4".to_string()),
            ("cursor_epoch".to_string(), "9".to_string()),
            ("batch_size".to_string(), "8".to_string()),
        ]))
        .expect("cursor pair should parse");

        assert_eq!(options.cursor, Cursor::new(Some(9), 4));
        assert_eq!(options.batch_size, 8);
    }

    #[test]
    fn tail_query_rejects_conflicting_epoch_cursor_shapes() {
        let error = parse_tail_options(&HashMap::from([
            ("cursor".to_string(), "9:4".to_string()),
            ("cursor_epoch".to_string(), "8".to_string()),
        ]))
        .expect_err("conflicting epoch cursor should fail");

        assert!(matches!(error, crate::error::AppError::InvalidCursor));
    }

    #[test]
    fn tail_query_rejects_invalid_batch_size() {
        let error = parse_tail_options(&HashMap::from([(
            "batch_size".to_string(),
            "0".to_string(),
        )]))
        .expect_err("batch size should fail");

        assert!(matches!(
            error,
            crate::error::AppError::InvalidTailBatchSize
        ));
    }
}
