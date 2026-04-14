use std::collections::HashMap;

use crate::{
    config::{DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT},
    error::AppError,
    model::{ArchivedFilter, EventsOptions, ListOptions, parse_query_scalar},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TailOptions {
    pub(crate) cursor: i64,
    pub(crate) batch_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LifecycleOptions {
    pub(crate) tenant_id: String,
    pub(crate) cursor: i64,
    pub(crate) session_id: Option<String>,
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

pub(crate) fn parse_events_options(
    params: HashMap<String, String>,
) -> Result<EventsOptions, AppError> {
    let mut cursor = 0_i64;
    let mut limit = DEFAULT_LIST_LIMIT;

    for (key, value) in params {
        match key.as_str() {
            "cursor" => {
                cursor = value.parse::<i64>().map_err(|_| AppError::InvalidCursor)?;

                if cursor < 0 {
                    return Err(AppError::InvalidCursor);
                }
            }
            "limit" => limit = parse_limit(&value)?,
            _ => {}
        }
    }

    Ok(EventsOptions { cursor, limit })
}

pub(crate) fn parse_tail_options(params: HashMap<String, String>) -> Result<TailOptions, AppError> {
    let mut cursor = 0_i64;
    let mut batch_size = 1_u32;

    for (key, value) in params {
        match key.as_str() {
            "cursor" => {
                cursor = value.parse::<i64>().map_err(|_| AppError::InvalidCursor)?;

                if cursor < 0 {
                    return Err(AppError::InvalidCursor);
                }
            }
            "batch_size" => {
                batch_size = value
                    .parse::<u32>()
                    .map_err(|_| AppError::InvalidTailBatchSize)?;

                if !(1..=MAX_LIST_LIMIT).contains(&batch_size) {
                    return Err(AppError::InvalidTailBatchSize);
                }
            }
            _ => {}
        }
    }

    Ok(TailOptions { cursor, batch_size })
}

pub(crate) fn parse_lifecycle_options(
    params: HashMap<String, String>,
) -> Result<LifecycleOptions, AppError> {
    let cursor = parse_events_options(params.clone())?.cursor;
    let session_id = parse_optional_session_id(&params)?;

    match params
        .get("tenant_id")
        .filter(|tenant_id| !tenant_id.is_empty())
    {
        Some(tenant_id) => Ok(LifecycleOptions {
            tenant_id: tenant_id.clone(),
            cursor,
            session_id,
        }),
        None => Err(AppError::InvalidTenantId),
    }
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

fn metadata_key(raw: &str) -> Option<&str> {
    raw.strip_prefix("metadata.")
        .or_else(|| raw.strip_prefix("metadata[")?.strip_suffix(']'))
        .filter(|key| !key.is_empty())
}

pub(crate) fn parse_optional_session_id(
    params: &HashMap<String, String>,
) -> Result<Option<String>, AppError> {
    match params.get("session_id") {
        None => Ok(None),
        Some(value) if value.is_empty() => Err(AppError::InvalidSessionId),
        Some(value) => Ok(Some(value.clone())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{
        LifecycleOptions, TailOptions, parse_archived_filter, parse_events_options,
        parse_lifecycle_options, parse_list_options, parse_tail_options,
    };
    use crate::model::ArchivedFilter;

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
    fn events_query_defaults_cursor_to_zero() {
        let opts = parse_events_options(HashMap::new()).expect("query should parse");
        assert_eq!(opts.cursor, 0);
    }

    #[test]
    fn tail_cursor_uses_same_validation_as_events_query() {
        let params = HashMap::from([("cursor".to_string(), "12".to_string())]);

        let options = parse_tail_options(params).expect("tail query should parse");
        assert_eq!(
            options,
            TailOptions {
                cursor: 12,
                batch_size: 1,
            }
        );
    }

    #[test]
    fn tail_query_supports_batch_size() {
        let params = HashMap::from([
            ("cursor".to_string(), "7".to_string()),
            ("batch_size".to_string(), "64".to_string()),
        ]);

        let options = parse_tail_options(params).expect("tail query should parse");
        assert_eq!(
            options,
            TailOptions {
                cursor: 7,
                batch_size: 64,
            }
        );
    }

    #[test]
    fn tail_query_rejects_invalid_batch_size() {
        let params = HashMap::from([("batch_size".to_string(), "0".to_string())]);
        assert!(parse_tail_options(params).is_err());
    }

    #[test]
    fn lifecycle_query_requires_tenant_id() {
        assert!(parse_lifecycle_options(HashMap::new()).is_err());
    }

    #[test]
    fn lifecycle_query_parses_tenant_id() {
        let params = HashMap::from([("tenant_id".to_string(), "acme".to_string())]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(
            options,
            LifecycleOptions {
                tenant_id: "acme".to_string(),
                cursor: 0,
                session_id: None,
            }
        );
    }

    #[test]
    fn lifecycle_query_parses_cursor() {
        let params = HashMap::from([
            ("tenant_id".to_string(), "acme".to_string()),
            ("cursor".to_string(), "9".to_string()),
        ]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(options.cursor, 9);
    }

    #[test]
    fn lifecycle_query_parses_session_filter() {
        let params = HashMap::from([
            ("tenant_id".to_string(), "acme".to_string()),
            ("session_id".to_string(), "ses_demo".to_string()),
        ]);

        let options = parse_lifecycle_options(params).expect("lifecycle query should parse");

        assert_eq!(options.session_id.as_deref(), Some("ses_demo"));
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
}
