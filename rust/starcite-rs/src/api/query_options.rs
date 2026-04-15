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

fn metadata_key(raw: &str) -> Option<&str> {
    raw.strip_prefix("metadata.")
        .or_else(|| raw.strip_prefix("metadata[")?.strip_suffix(']'))
        .filter(|key| !key.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{parse_archived_filter, parse_list_options};
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
