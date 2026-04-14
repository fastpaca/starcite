use axum::{
    body,
    http::{HeaderMap, header},
};

use crate::{error::AppError, model::AppendEventRequest};

pub(crate) fn validate_session_id(session_id: &str) -> Result<(), AppError> {
    if session_id.is_empty() {
        Err(AppError::InvalidSessionId)
    } else {
        Ok(())
    }
}

pub(crate) async fn read_append_body(
    headers: &HeaderMap,
    body: axum::body::Body,
) -> Result<Vec<u8>, AppError> {
    validate_json_content_type(headers)?;
    let body = body::to_bytes(body, usize::MAX)
        .await
        .map_err(|_| AppError::InvalidEvent)?;
    Ok(body.to_vec())
}

pub(crate) fn parse_append_request(body: &[u8]) -> Result<AppendEventRequest, AppError> {
    serde_json::from_slice(body).map_err(|_| AppError::InvalidEvent)
}

fn validate_json_content_type(headers: &HeaderMap) -> Result<(), AppError> {
    let value = headers
        .get(header::CONTENT_TYPE)
        .ok_or(AppError::InvalidEvent)?;
    let value = value.to_str().map_err(|_| AppError::InvalidEvent)?;
    let mime = value
        .split(';')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(AppError::InvalidEvent)?;
    let mime = mime.to_ascii_lowercase();

    if mime == "application/json" || mime.ends_with("+json") {
        Ok(())
    } else {
        Err(AppError::InvalidEvent)
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, header};

    use super::validate_json_content_type;
    use crate::error::AppError;

    #[test]
    fn json_content_type_accepts_json_with_charset() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "application/json; charset=utf-8"
                .parse()
                .expect("content type"),
        );

        assert!(validate_json_content_type(&headers).is_ok());
    }

    #[test]
    fn json_content_type_rejects_non_json_requests() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            "text/plain".parse().expect("content type"),
        );

        assert!(matches!(
            validate_json_content_type(&headers),
            Err(AppError::InvalidEvent)
        ));
    }
}
