use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode, header::HeaderName},
};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{error::AppError, model::AppendEventRequest};

#[derive(Debug, Clone)]
pub struct OwnerProxy {
    timeout: Duration,
    public_url: Option<Arc<str>>,
}

#[derive(Debug, Clone)]
struct HttpPeer {
    authority: String,
    connect_addr: String,
}

#[derive(Debug)]
enum ProxyError {
    BadResponse,
}

impl OwnerProxy {
    pub fn new(timeout: Duration, public_url: Option<String>) -> Self {
        Self {
            timeout,
            public_url: public_url.map(Arc::from),
        }
    }

    pub fn timeout_ms(&self) -> u64 {
        self.timeout.as_millis().min(u64::MAX as u128) as u64
    }

    pub fn public_url(&self) -> Option<&str> {
        self.public_url.as_deref()
    }

    pub async fn forward_read_events(
        &self,
        owner_url: &str,
        session_id: &str,
        params: &HashMap<String, String>,
        authorization: Option<&HeaderValue>,
    ) -> Result<Response<Body>, AppError> {
        let path = build_events_path(session_id, params);
        self.send(
            owner_url,
            "GET",
            &path,
            authorization,
            None::<&AppendEventRequest>,
        )
        .await
    }

    pub async fn forward_append(
        &self,
        owner_url: &str,
        session_id: &str,
        request: &AppendEventRequest,
        authorization: Option<&HeaderValue>,
    ) -> Result<Response<Body>, AppError> {
        let path = format!(
            "/v1/sessions/{}/append",
            percent_encode_component(session_id)
        );
        self.send(owner_url, "POST", &path, authorization, Some(request))
            .await
    }

    async fn send<T: Serialize>(
        &self,
        owner_url: &str,
        method: &str,
        path: &str,
        authorization: Option<&HeaderValue>,
        body: Option<&T>,
    ) -> Result<Response<Body>, AppError> {
        let Some(owner_url) = self.proxy_target(owner_url) else {
            return Err(AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            });
        };
        let peer = parse_http_peer(owner_url).map_err(|_| AppError::OwnerProxyUnavailable {
            owner_url: owner_url.to_string(),
        })?;
        let body_json = body.map(serde_json::to_string).transpose().map_err(|_| {
            AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            }
        })?;
        let authorization = authorization
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?;

        let request = build_request(
            method,
            path,
            &peer.authority,
            authorization.as_deref(),
            body_json.as_deref(),
        );

        let mut stream = timeout(self.timeout, TcpStream::connect(&peer.connect_addr))
            .await
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?;

        timeout(self.timeout, stream.write_all(request.as_bytes()))
            .await
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?;

        let mut response = Vec::new();
        timeout(self.timeout, stream.read_to_end(&mut response))
            .await
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?;

        let parsed =
            parse_http_response(&response).map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: owner_url.to_string(),
            })?;

        Ok(parsed.into_response())
    }

    fn proxy_target<'a>(&'a self, owner_url: &'a str) -> Option<&'a str> {
        match self.public_url() {
            Some(public_url) if public_url == owner_url => None,
            _ => Some(owner_url),
        }
    }
}

#[derive(Debug)]
struct ParsedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl ParsedResponse {
    fn into_response(self) -> Response<Body> {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::BAD_GATEWAY);
        let mut response = Response::builder()
            .status(status)
            .body(Body::from(self.body))
            .expect("owner proxy response");

        for (name, value) in self.headers {
            let lower = name.to_ascii_lowercase();
            if lower == "connection"
                || lower == "transfer-encoding"
                || lower == "vary"
                || lower.starts_with("access-control-")
            {
                continue;
            }

            if let (Ok(name), Ok(value)) = (
                HeaderName::from_bytes(lower.as_bytes()),
                HeaderValue::from_str(&value),
            ) {
                response.headers_mut().insert(name, value);
            }
        }

        response
    }
}

fn build_events_path(session_id: &str, params: &HashMap<String, String>) -> String {
    let mut path = format!(
        "/v1/sessions/{}/events",
        percent_encode_component(session_id)
    );
    let query = encode_query(params);
    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }

    path
}

fn build_request(
    method: &str,
    path: &str,
    authority: &str,
    authorization: Option<&str>,
    body: Option<&str>,
) -> String {
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\n");

    if let Some(authorization) = authorization {
        request.push_str("Authorization: ");
        request.push_str(authorization);
        request.push_str("\r\n");
    }

    match body {
        Some(body) => {
            request.push_str("Content-Type: application/json\r\n");
            request.push_str("Content-Length: ");
            request.push_str(&body.len().to_string());
            request.push_str("\r\n");
            request.push_str("Connection: close\r\n\r\n");
            request.push_str(body);
        }
        None => {
            request.push_str("Connection: close\r\n\r\n");
        }
    }

    request
}

fn encode_query(params: &HashMap<String, String>) -> String {
    let mut entries = params.iter().collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(right.0));

    entries
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                percent_encode_component(key),
                percent_encode_component(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn percent_encode_component(value: &str) -> String {
    let mut out = String::new();

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push('%');
            out.push(hex(byte >> 4));
            out.push(hex(byte & 0x0f));
        }
    }

    out
}

fn hex(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'A' + (value - 10)),
        _ => unreachable!("nibble out of range"),
    }
}

fn parse_http_peer(raw: &str) -> Result<HttpPeer, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let Some(rest) = trimmed.strip_prefix("http://") else {
        return Err(format!("owner URL must start with http://: {raw}"));
    };

    if rest.is_empty() || rest.contains('/') {
        return Err(format!(
            "owner URL must be a bare http host:port base URL: {raw}"
        ));
    }

    let Some((host, port_raw)) = rest.rsplit_once(':') else {
        return Err(format!("owner URL must include a port: {raw}"));
    };

    if host.is_empty() {
        return Err(format!("owner URL host is empty: {raw}"));
    }

    let port = port_raw
        .parse::<u16>()
        .map_err(|_| format!("owner URL has invalid port: {raw}"))?;

    if port == 0 {
        return Err(format!("owner URL has invalid port: {raw}"));
    }

    Ok(HttpPeer {
        authority: rest.to_string(),
        connect_addr: format!("{host}:{port}"),
    })
}

fn parse_http_response(bytes: &[u8]) -> Result<ParsedResponse, ProxyError> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or(ProxyError::BadResponse)?;
    let header_bytes = &bytes[..header_end];
    let body = bytes[header_end + 4..].to_vec();
    let head = std::str::from_utf8(header_bytes).map_err(|_| ProxyError::BadResponse)?;
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(ProxyError::BadResponse)?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_string(), value.trim().to_string()))
        .collect::<Vec<_>>();

    Ok(ParsedResponse {
        status,
        headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::{OwnerProxy, parse_http_peer};
    use crate::{error::OWNER_URL_HEADER, model::AppendEventRequest};
    use axum::{
        body,
        http::{HeaderValue, StatusCode, header},
    };
    use serde_json::json;
    use std::{collections::HashMap, time::Duration};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn parses_owner_base_url() {
        let peer = parse_http_peer("http://127.0.0.1:4191").expect("peer");
        assert_eq!(peer.authority, "127.0.0.1:4191");
        assert_eq!(peer.connect_addr, "127.0.0.1:4191");
    }

    #[tokio::test]
    async fn forwards_append_and_preserves_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket).await;
            assert!(request.contains("POST /v1/sessions/ses_demo/append HTTP/1.1"));
            assert!(request.contains("Authorization: Bearer test-token"));
            assert!(request.contains("\"producer_seq\":1"));

            socket
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nx-starcite-owner-url: http://127.0.0.1:4191\r\n\r\n{\"seq\":1}",
                )
                .await
                .expect("write response");
        });

        let proxy = OwnerProxy::new(Duration::from_secs(1), Some("http://127.0.0.1:4193".into()));
        let response = proxy
            .forward_append(
                &format!("http://{}", addr),
                "ses_demo",
                &AppendEventRequest {
                    event_type: "content".to_string(),
                    payload: json!({"text": "hello"}),
                    actor: Some("user:user-1".to_string()),
                    source: None,
                    metadata: None,
                    refs: None,
                    idempotency_key: None,
                    producer_id: "p1".to_string(),
                    producer_seq: 1,
                    expected_seq: Some(0),
                },
                Some(&HeaderValue::from_static("Bearer test-token")),
            )
            .await
            .expect("forward append");

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get(&OWNER_URL_HEADER)
                .expect("owner header"),
            "http://127.0.0.1:4191"
        );
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .expect("content type"),
            "application/json"
        );

        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(body.as_ref(), b"{\"seq\":1}");
    }

    #[tokio::test]
    async fn forwards_event_reads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket).await;
            assert!(
                request.contains("GET /v1/sessions/ses_demo/events?cursor=0&limit=10 HTTP/1.1")
            );

            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"events\":[],\"next_cursor\":null}",
                )
                .await
                .expect("write response");
        });

        let proxy = OwnerProxy::new(Duration::from_secs(1), None);
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "10".to_string());
        params.insert("cursor".to_string(), "0".to_string());

        let response = proxy
            .forward_read_events(&format!("http://{}", addr), "ses_demo", &params, None)
            .await
            .expect("forward read");

        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];

        loop {
            let read = socket.read(&mut buffer).await.expect("read request");
            if read == 0 {
                break;
            }

            bytes.extend_from_slice(&buffer[..read]);

            if let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let header_bytes = &bytes[..header_end];
                let header_text = std::str::from_utf8(header_bytes).expect("header text");
                let content_length = header_text
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("Content-Length: ")
                            .and_then(|value| value.parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let body_len = bytes.len().saturating_sub(header_end + 4);

                if body_len >= content_length {
                    return String::from_utf8(bytes).expect("request bytes");
                }
            }
        }

        String::from_utf8(bytes).expect("request bytes")
    }
}
