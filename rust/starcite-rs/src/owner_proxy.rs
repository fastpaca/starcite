use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode, header::HeaderName},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};

use crate::{error::AppError, model::AppendEventRequest};

const MAX_IDLE_CONNECTIONS_PER_PEER: usize = 8;

#[derive(Debug, Clone)]
pub struct OwnerProxy {
    timeout: Duration,
    public_url: Option<Arc<str>>,
    idle_connections: Arc<Mutex<HashMap<String, Vec<TcpStream>>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BaseUrlScheme {
    Http,
    Https,
}

#[derive(Debug, Clone)]
struct HttpPeer {
    authority: String,
    connect_addr: String,
}

#[derive(Debug)]
enum ProxyError {
    Connect,
    Write,
    Read,
    Timeout,
    BadResponse,
}

#[derive(Debug)]
struct HttpResponse {
    parsed: ParsedResponse,
    reusable: bool,
}

#[derive(Debug)]
struct ParsedResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Debug)]
struct ParsedHttpHead {
    status: u16,
    headers: Vec<(String, String)>,
    content_length: usize,
    connection_close: bool,
}

impl OwnerProxy {
    pub fn new(timeout: Duration, public_url: Option<String>) -> Self {
        Self {
            timeout,
            public_url: public_url.map(Arc::from),
            idle_connections: Arc::new(Mutex::new(HashMap::new())),
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
        self.send(owner_url, "GET", &path, authorization, None)
            .await
    }

    pub async fn forward_append(
        &self,
        owner_url: &str,
        session_id: &str,
        request: &AppendEventRequest,
        authorization: Option<&HeaderValue>,
    ) -> Result<Response<Body>, AppError> {
        let body = serde_json::to_vec(request).map_err(|_| AppError::OwnerProxyUnavailable {
            owner_url: owner_url.to_string(),
        })?;
        self.forward_append_raw(owner_url, session_id, &body, authorization)
            .await
    }

    pub async fn forward_append_raw(
        &self,
        owner_url: &str,
        session_id: &str,
        body: &[u8],
        authorization: Option<&HeaderValue>,
    ) -> Result<Response<Body>, AppError> {
        let path = format!(
            "/v1/sessions/{}/append",
            percent_encode_component(session_id)
        );
        self.send(owner_url, "POST", &path, authorization, Some(body))
            .await
    }

    async fn send(
        &self,
        owner_url: &str,
        method: &str,
        path: &str,
        authorization: Option<&HeaderValue>,
        body: Option<&[u8]>,
    ) -> Result<Response<Body>, AppError> {
        let mut current_owner_url = owner_url.to_string();
        let mut retried_with_fresh_connection = false;
        let mut followed_owner_redirect = false;
        let authorization = authorization
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: current_owner_url.clone(),
            })?;

        loop {
            let Some(proxy_target) = self.proxy_target(&current_owner_url) else {
                return Err(AppError::OwnerProxyUnavailable {
                    owner_url: current_owner_url,
                });
            };
            let peer =
                parse_http_peer(proxy_target).map_err(|_| AppError::OwnerProxyUnavailable {
                    owner_url: current_owner_url.clone(),
                })?;
            let request = build_request(
                method,
                path,
                &peer.authority,
                authorization.as_deref(),
                body,
            );
            let pooled = self.take_connection(&peer).await;
            let mut stream = match pooled {
                Some(stream) => stream,
                None => self
                    .connect(&peer)
                    .await
                    .map_err(|_| AppError::OwnerProxyUnavailable {
                        owner_url: current_owner_url.clone(),
                    })?,
            };

            match self.send_request(&mut stream, &request).await {
                Ok(response) => {
                    if let Some(redirect_owner_url) = redirect_owner_url(
                        response.parsed.status,
                        &response.parsed.headers,
                        proxy_target,
                    ) && !followed_owner_redirect
                        && self.proxy_target(&redirect_owner_url).is_some()
                    {
                        if response.reusable {
                            self.return_connection(&peer, stream).await;
                        }

                        current_owner_url = redirect_owner_url;
                        retried_with_fresh_connection = false;
                        followed_owner_redirect = true;
                        continue;
                    }

                    if response.reusable {
                        self.return_connection(&peer, stream).await;
                    }

                    return Ok(response.parsed.into_response());
                }
                Err(error)
                    if !retried_with_fresh_connection && is_retryable_proxy_error(&error) =>
                {
                    retried_with_fresh_connection = true;
                }
                Err(_error) => {
                    return Err(AppError::OwnerProxyUnavailable {
                        owner_url: current_owner_url,
                    });
                }
            }
        }
    }

    fn proxy_target<'a>(&'a self, owner_url: &'a str) -> Option<&'a str> {
        match self.public_url() {
            Some(public_url) if public_url == owner_url => None,
            _ => Some(owner_url),
        }
    }

    async fn connect(&self, peer: &HttpPeer) -> Result<TcpStream, ProxyError> {
        timeout(self.timeout, TcpStream::connect(&peer.connect_addr))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::Connect)
    }

    async fn take_connection(&self, peer: &HttpPeer) -> Option<TcpStream> {
        let mut idle_connections = self.idle_connections.lock().await;
        let connections = idle_connections.get_mut(&peer.authority)?;
        let stream = connections.pop();

        if connections.is_empty() {
            idle_connections.remove(&peer.authority);
        }

        stream
    }

    async fn return_connection(&self, peer: &HttpPeer, stream: TcpStream) {
        let mut idle_connections = self.idle_connections.lock().await;
        let connections = idle_connections
            .entry(peer.authority.clone())
            .or_insert_with(Vec::new);

        if connections.len() < MAX_IDLE_CONNECTIONS_PER_PEER {
            connections.push(stream);
        }
    }

    async fn send_request(
        &self,
        stream: &mut TcpStream,
        request: &[u8],
    ) -> Result<HttpResponse, ProxyError> {
        timeout(self.timeout, stream.write_all(request))
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::Write)?;

        timeout(self.timeout, stream.flush())
            .await
            .map_err(|_| ProxyError::Timeout)?
            .map_err(|_| ProxyError::Write)?;

        self.read_response(stream).await
    }

    async fn read_response(&self, stream: &mut TcpStream) -> Result<HttpResponse, ProxyError> {
        let mut response = Vec::with_capacity(512);
        let mut read_buf = [0_u8; 4096];

        let (head_end, parsed_head) = loop {
            if let Some(head_end) = find_http_head_end(&response) {
                let parsed_head = parse_http_head(&response[..head_end])?;
                break (head_end, parsed_head);
            }

            let read = timeout(self.timeout, stream.read(&mut read_buf))
                .await
                .map_err(|_| ProxyError::Timeout)?
                .map_err(|_| ProxyError::Read)?;

            if read == 0 {
                return Err(ProxyError::BadResponse);
            }

            response.extend_from_slice(&read_buf[..read]);
        };

        let body_start = head_end + 4;
        let body_end = body_start + parsed_head.content_length;

        while response.len() < body_end {
            let read = timeout(self.timeout, stream.read(&mut read_buf))
                .await
                .map_err(|_| ProxyError::Timeout)?
                .map_err(|_| ProxyError::Read)?;

            if read == 0 {
                return Err(ProxyError::BadResponse);
            }

            response.extend_from_slice(&read_buf[..read]);
        }

        Ok(HttpResponse {
            parsed: ParsedResponse {
                status: parsed_head.status,
                headers: parsed_head.headers,
                body: response[body_start..body_end].to_vec(),
            },
            reusable: !parsed_head.connection_close,
        })
    }
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

pub fn build_tail_ws_url(
    owner_url: &str,
    session_id: &str,
    params: &HashMap<String, String>,
) -> Option<String> {
    let base = websocket_base_url(owner_url)?;
    let mut path = format!(
        "{base}/v1/sessions/{}/tail",
        percent_encode_component(session_id)
    );
    let query = encode_query(params);

    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }

    Some(path)
}

pub fn build_phoenix_socket_ws_url(
    owner_url: &str,
    params: &HashMap<String, String>,
) -> Option<String> {
    let base = websocket_base_url(owner_url)?;
    let mut path = format!("{base}/v1/socket/websocket");
    let query = encode_query(params);

    if !query.is_empty() {
        path.push('?');
        path.push_str(&query);
    }

    Some(path)
}

fn build_request(
    method: &str,
    path: &str,
    authority: &str,
    authorization: Option<&str>,
    body: Option<&[u8]>,
) -> Vec<u8> {
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\n").into_bytes();

    if let Some(authorization) = authorization {
        request.extend_from_slice(b"Authorization: ");
        request.extend_from_slice(authorization.as_bytes());
        request.extend_from_slice(b"\r\n");
    }

    match body {
        Some(body) => {
            request.extend_from_slice(b"Content-Type: application/json\r\n");
            request.extend_from_slice(b"Content-Length: ");
            request.extend_from_slice(body.len().to_string().as_bytes());
            request.extend_from_slice(b"\r\n");
            request.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
            request.extend_from_slice(body);
        }
        None => {
            request.extend_from_slice(b"Connection: keep-alive\r\n\r\n");
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
    let (_, authority) = parse_base_url(raw, false)?;
    let (host, port) = split_host_port(&authority, raw)?;
    let connect_addr = format!("{host}:{port}");

    Ok(HttpPeer {
        authority,
        connect_addr,
    })
}

fn websocket_base_url(raw: &str) -> Option<String> {
    let (scheme, authority) = parse_base_url(raw, true).ok()?;
    let ws_scheme = match scheme {
        BaseUrlScheme::Http => "ws",
        BaseUrlScheme::Https => "wss",
    };

    Some(format!("{ws_scheme}://{authority}"))
}

fn parse_base_url(raw: &str, allow_https: bool) -> Result<(BaseUrlScheme, String), String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let (scheme, authority) = if let Some(rest) = trimmed.strip_prefix("http://") {
        (BaseUrlScheme::Http, rest)
    } else if allow_https {
        match trimmed.strip_prefix("https://") {
            Some(rest) => (BaseUrlScheme::Https, rest),
            None => {
                return Err(format!(
                    "owner URL must start with http:// or https://: {raw}"
                ));
            }
        }
    } else {
        return Err(format!("owner URL must start with http://: {raw}"));
    };

    if authority.is_empty() || authority.contains('/') {
        return Err(format!(
            "owner URL must be a bare host:port base URL: {raw}"
        ));
    }

    split_host_port(authority, raw)?;

    Ok((scheme, authority.to_string()))
}

fn split_host_port<'a>(authority: &'a str, raw: &str) -> Result<(&'a str, u16), String> {
    let Some((host, port_raw)) = authority.rsplit_once(':') else {
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

    Ok((host, port))
}

fn is_retryable_proxy_error(error: &ProxyError) -> bool {
    matches!(
        error,
        ProxyError::Connect
            | ProxyError::Write
            | ProxyError::Read
            | ProxyError::Timeout
            | ProxyError::BadResponse
    )
}

fn redirect_owner_url(
    status: u16,
    headers: &[(String, String)],
    current_owner_url: &str,
) -> Option<String> {
    if status != StatusCode::CONFLICT.as_u16() {
        return None;
    }

    headers
        .iter()
        .find(|(name, _value)| name.eq_ignore_ascii_case("x-starcite-owner-url"))
        .map(|(_name, value)| value.trim())
        .filter(|value| !value.is_empty())
        .filter(|value| *value != current_owner_url)
        .map(str::to_owned)
}

fn find_http_head_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_http_head(bytes: &[u8]) -> Result<ParsedHttpHead, ProxyError> {
    let head = std::str::from_utf8(bytes).map_err(|_| ProxyError::BadResponse)?;
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(ProxyError::BadResponse)?;
    let mut headers = Vec::new();
    let mut content_length = 0_usize;
    let mut connection_close = false;

    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };

        let value = value.trim();
        headers.push((name.trim().to_string(), value.to_string()));

        if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse::<usize>()
                .map_err(|_| ProxyError::BadResponse)?;
        } else if name.eq_ignore_ascii_case("connection") && value.eq_ignore_ascii_case("close") {
            connection_close = true;
        }
    }

    Ok(ParsedHttpHead {
        status,
        headers,
        content_length,
        connection_close,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        OwnerProxy, build_phoenix_socket_ws_url, build_tail_ws_url, find_http_head_end,
        parse_http_head, parse_http_peer,
    };
    use crate::{error::OWNER_URL_HEADER, model::AppendEventRequest};
    use axum::{
        body,
        http::{HeaderValue, StatusCode, header},
    };
    use serde_json::json;
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    #[test]
    fn parses_owner_base_url() {
        let peer = parse_http_peer("http://127.0.0.1:4191").expect("peer");
        assert_eq!(peer.authority, "127.0.0.1:4191");
        assert_eq!(peer.connect_addr, "127.0.0.1:4191");
    }

    #[test]
    fn parses_http_head_with_keepalive_defaults() {
        let parsed = parse_http_head(
            b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 9\r\n\r\n",
        )
        .expect("parsed head");

        assert_eq!(parsed.status, 201);
        assert_eq!(parsed.content_length, 9);
        assert!(!parsed.connection_close);
    }

    #[test]
    fn finds_http_head_end() {
        let response = b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n{}";
        let expected = std::str::from_utf8(response)
            .expect("utf8")
            .find("\r\n\r\n")
            .expect("head end");

        assert_eq!(find_http_head_end(response), Some(expected));
    }

    #[test]
    fn builds_tail_websocket_url_from_owner_public_url() {
        let params = HashMap::from([
            ("batch_size".to_string(), "32".to_string()),
            ("cursor".to_string(), "8".to_string()),
        ]);

        let url = build_tail_ws_url("https://owner.example:4443/", "ses demo", &params)
            .expect("websocket URL");

        assert_eq!(
            url,
            "wss://owner.example:4443/v1/sessions/ses%20demo/tail?batch_size=32&cursor=8"
        );
    }

    #[test]
    fn builds_phoenix_socket_url_from_owner_public_url() {
        let params = HashMap::from([
            ("token".to_string(), "jwt-token".to_string()),
            ("vsn".to_string(), "2.0.0".to_string()),
        ]);
        let url =
            build_phoenix_socket_ws_url("http://127.0.0.1:4191", &params).expect("socket URL");

        assert_eq!(
            url,
            "ws://127.0.0.1:4191/v1/socket/websocket?token=jwt-token&vsn=2.0.0"
        );
    }

    #[tokio::test]
    async fn forwards_append_and_preserves_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let request = read_request(&mut socket)
                .await
                .expect("request")
                .expect("request body");
            assert!(request.contains("POST /v1/sessions/ses_demo/append HTTP/1.1"));
            assert!(request.contains("Authorization: Bearer test-token"));
            assert!(request.contains("\"producer_seq\":1"));

            let response_body = "{\"seq\":1}";
            let response = format!(
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-starcite-owner-url: http://127.0.0.1:4191\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );

            socket
                .write_all(response.as_bytes())
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
            let request = read_request(&mut socket)
                .await
                .expect("request")
                .expect("request body");
            assert!(
                request.contains("GET /v1/sessions/ses_demo/events?cursor=0&limit=10 HTTP/1.1")
            );

            let response_body = "{\"events\":[],\"next_cursor\":null}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );

            socket
                .write_all(response.as_bytes())
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

    #[tokio::test]
    async fn reuses_keepalive_connection_for_second_forward_append() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("listener addr");
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let accepted_connections_task = accepted_connections.clone();

        tokio::spawn(async move {
            let mut handled_requests = 0_usize;

            loop {
                let accepted = timeout(Duration::from_secs(2), listener.accept()).await;
                let Ok(Ok((mut stream, _peer_addr))) = accepted else {
                    break;
                };
                accepted_connections_task.fetch_add(1, Ordering::Relaxed);

                while handled_requests < 2 {
                    let Ok(Ok(Some(request))) =
                        timeout(Duration::from_secs(2), read_request(&mut stream)).await
                    else {
                        break;
                    };

                    handled_requests += 1;
                    assert!(request.contains("\"producer_seq\":1"));

                    let response_body = "{\"seq\":1}";
                    let response = format!(
                        "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );

                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("write response");

                    if handled_requests == 2 {
                        return;
                    }
                }
            }
        });

        let proxy = OwnerProxy::new(
            Duration::from_millis(500),
            Some("http://127.0.0.1:4193".into()),
        );
        let request = AppendEventRequest {
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
        };
        let owner_url = format!("http://{}", addr);

        proxy
            .forward_append(
                &owner_url,
                "ses_demo",
                &request,
                Some(&HeaderValue::from_static("Bearer test-token")),
            )
            .await
            .expect("first append");
        proxy
            .forward_append(
                &owner_url,
                "ses_demo",
                &request,
                Some(&HeaderValue::from_static("Bearer test-token")),
            )
            .await
            .expect("second append");

        assert_eq!(accepted_connections.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn follows_session_not_owned_redirect_once() {
        let first_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("first listener");
        let first_addr = first_listener.local_addr().expect("first addr");
        let second_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("second listener");
        let second_addr = second_listener.local_addr().expect("second addr");
        let redirected_owner_url = format!("http://{second_addr}");

        tokio::spawn(async move {
            let (mut socket, _) = first_listener.accept().await.expect("first accept");
            let request = read_request(&mut socket)
                .await
                .expect("request")
                .expect("request body");
            assert!(request.contains("POST /v1/sessions/ses_demo/append HTTP/1.1"));

            let response_body = "{\"error\":\"session_not_owned\"}";
            let response = format!(
                "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\nx-starcite-owner-url: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                redirected_owner_url,
                response_body
            );

            socket
                .write_all(response.as_bytes())
                .await
                .expect("write redirect response");
        });

        tokio::spawn(async move {
            let (mut socket, _) = second_listener.accept().await.expect("second accept");
            let request = read_request(&mut socket)
                .await
                .expect("request")
                .expect("request body");
            assert!(request.contains("POST /v1/sessions/ses_demo/append HTTP/1.1"));

            let response_body = "{\"seq\":2}";
            let response = format!(
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );

            socket
                .write_all(response.as_bytes())
                .await
                .expect("write success response");
        });

        let proxy = OwnerProxy::new(Duration::from_secs(1), None);
        let response = proxy
            .forward_append(
                &format!("http://{first_addr}"),
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
                None,
            )
            .await
            .expect("forward append after redirect");

        assert_eq!(response.status(), StatusCode::CREATED);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        assert_eq!(body.as_ref(), b"{\"seq\":2}");
    }

    async fn read_request(stream: &mut TcpStream) -> Result<Option<String>, std::io::Error> {
        let mut request = Vec::with_capacity(512);
        let mut read_buf = [0_u8; 4096];

        let head_end = loop {
            if let Some(head_end) = find_http_head_end(&request) {
                break head_end;
            }

            let read = stream.read(&mut read_buf).await?;
            if read == 0 {
                return Ok(None);
            }

            request.extend_from_slice(&read_buf[..read]);
        };

        let head = std::str::from_utf8(&request[..head_end])
            .expect("request head")
            .to_string();
        let content_length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let body_start = head_end + 4;
        let body_end = body_start + content_length;

        while request.len() < body_end {
            let read = stream.read(&mut read_buf).await?;
            if read == 0 {
                return Ok(None);
            }

            request.extend_from_slice(&read_buf[..read]);
        }

        Ok(Some(String::from_utf8(request).expect("request utf8")))
    }
}
