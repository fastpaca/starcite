use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
    time::timeout,
};

use crate::{error::AppError, model::EventResponse};

const APPLY_PATH: &str = "/internal/replication/apply";
const SNAPSHOT_PATH: &str = "/internal/replication/snapshot";
const MAX_IDLE_CONNECTIONS_PER_PEER: usize = 8;

#[derive(Debug, Clone)]
pub struct ReplicationCoordinator {
    require_peer_replication: bool,
    instance_id: Arc<str>,
    timeout: Duration,
    idle_connections: Arc<Mutex<HashMap<String, Vec<TcpStream>>>>,
}

#[derive(Debug, Clone)]
struct ControlPeer {
    base_url: String,
    authority: String,
    connect_addr: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicationSnapshot {
    pub enabled: bool,
    pub peer_replication_required: bool,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReplicationPeer {
    pub node_id: String,
    pub ops_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSessionState {
    pub session_id: String,
    pub tenant_id: String,
    pub last_seq: i64,
    pub archived_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ApplyReplicaRequest {
    pub owner_id: String,
    pub epoch: i64,
    pub state: ReplicaSessionState,
    pub events: Vec<EventResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationAck {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaSnapshotRequest {
    pub owner_id: String,
    pub epoch: i64,
    pub session_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplicaSnapshotResponse {
    pub state: ReplicaSessionState,
    pub events: Vec<EventResponse>,
}

#[derive(Debug)]
enum ReplicationClientError {
    Connect(std::io::Error),
    Write(std::io::Error),
    Read(std::io::Error),
    Timeout,
    Encode(serde_json::Error),
    BadResponse,
    HttpStatus { status: u16, body: String },
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
    reusable: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct ParsedHttpHead {
    status: u16,
    content_length: usize,
    connection_close: bool,
}

impl ReplicationClientError {
    fn message(&self) -> String {
        match self {
            Self::Connect(error) => format!("connect failed: {error}"),
            Self::Write(error) => format!("write failed: {error}"),
            Self::Read(error) => format!("read failed: {error}"),
            Self::Timeout => "request timed out".to_string(),
            Self::Encode(error) => format!("encode failed: {error}"),
            Self::BadResponse => "peer returned an invalid HTTP response".to_string(),
            Self::HttpStatus { status, body } => {
                format!("peer returned HTTP {status}: {}", body.trim())
            }
        }
    }
}

impl ReplicationCoordinator {
    pub fn new(
        instance_id: Arc<str>,
        require_peer_replication: bool,
        timeout: Duration,
    ) -> Result<Self, String> {
        Ok(Self {
            require_peer_replication,
            instance_id,
            timeout,
            idle_connections: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn replicate_session_state(
        &self,
        epoch: i64,
        state: ReplicaSessionState,
        events: &[EventResponse],
        assigned_replicas: &[ReplicationPeer],
    ) -> Result<(), AppError> {
        let peers = self.control_peers(assigned_replicas)?;
        if peers.is_empty() {
            return Ok(());
        }

        let total_members = peers.len() + 1;
        let required_remote_acks = required_remote_acks(total_members);
        let request = ApplyReplicaRequest {
            owner_id: self.instance_id.to_string(),
            epoch,
            state,
            events: events.to_vec(),
        };
        let mut acknowledgements = 0_usize;

        for peer in &peers {
            match self.post_json(peer, APPLY_PATH, &request).await {
                Ok(()) => {
                    acknowledgements += 1;
                    if acknowledgements >= required_remote_acks {
                        return Ok(());
                    }
                }
                Err(error) => {
                    let session_id = &request.state.session_id;
                    let last_seq = request.state.last_seq;
                    tracing::warn!(
                        error = error.message(),
                        peer = peer.base_url,
                        session_id,
                        last_seq,
                        required_remote_acks,
                        acknowledgements,
                        "replica session-state apply failed"
                    );
                }
            }
        }

        Err(AppError::QuorumUnavailable {
            required: quorum_size(total_members),
            acknowledged: (acknowledgements + 1) as u32,
        })
    }

    pub async fn fetch_session_snapshot(
        &self,
        epoch: i64,
        session_id: &str,
        replica: &ReplicationPeer,
    ) -> Result<ReplicaSnapshotResponse, AppError> {
        let peer =
            parse_control_peer(&replica.ops_url).map_err(|_| AppError::OwnerProxyUnavailable {
                owner_url: replica.ops_url.clone(),
            })?;
        let body = self
            .post_json_with_response(
                &peer,
                SNAPSHOT_PATH,
                &ReplicaSnapshotRequest {
                    owner_id: self.instance_id.to_string(),
                    epoch,
                    session_id: session_id.to_string(),
                },
            )
            .await
            .map_err(|_error| AppError::OwnerProxyUnavailable {
                owner_url: replica.ops_url.clone(),
            })?;

        serde_json::from_str(&body).map_err(|_error| AppError::OwnerProxyUnavailable {
            owner_url: replica.ops_url.clone(),
        })
    }

    pub fn snapshot(&self) -> ReplicationSnapshot {
        ReplicationSnapshot {
            enabled: self.require_peer_replication,
            peer_replication_required: self.require_peer_replication,
            timeout_ms: self.timeout.as_millis().min(u64::MAX as u128) as u64,
        }
    }

    fn control_peers(
        &self,
        assigned_replicas: &[ReplicationPeer],
    ) -> Result<Vec<ControlPeer>, AppError> {
        if assigned_replicas.is_empty() {
            if self.require_peer_replication {
                return Err(AppError::QuorumUnavailable {
                    required: 2,
                    acknowledged: 1,
                });
            }

            return Ok(Vec::new());
        }

        assigned_replicas
            .iter()
            .map(|replica| {
                parse_control_peer(&replica.ops_url).map_err(|_| AppError::QuorumUnavailable {
                    required: 2,
                    acknowledged: 1,
                })
            })
            .collect()
    }

    async fn post_json<T: Serialize>(
        &self,
        peer: &ControlPeer,
        path: &str,
        body: &T,
    ) -> Result<(), ReplicationClientError> {
        self.post_json_with_response(peer, path, body)
            .await
            .map(|_body| ())
    }

    async fn post_json_with_response<T: Serialize>(
        &self,
        peer: &ControlPeer,
        path: &str,
        body: &T,
    ) -> Result<String, ReplicationClientError> {
        let payload = serde_json::to_string(body).map_err(ReplicationClientError::Encode)?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {content_length}\r\nConnection: keep-alive\r\n\r\n{payload}",
            host = peer.authority,
            content_length = payload.len()
        );

        let mut retried_with_fresh_connection = false;

        loop {
            let pooled = self.take_connection(peer).await;
            let mut stream = match pooled {
                Some(stream) => stream,
                None => self.connect(peer).await?,
            };

            match self.send_request(&mut stream, &request).await {
                Ok(response) => {
                    if response.reusable {
                        self.return_connection(peer, stream).await;
                    }

                    if (200..300).contains(&response.status) {
                        return Ok(response.body);
                    }

                    return Err(ReplicationClientError::HttpStatus {
                        status: response.status,
                        body: response.body,
                    });
                }
                Err(error)
                    if !retried_with_fresh_connection && is_retryable_transport_error(&error) =>
                {
                    retried_with_fresh_connection = true;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn connect(&self, peer: &ControlPeer) -> Result<TcpStream, ReplicationClientError> {
        timeout(self.timeout, TcpStream::connect(&peer.connect_addr))
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Connect)
    }

    async fn take_connection(&self, peer: &ControlPeer) -> Option<TcpStream> {
        let mut idle_connections = self.idle_connections.lock().await;
        let connections = idle_connections.get_mut(&peer.base_url)?;
        let stream = connections.pop();

        if connections.is_empty() {
            idle_connections.remove(&peer.base_url);
        }

        stream
    }

    async fn return_connection(&self, peer: &ControlPeer, stream: TcpStream) {
        let mut idle_connections = self.idle_connections.lock().await;
        let connections = idle_connections
            .entry(peer.base_url.clone())
            .or_insert_with(Vec::new);

        if connections.len() < MAX_IDLE_CONNECTIONS_PER_PEER {
            connections.push(stream);
        }
    }

    async fn send_request(
        &self,
        stream: &mut TcpStream,
        request: &str,
    ) -> Result<HttpResponse, ReplicationClientError> {
        timeout(self.timeout, stream.write_all(request.as_bytes()))
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Write)?;

        timeout(self.timeout, stream.flush())
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Write)?;

        self.read_response(stream).await
    }

    async fn read_response(
        &self,
        stream: &mut TcpStream,
    ) -> Result<HttpResponse, ReplicationClientError> {
        let mut response = Vec::with_capacity(512);
        let mut read_buf = [0_u8; 4096];

        let (head_end, parsed_head) = loop {
            if let Some(head_end) = find_http_head_end(&response) {
                let parsed_head = parse_http_head(&response[..head_end])?;
                break (head_end, parsed_head);
            }

            let read = timeout(self.timeout, stream.read(&mut read_buf))
                .await
                .map_err(|_| ReplicationClientError::Timeout)?
                .map_err(ReplicationClientError::Read)?;

            if read == 0 {
                return Err(ReplicationClientError::BadResponse);
            }

            response.extend_from_slice(&read_buf[..read]);
        };

        let body_start = head_end + 4;
        let body_end = body_start + parsed_head.content_length;

        while response.len() < body_end {
            let read = timeout(self.timeout, stream.read(&mut read_buf))
                .await
                .map_err(|_| ReplicationClientError::Timeout)?
                .map_err(ReplicationClientError::Read)?;

            if read == 0 {
                return Err(ReplicationClientError::BadResponse);
            }

            response.extend_from_slice(&read_buf[..read]);
        }

        let body = String::from_utf8(response[body_start..body_end].to_vec())
            .map_err(|_| ReplicationClientError::BadResponse)?;

        Ok(HttpResponse {
            status: parsed_head.status,
            body,
            reusable: !parsed_head.connection_close,
        })
    }
}

fn parse_control_peer(raw: &str) -> Result<ControlPeer, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let Some(rest) = trimmed.strip_prefix("http://") else {
        return Err(format!("control peer URL must start with http://: {raw}"));
    };

    if rest.is_empty() || rest.contains('/') {
        return Err(format!(
            "control peer URL must be a bare http host:port base URL: {raw}"
        ));
    }

    let Some((host, port_raw)) = rest.rsplit_once(':') else {
        return Err(format!("control peer URL must include a port: {raw}"));
    };

    if host.is_empty() {
        return Err(format!("control peer URL host is empty: {raw}"));
    }

    let port = port_raw
        .parse::<u16>()
        .map_err(|_| format!("control peer URL has invalid port: {raw}"))?;

    if port == 0 {
        return Err(format!("control peer URL has invalid port: {raw}"));
    }

    Ok(ControlPeer {
        base_url: trimmed.to_string(),
        authority: rest.to_string(),
        connect_addr: format!("{host}:{port}"),
    })
}

fn is_retryable_transport_error(error: &ReplicationClientError) -> bool {
    matches!(
        error,
        ReplicationClientError::Connect(_)
            | ReplicationClientError::Write(_)
            | ReplicationClientError::Read(_)
            | ReplicationClientError::Timeout
            | ReplicationClientError::BadResponse
    )
}

fn quorum_size(replica_count: usize) -> u32 {
    (replica_count / 2 + 1) as u32
}

fn required_remote_acks(total_members: usize) -> usize {
    quorum_size(total_members).saturating_sub(1) as usize
}

fn find_http_head_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_http_head(bytes: &[u8]) -> Result<ParsedHttpHead, ReplicationClientError> {
    let head = std::str::from_utf8(bytes).map_err(|_| ReplicationClientError::BadResponse)?;
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(ReplicationClientError::BadResponse)?;
    let mut content_length = 0_usize;
    let mut connection_close = false;

    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };

        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse::<usize>()
                .map_err(|_| ReplicationClientError::BadResponse)?;
        } else if name.eq_ignore_ascii_case("connection") && value.eq_ignore_ascii_case("close") {
            connection_close = true;
        }
    }

    Ok(ParsedHttpHead {
        status,
        content_length,
        connection_close,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ApplyReplicaRequest, ReplicaSessionState, ReplicationCoordinator, ReplicationPeer,
        find_http_head_end, parse_control_peer, parse_http_head,
    };
    use crate::model::EventResponse;
    use serde_json::Map;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::oneshot,
        time::timeout,
    };

    fn event() -> EventResponse {
        EventResponse {
            session_id: "ses_demo".to_string(),
            seq: 1,
            event_type: "content".to_string(),
            payload: Map::new(),
            actor: "service:bench".to_string(),
            source: Some("test".to_string()),
            metadata: Map::new(),
            refs: Map::new(),
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq: 1,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-13T00:00:00Z".to_string(),
            epoch: None,
            cursor: 1,
        }
    }

    fn replica_state(last_seq: i64, archived_seq: i64) -> ReplicaSessionState {
        ReplicaSessionState {
            session_id: "ses_demo".to_string(),
            tenant_id: "acme".to_string(),
            last_seq,
            archived_seq,
        }
    }

    #[test]
    fn parses_control_peer_base_url() {
        let peer = parse_control_peer("http://127.0.0.1:4194").expect("peer");

        assert_eq!(peer.base_url, "http://127.0.0.1:4194");
        assert_eq!(peer.authority, "127.0.0.1:4194");
        assert_eq!(peer.connect_addr, "127.0.0.1:4194");
    }

    #[test]
    fn rejects_control_peer_without_port() {
        let error = parse_control_peer("http://127.0.0.1").expect_err("missing port");
        assert!(error.contains("must include a port"));
    }

    #[test]
    fn replica_requests_round_trip_through_json() {
        let append = ApplyReplicaRequest {
            owner_id: "node-a".to_string(),
            epoch: 2,
            state: replica_state(1, 0),
            events: vec![event()],
        };
        let append_json = serde_json::to_string(&append).expect("append json");

        assert_eq!(
            serde_json::from_str::<ApplyReplicaRequest>(&append_json).expect("append decode"),
            append
        );
    }

    #[test]
    fn parses_http_head_with_keepalive_defaults() {
        let parsed = parse_http_head(
            b"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 22\r\n\r\n",
        )
        .expect("parsed head");

        assert_eq!(parsed.status, 202);
        assert_eq!(parsed.content_length, 22);
        assert!(!parsed.connection_close);
    }

    #[test]
    fn finds_http_head_end() {
        let response = b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n{}";
        let expected = std::str::from_utf8(response)
            .expect("utf8")
            .find("\r\n\r\n")
            .expect("head end");

        assert_eq!(find_http_head_end(response), Some(expected));
    }

    #[tokio::test]
    async fn required_replication_without_assigned_replica_fails_closed() {
        let coordinator = ReplicationCoordinator::new(
            Arc::<str>::from("node-a"),
            true,
            Duration::from_millis(500),
        )
        .expect("replication coordinator");

        let error = coordinator
            .replicate_session_state(7, replica_state(1, 0), &[event()], &[])
            .await
            .expect_err("missing replica should fail");

        assert!(matches!(
            error,
            crate::error::AppError::QuorumUnavailable {
                required: 2,
                acknowledged: 1,
            }
        ));
    }

    #[tokio::test]
    async fn optional_replication_without_assigned_replica_stays_local() {
        let coordinator = ReplicationCoordinator::new(
            Arc::<str>::from("node-a"),
            false,
            Duration::from_millis(500),
        )
        .expect("replication coordinator");

        coordinator
            .replicate_session_state(7, replica_state(1, 0), &[event()], &[])
            .await
            .expect("local-only replication should succeed");
    }

    #[tokio::test]
    async fn invalid_assigned_replica_url_fails_closed() {
        let coordinator = ReplicationCoordinator::new(
            Arc::<str>::from("node-a"),
            true,
            Duration::from_millis(500),
        )
        .expect("replication coordinator");
        let assigned_replica = ReplicationPeer {
            node_id: "node-b".to_string(),
            ops_url: "node-b:4002".to_string(),
        };

        let error = coordinator
            .replicate_session_state(7, replica_state(1, 0), &[event()], &[assigned_replica])
            .await
            .expect_err("invalid assigned replica should fail");

        assert!(matches!(
            error,
            crate::error::AppError::QuorumUnavailable {
                required: 2,
                acknowledged: 1,
            }
        ));
    }

    #[tokio::test]
    async fn reuses_keepalive_connection_for_second_append() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let addr = listener.local_addr().expect("listener addr");
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let (handled_tx, handled_rx) = oneshot::channel();
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
                    let Ok(Ok(Some(body))) =
                        timeout(Duration::from_secs(2), read_http_request(&mut stream)).await
                    else {
                        break;
                    };

                    handled_requests += 1;
                    assert!(body.contains("\"owner_id\":\"node-a\""));

                    let response_body = "{\"status\":\"committed\"}";
                    let response = format!(
                        "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );

                    stream
                        .write_all(response.as_bytes())
                        .await
                        .expect("write response");

                    if handled_requests == 2 {
                        let _ = handled_tx.send(());
                        return;
                    }
                }
            }
        });

        let coordinator = ReplicationCoordinator::new(
            Arc::<str>::from("node-a"),
            true,
            Duration::from_millis(500),
        )
        .expect("replication coordinator");
        let event = event();
        let assigned_replica = ReplicationPeer {
            node_id: "node-b".to_string(),
            ops_url: format!("http://127.0.0.1:{}", addr.port()),
        };

        coordinator
            .replicate_session_state(
                7,
                replica_state(event.seq, 0),
                std::slice::from_ref(&event),
                &[assigned_replica.clone()],
            )
            .await
            .expect("first replicate");
        coordinator
            .replicate_session_state(
                7,
                replica_state(event.seq, 0),
                std::slice::from_ref(&event),
                &[assigned_replica],
            )
            .await
            .expect("second replicate");

        timeout(Duration::from_secs(2), handled_rx)
            .await
            .expect("handled signal")
            .expect("handled result");

        assert_eq!(accepted_connections.load(Ordering::Relaxed), 1);
    }

    async fn read_http_request(stream: &mut TcpStream) -> Result<Option<String>, std::io::Error> {
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

        Ok(Some(
            String::from_utf8(request[body_start..body_end].to_vec()).expect("request body"),
        ))
    }
}
