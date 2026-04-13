use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

use crate::{error::AppError, model::EventResponse};

const PREPARE_PATH: &str = "/internal/replication/prepare";
const COMMIT_PATH: &str = "/internal/replication/commit";

#[derive(Debug, Clone)]
pub struct ReplicationCoordinator {
    standby: Option<ControlPeer>,
    instance_id: Arc<str>,
    timeout: Duration,
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
    pub standby_url: Option<String>,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrepareReplicaRequest {
    pub owner_id: String,
    pub epoch: i64,
    pub event: EventResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitReplicaRequest {
    pub owner_id: String,
    pub epoch: i64,
    pub session_id: String,
    pub seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationAck {
    pub status: String,
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
        standby_url: Option<String>,
        timeout: Duration,
    ) -> Result<Self, String> {
        let standby = standby_url.as_deref().map(parse_control_peer).transpose()?;

        Ok(Self {
            standby,
            instance_id,
            timeout,
        })
    }

    pub async fn replicate(&self, epoch: i64, event: &EventResponse) -> Result<(), AppError> {
        let Some(standby) = self.standby.as_ref() else {
            return Ok(());
        };

        let owner_id = self.instance_id.to_string();

        self.post_json(
            standby,
            PREPARE_PATH,
            &PrepareReplicaRequest {
                owner_id: owner_id.clone(),
                epoch,
                event: event.clone(),
            },
        )
        .await
        .map_err(|error| self.quorum_error(error, standby, "prepare", event))?;

        self.post_json(
            standby,
            COMMIT_PATH,
            &CommitReplicaRequest {
                owner_id,
                epoch,
                session_id: event.session_id.clone(),
                seq: event.seq,
            },
        )
        .await
        .map_err(|error| self.quorum_error(error, standby, "commit", event))?;

        Ok(())
    }

    pub fn snapshot(&self) -> ReplicationSnapshot {
        ReplicationSnapshot {
            enabled: self.standby.is_some(),
            standby_url: self.standby.as_ref().map(|peer| peer.base_url.clone()),
            timeout_ms: self.timeout.as_millis().min(u64::MAX as u128) as u64,
        }
    }

    fn quorum_error(
        &self,
        error: ReplicationClientError,
        standby: &ControlPeer,
        phase: &str,
        event: &EventResponse,
    ) -> AppError {
        let message = error.message();
        tracing::warn!(
            error = message,
            standby = standby.base_url,
            phase,
            session_id = event.session_id,
            seq = event.seq,
            "standby replication failed"
        );

        AppError::QuorumUnavailable {
            required: 2,
            acknowledged: 1,
        }
    }

    async fn post_json<T: Serialize>(
        &self,
        peer: &ControlPeer,
        path: &str,
        body: &T,
    ) -> Result<(), ReplicationClientError> {
        let payload = serde_json::to_string(body).map_err(ReplicationClientError::Encode)?;
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n{payload}",
            host = peer.authority,
            content_length = payload.len()
        );

        let mut stream = timeout(self.timeout, TcpStream::connect(&peer.connect_addr))
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Connect)?;

        timeout(self.timeout, stream.write_all(request.as_bytes()))
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Write)?;

        let mut response = Vec::new();
        timeout(self.timeout, stream.read_to_end(&mut response))
            .await
            .map_err(|_| ReplicationClientError::Timeout)?
            .map_err(ReplicationClientError::Read)?;

        let (status, body) = parse_http_response(&response)?;
        if (200..300).contains(&status) {
            return Ok(());
        }

        Err(ReplicationClientError::HttpStatus { status, body })
    }
}

fn parse_control_peer(raw: &str) -> Result<ControlPeer, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let Some(rest) = trimmed.strip_prefix("http://") else {
        return Err(format!(
            "LOCAL_ASYNC_STANDBY_URL must start with http://: {raw}"
        ));
    };

    if rest.is_empty() || rest.contains('/') {
        return Err(format!(
            "LOCAL_ASYNC_STANDBY_URL must be a bare http host:port base URL: {raw}"
        ));
    }

    let Some((host, port_raw)) = rest.rsplit_once(':') else {
        return Err(format!(
            "LOCAL_ASYNC_STANDBY_URL must include a port: {raw}"
        ));
    };

    if host.is_empty() {
        return Err(format!("LOCAL_ASYNC_STANDBY_URL host is empty: {raw}"));
    }

    let port = port_raw
        .parse::<u16>()
        .map_err(|_| format!("LOCAL_ASYNC_STANDBY_URL has invalid port: {raw}"))?;

    if port == 0 {
        return Err(format!("LOCAL_ASYNC_STANDBY_URL has invalid port: {raw}"));
    }

    Ok(ControlPeer {
        base_url: trimmed.to_string(),
        authority: rest.to_string(),
        connect_addr: format!("{host}:{port}"),
    })
}

fn parse_http_response(bytes: &[u8]) -> Result<(u16, String), ReplicationClientError> {
    let response = std::str::from_utf8(bytes).map_err(|_| ReplicationClientError::BadResponse)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or(ReplicationClientError::BadResponse)?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or(ReplicationClientError::BadResponse)?;

    Ok((status, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{CommitReplicaRequest, PrepareReplicaRequest, parse_control_peer};
    use crate::model::EventResponse;
    use serde_json::Map;

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
            cursor: 1,
        }
    }

    #[test]
    fn parses_standby_base_url() {
        let peer = parse_control_peer("http://127.0.0.1:4194").expect("peer");

        assert_eq!(peer.base_url, "http://127.0.0.1:4194");
        assert_eq!(peer.authority, "127.0.0.1:4194");
        assert_eq!(peer.connect_addr, "127.0.0.1:4194");
    }

    #[test]
    fn rejects_standby_url_without_port() {
        let error = parse_control_peer("http://127.0.0.1").expect_err("missing port");
        assert!(error.contains("must include a port"));
    }

    #[test]
    fn replica_requests_round_trip_through_json() {
        let prepare = PrepareReplicaRequest {
            owner_id: "node-a".to_string(),
            epoch: 2,
            event: event(),
        };
        let commit = CommitReplicaRequest {
            owner_id: "node-a".to_string(),
            epoch: 2,
            session_id: "ses_demo".to_string(),
            seq: 1,
        };

        let prepare_json = serde_json::to_string(&prepare).expect("prepare json");
        let commit_json = serde_json::to_string(&commit).expect("commit json");

        assert_eq!(
            serde_json::from_str::<PrepareReplicaRequest>(&prepare_json).expect("prepare decode"),
            prepare
        );
        assert_eq!(
            serde_json::from_str::<CommitReplicaRequest>(&commit_json).expect("commit decode"),
            commit
        );
    }
}
