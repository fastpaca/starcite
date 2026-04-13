use std::{sync::Arc, time::Duration};

use serde::Serialize;
use sqlx::PgPool;
use tokio::time::sleep;

use crate::{ops::OpsState, repository};

#[derive(Debug, Clone)]
pub struct ControlPlaneState {
    public_url: Option<Arc<str>>,
    advertise_url: Option<Arc<str>>,
    node_ttl: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ControlPlaneSnapshot {
    pub enabled: bool,
    pub public_url: Option<String>,
    pub advertise_url: Option<String>,
    pub node_ttl_ms: u64,
}

impl ControlPlaneState {
    pub fn new(
        public_url: Option<String>,
        advertise_url: Option<String>,
        node_ttl: Duration,
    ) -> Self {
        Self {
            public_url: public_url.map(Arc::from),
            advertise_url: advertise_url.map(Arc::from),
            node_ttl,
        }
    }

    pub fn spawn(self, pool: PgPool, instance_id: Arc<str>, ops: OpsState) {
        let Some(advertise_url) = self.advertise_url.clone() else {
            return;
        };
        let public_url = self.public_url.clone();

        let heartbeat_interval = refresh_interval(self.node_ttl);
        let ttl_ms = self.node_ttl.as_millis().min(i64::MAX as u128) as i64;

        tokio::spawn(async move {
            loop {
                let draining = ops.snapshot().draining;

                if let Err(error) = repository::upsert_control_node(
                    &pool,
                    instance_id.as_ref(),
                    public_url.as_deref(),
                    advertise_url.as_ref(),
                    draining,
                    ttl_ms,
                )
                .await
                {
                    tracing::warn!(error = ?error, "failed to refresh control-plane heartbeat");
                }

                sleep(heartbeat_interval).await;
            }
        });
    }

    pub fn snapshot(&self) -> ControlPlaneSnapshot {
        ControlPlaneSnapshot {
            enabled: self.advertise_url.is_some(),
            public_url: self.public_url.as_ref().map(|value| value.to_string()),
            advertise_url: self.advertise_url.as_ref().map(|value| value.to_string()),
            node_ttl_ms: self.node_ttl.as_millis().min(u64::MAX as u128) as u64,
        }
    }

    pub fn enabled(&self) -> bool {
        self.advertise_url.is_some()
    }
}

fn refresh_interval(node_ttl: Duration) -> Duration {
    let ttl_ms = node_ttl.as_millis().min(u64::MAX as u128) as u64;
    Duration::from_millis((ttl_ms / 2).max(1))
}

#[cfg(test)]
mod tests {
    use super::{ControlPlaneState, refresh_interval};
    use std::time::Duration;

    #[test]
    fn heartbeat_interval_uses_half_ttl() {
        assert_eq!(refresh_interval(Duration::from_millis(10)).as_millis(), 5);
        assert_eq!(refresh_interval(Duration::from_millis(1)).as_millis(), 1);
    }

    #[test]
    fn snapshot_reports_disabled_state() {
        let snapshot = ControlPlaneState::new(None, None, Duration::from_secs(3)).snapshot();

        assert!(!snapshot.enabled);
        assert_eq!(snapshot.public_url, None);
        assert_eq!(snapshot.advertise_url, None);
        assert_eq!(snapshot.node_ttl_ms, 3_000);
    }
}
