use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use serde::Serialize;
use sqlx::PgPool;
use tokio::time::sleep;

use crate::{data_plane::repository, runtime::OpsState};

#[derive(Debug, Clone)]
pub struct ControlPlaneState {
    public_url: Option<Arc<str>>,
    advertise_url: Option<Arc<str>>,
    node_ttl: Duration,
    heartbeat: Arc<RwLock<ControlPlaneHealth>>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ControlPlaneSnapshot {
    pub enabled: bool,
    pub public_url: Option<String>,
    pub advertise_url: Option<String>,
    pub node_ttl_ms: u64,
    pub ready: bool,
    pub heartbeat_status: &'static str,
    pub reason: Option<&'static str>,
    pub last_heartbeat_age_ms: Option<u64>,
}

#[derive(Debug, Clone)]
enum ControlPlaneHealth {
    Disabled,
    Starting,
    SchemaWaiting,
    Error,
    Healthy { last_success: Instant },
}

impl ControlPlaneState {
    pub fn new(
        public_url: Option<String>,
        advertise_url: Option<String>,
        node_ttl: Duration,
    ) -> Self {
        let enabled = advertise_url.is_some();

        Self {
            public_url: public_url.map(Arc::from),
            advertise_url: advertise_url.map(Arc::from),
            node_ttl,
            heartbeat: Arc::new(RwLock::new(if enabled {
                ControlPlaneHealth::Starting
            } else {
                ControlPlaneHealth::Disabled
            })),
        }
    }

    pub fn spawn(self, pool: PgPool, instance_id: Arc<str>, ops: OpsState) {
        let Some(advertise_url) = self.advertise_url.clone() else {
            return;
        };
        let public_url = self.public_url.clone();
        let heartbeat = self.heartbeat.clone();

        let heartbeat_interval = refresh_interval(self.node_ttl);
        let bootstrap_interval = bootstrap_interval(self.node_ttl);
        let ttl_ms = self.node_ttl.as_millis().min(i64::MAX as u128) as i64;

        tokio::spawn(async move {
            loop {
                match repository::control_plane_table_exists(&pool).await {
                    Ok(false) => {
                        set_health(&heartbeat, ControlPlaneHealth::SchemaWaiting);
                        sleep(bootstrap_interval).await;
                        continue;
                    }
                    Ok(true) => {}
                    Err(error) => {
                        set_health(&heartbeat, ControlPlaneHealth::Error);
                        tracing::warn!(
                            error = ?error,
                            "failed to check control-plane schema readiness"
                        );
                        sleep(heartbeat_interval).await;
                        continue;
                    }
                }

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
                    set_health(&heartbeat, ControlPlaneHealth::Error);
                    tracing::warn!(error = ?error, "failed to refresh control-plane heartbeat");
                } else {
                    set_health(
                        &heartbeat,
                        ControlPlaneHealth::Healthy {
                            last_success: Instant::now(),
                        },
                    );
                }

                sleep(heartbeat_interval).await;
            }
        });
    }

    pub fn snapshot(&self) -> ControlPlaneSnapshot {
        let health = self
            .heartbeat
            .read()
            .expect("control plane heartbeat lock")
            .clone();
        let (ready, heartbeat_status, reason, last_heartbeat_age_ms) =
            heartbeat_snapshot(&health, self.node_ttl);

        ControlPlaneSnapshot {
            enabled: self.advertise_url.is_some(),
            public_url: self.public_url.as_ref().map(|value| value.to_string()),
            advertise_url: self.advertise_url.as_ref().map(|value| value.to_string()),
            node_ttl_ms: self.node_ttl.as_millis().min(u64::MAX as u128) as u64,
            ready,
            heartbeat_status,
            reason,
            last_heartbeat_age_ms,
        }
    }

    pub fn enabled(&self) -> bool {
        self.advertise_url.is_some()
    }

    pub fn readiness_reason(&self) -> Option<&'static str> {
        self.snapshot().reason
    }
}

fn refresh_interval(node_ttl: Duration) -> Duration {
    let ttl_ms = node_ttl.as_millis().min(u64::MAX as u128) as u64;
    Duration::from_millis((ttl_ms / 2).max(1))
}

fn bootstrap_interval(node_ttl: Duration) -> Duration {
    refresh_interval(node_ttl).min(Duration::from_millis(500))
}

fn set_health(heartbeat: &RwLock<ControlPlaneHealth>, health: ControlPlaneHealth) {
    *heartbeat.write().expect("control plane heartbeat lock") = health;
}

fn heartbeat_snapshot(
    health: &ControlPlaneHealth,
    node_ttl: Duration,
) -> (bool, &'static str, Option<&'static str>, Option<u64>) {
    match health {
        ControlPlaneHealth::Disabled => (true, "disabled", None, None),
        ControlPlaneHealth::Starting => (false, "starting", Some("routing_sync"), None),
        ControlPlaneHealth::SchemaWaiting => (false, "schema_waiting", Some("routing_sync"), None),
        ControlPlaneHealth::Error => (false, "error", Some("routing_sync"), None),
        ControlPlaneHealth::Healthy { last_success } => {
            let age_ms = last_success.elapsed().as_millis().min(u64::MAX as u128) as u64;

            if last_success.elapsed() > node_ttl {
                (false, "stale", Some("routing_sync"), Some(age_ms))
            } else {
                (true, "ready", None, Some(age_ms))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ControlPlaneHealth, ControlPlaneState, bootstrap_interval, heartbeat_snapshot,
        refresh_interval,
    };
    use std::time::{Duration, Instant};

    #[test]
    fn heartbeat_interval_uses_half_ttl() {
        assert_eq!(refresh_interval(Duration::from_millis(10)).as_millis(), 5);
        assert_eq!(refresh_interval(Duration::from_millis(1)).as_millis(), 1);
    }

    #[test]
    fn bootstrap_interval_caps_wait_time() {
        assert_eq!(bootstrap_interval(Duration::from_secs(10)).as_millis(), 500);
        assert_eq!(bootstrap_interval(Duration::from_millis(10)).as_millis(), 5);
    }

    #[test]
    fn snapshot_reports_disabled_state() {
        let snapshot = ControlPlaneState::new(None, None, Duration::from_secs(3)).snapshot();

        assert!(!snapshot.enabled);
        assert_eq!(snapshot.public_url, None);
        assert_eq!(snapshot.advertise_url, None);
        assert_eq!(snapshot.node_ttl_ms, 3_000);
        assert!(snapshot.ready);
        assert_eq!(snapshot.heartbeat_status, "disabled");
    }

    #[test]
    fn heartbeat_snapshot_marks_starting_state_unready() {
        let (ready, status, reason, age_ms) =
            heartbeat_snapshot(&ControlPlaneHealth::Starting, Duration::from_secs(3));

        assert!(!ready);
        assert_eq!(status, "starting");
        assert_eq!(reason, Some("routing_sync"));
        assert_eq!(age_ms, None);
    }

    #[test]
    fn heartbeat_snapshot_marks_stale_heartbeats_unready() {
        let (ready, status, reason, age_ms) = heartbeat_snapshot(
            &ControlPlaneHealth::Healthy {
                last_success: Instant::now() - Duration::from_secs(5),
            },
            Duration::from_secs(3),
        );

        assert!(!ready);
        assert_eq!(status, "stale");
        assert_eq!(reason, Some("routing_sync"));
        assert!(age_ms.is_some());
    }
}
