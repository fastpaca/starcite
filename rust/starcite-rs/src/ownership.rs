use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::{error::AppError, replication::ReplicationPeer, repository};

#[derive(Debug, Clone)]
pub struct OwnershipManager {
    leases: Arc<Mutex<HashMap<String, LocalLease>>>,
    pool: PgPool,
    instance_id: Arc<str>,
    lease_ttl: Duration,
    renew_before: Duration,
}

#[derive(Debug, Clone)]
struct LocalLease {
    epoch: i64,
    expires_at: Instant,
    standby: Option<ReplicationPeer>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OwnershipSnapshot {
    pub lease_ttl_ms: u64,
    pub renew_before_ms: u64,
    pub active_session_count: usize,
    pub sessions: Vec<OwnershipSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OwnershipSessionSnapshot {
    pub session_id: String,
    pub epoch: i64,
    pub expires_in_ms: u64,
    pub standby_node_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedLease {
    pub epoch: i64,
    pub standby: Option<ReplicationPeer>,
}

impl OwnershipManager {
    pub fn new(pool: PgPool, instance_id: Arc<str>, lease_ttl: Duration) -> Self {
        Self {
            leases: Arc::new(Mutex::new(HashMap::new())),
            pool,
            instance_id,
            lease_ttl,
            renew_before: renew_before(lease_ttl),
        }
    }

    pub async fn ensure_owned(&self, session_id: &str) -> Result<OwnedLease, AppError> {
        if let Some(lease) = self.cached_lease(session_id).await {
            return Ok(OwnedLease {
                epoch: lease.epoch,
                standby: lease.standby,
            });
        }

        let row = repository::acquire_session_lease(
            &self.pool,
            session_id,
            self.instance_id.as_ref(),
            self.lease_ttl.as_millis().min(i64::MAX as u128) as i64,
        )
        .await?;

        if row.owner_id != self.instance_id.as_ref() {
            return Err(AppError::SessionNotOwned {
                owner_id: row.owner_id,
                owner_public_url: row.owner_public_url,
                epoch: row.epoch,
            });
        }

        let lease = LocalLease {
            epoch: row.epoch,
            expires_at: lease_deadline(row.expires_at),
            standby: row
                .standby_node_id
                .zip(row.standby_ops_url)
                .map(|(node_id, ops_url)| ReplicationPeer { node_id, ops_url }),
        };
        self.leases
            .lock()
            .await
            .insert(session_id.to_string(), lease.clone());

        Ok(OwnedLease {
            epoch: lease.epoch,
            standby: lease.standby,
        })
    }

    pub async fn release(&self, session_id: &str) {
        self.leases.lock().await.remove(session_id);

        if let Err(error) =
            repository::release_session_lease(&self.pool, session_id, self.instance_id.as_ref())
                .await
        {
            tracing::warn!(error = ?error, session_id, "failed to release session lease");
        }
    }

    pub async fn snapshot(&self) -> OwnershipSnapshot {
        let now = Instant::now();
        let leases = self.leases.lock().await;
        let mut sessions = leases
            .iter()
            .filter(|(_session_id, lease)| lease.expires_at > now)
            .map(|(session_id, lease)| OwnershipSessionSnapshot {
                session_id: session_id.clone(),
                epoch: lease.epoch,
                expires_in_ms: lease
                    .expires_at
                    .saturating_duration_since(now)
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
                standby_node_id: lease
                    .standby
                    .as_ref()
                    .map(|standby| standby.node_id.clone()),
            })
            .collect::<Vec<_>>();

        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        OwnershipSnapshot {
            lease_ttl_ms: self.lease_ttl.as_millis().min(u64::MAX as u128) as u64,
            renew_before_ms: self.renew_before.as_millis().min(u64::MAX as u128) as u64,
            active_session_count: sessions.len(),
            sessions,
        }
    }

    async fn cached_lease(&self, session_id: &str) -> Option<LocalLease> {
        let now = Instant::now();
        let leases = self.leases.lock().await;
        let lease = leases.get(session_id)?;

        if lease.expires_at > now + self.renew_before {
            Some(lease.clone())
        } else {
            None
        }
    }

    pub fn renew_interval(&self) -> Duration {
        renew_interval(self.renew_before)
    }
}

fn renew_before(lease_ttl: Duration) -> Duration {
    let ttl_ms = lease_ttl.as_millis().min(u64::MAX as u128) as u64;
    Duration::from_millis((ttl_ms / 2).max(1))
}

fn renew_interval(renew_before: Duration) -> Duration {
    let renew_before_ms = renew_before.as_millis().min(u64::MAX as u128) as u64;
    Duration::from_millis((renew_before_ms / 2).max(1))
}

fn lease_deadline(expires_at: chrono::DateTime<Utc>) -> Instant {
    let now = Utc::now();
    if expires_at <= now {
        return Instant::now();
    }

    let remaining_ms = (expires_at - now).num_milliseconds().max(0) as u64;
    Instant::now() + Duration::from_millis(remaining_ms)
}

#[cfg(test)]
mod tests {
    use super::{LocalLease, OwnershipManager, lease_deadline, renew_before, renew_interval};
    use chrono::{Duration as ChronoDuration, Utc};
    use sqlx::postgres::PgPoolOptions;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    fn manager(lease_ttl: Duration) -> OwnershipManager {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");

        OwnershipManager::new(pool, Arc::<str>::from("node-a"), lease_ttl)
    }

    #[test]
    fn renews_before_half_life() {
        assert_eq!(renew_before(Duration::from_millis(10)).as_millis(), 5);
        assert_eq!(renew_before(Duration::from_millis(1)).as_millis(), 1);
    }

    #[test]
    fn renew_interval_uses_half_of_renew_window() {
        assert_eq!(renew_interval(Duration::from_millis(10)).as_millis(), 5);
        assert_eq!(renew_interval(Duration::from_millis(1)).as_millis(), 1);
    }

    #[test]
    fn lease_deadline_clamps_expired_rows_to_now() {
        let deadline = lease_deadline(Utc::now() - ChronoDuration::seconds(1));
        assert!(deadline <= Instant::now());
    }

    #[tokio::test]
    async fn snapshot_sorts_live_sessions() {
        let manager = manager(Duration::from_secs(5));
        let now = Instant::now();
        {
            let mut leases = manager.leases.lock().await;
            leases.insert(
                "ses_b".to_string(),
                LocalLease {
                    epoch: 2,
                    expires_at: now + Duration::from_secs(5),
                    standby: None,
                },
            );
            leases.insert(
                "ses_a".to_string(),
                LocalLease {
                    epoch: 1,
                    expires_at: now + Duration::from_secs(5),
                    standby: None,
                },
            );
        }

        let snapshot = manager.snapshot().await;

        assert_eq!(snapshot.active_session_count, 2);
        assert_eq!(snapshot.sessions[0].session_id, "ses_a");
        assert_eq!(snapshot.sessions[1].epoch, 2);
    }
}
