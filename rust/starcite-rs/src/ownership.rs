use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use serde::Serialize;
use sqlx::PgPool;
use tokio::sync::Mutex;

use crate::{
    error::AppError,
    replication::ReplicationPeer,
    repository::{self, SessionLeaseTakeoverHint},
};

#[derive(Debug, Clone)]
pub struct OwnershipManager {
    leases: Arc<Mutex<HashMap<String, LocalLease>>>,
    remote_owners: Arc<Mutex<HashMap<String, RemoteOwnerHint>>>,
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

#[derive(Debug, Clone)]
struct RemoteOwnerHint {
    owner_id: String,
    owner_public_url: String,
    epoch: i64,
    expires_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedRemoteOwner {
    pub owner_id: String,
    pub owner_public_url: String,
    pub epoch: i64,
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
            remote_owners: Arc::new(Mutex::new(HashMap::new())),
            pool,
            instance_id,
            lease_ttl,
            renew_before: renew_before(lease_ttl),
        }
    }

    pub async fn ensure_owned(&self, session_id: &str) -> Result<OwnedLease, AppError> {
        if let Some(lease) = self.cached_lease(session_id).await {
            self.forget_remote_owner_hint(session_id).await;
            return Ok(OwnedLease {
                epoch: lease.epoch,
                standby: lease.standby,
            });
        }

        if let Some(redirect) = self.cached_remote_owner_hint(session_id).await {
            return Err(session_not_owned(
                redirect.owner_id,
                redirect.owner_public_url,
                redirect.epoch,
            ));
        }

        if let Some(hint) =
            repository::load_session_lease_takeover_hint(&self.pool, session_id).await?
            && let Some(redirect) = preferred_takeover_owner(&hint, self.instance_id.as_ref())
        {
            return Err(self
                .reject_remote_owner(
                    session_id,
                    redirect.owner_id,
                    redirect.owner_public_url,
                    redirect.epoch,
                )
                .await);
        }

        let row = repository::acquire_session_lease(
            &self.pool,
            session_id,
            self.instance_id.as_ref(),
            self.lease_ttl.as_millis().min(i64::MAX as u128) as i64,
        )
        .await?;

        if row.owner_id != self.instance_id.as_ref() {
            return Err(self
                .reject_remote_owner(session_id, row.owner_id, row.owner_public_url, row.epoch)
                .await);
        }

        self.forget_remote_owner_hint(session_id).await;

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

    pub async fn live_or_renew_owned(&self, session_id: &str) -> Result<OwnedLease, AppError> {
        if let Some(lease) = self.live_owned_lease(session_id).await {
            return Ok(lease);
        }

        self.ensure_owned(session_id).await
    }

    pub async fn cached_remote_owner(&self, session_id: &str) -> Option<CachedRemoteOwner> {
        self.cached_remote_owner_hint(session_id)
            .await
            .and_then(|redirect| {
                redirect
                    .owner_public_url
                    .map(|owner_public_url| CachedRemoteOwner {
                        owner_id: redirect.owner_id,
                        owner_public_url,
                        epoch: redirect.epoch,
                    })
            })
    }

    async fn live_owned_lease(&self, session_id: &str) -> Option<OwnedLease> {
        self.live_cached_lease(session_id)
            .await
            .map(|lease| OwnedLease {
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

    pub async fn owned_epoch(&self, session_id: &str) -> Option<i64> {
        let now = Instant::now();
        let leases = self.leases.lock().await;
        let lease = leases.get(session_id)?;

        (lease.expires_at > now).then_some(lease.epoch)
    }

    pub async fn forget_remote_owner_hint(&self, session_id: &str) {
        self.remote_owners.lock().await.remove(session_id);
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

    async fn live_cached_lease(&self, session_id: &str) -> Option<LocalLease> {
        let now = Instant::now();
        let leases = self.leases.lock().await;
        let lease = leases.get(session_id)?;

        (lease.expires_at > now).then(|| lease.clone())
    }

    async fn cached_remote_owner_hint(&self, session_id: &str) -> Option<TakeoverRedirect> {
        let now = Instant::now();
        let mut remote_owners = self.remote_owners.lock().await;
        let hint = remote_owners.get(session_id)?;

        if hint.expires_at <= now {
            remote_owners.remove(session_id);
            return None;
        }

        Some(TakeoverRedirect {
            owner_id: hint.owner_id.clone(),
            owner_public_url: Some(hint.owner_public_url.clone()),
            epoch: hint.epoch,
        })
    }

    async fn reject_remote_owner(
        &self,
        session_id: &str,
        owner_id: String,
        owner_public_url: Option<String>,
        epoch: i64,
    ) -> AppError {
        self.remember_remote_owner_hint(session_id, &owner_id, owner_public_url.as_deref(), epoch)
            .await;

        session_not_owned(owner_id, owner_public_url, epoch)
    }

    async fn remember_remote_owner_hint(
        &self,
        session_id: &str,
        owner_id: &str,
        owner_public_url: Option<&str>,
        epoch: i64,
    ) {
        let Some(owner_public_url) = owner_public_url else {
            self.forget_remote_owner_hint(session_id).await;
            return;
        };

        if owner_id == self.instance_id.as_ref() {
            self.forget_remote_owner_hint(session_id).await;
            return;
        }

        let expires_at = Instant::now() + self.renew_before;
        self.remote_owners.lock().await.insert(
            session_id.to_string(),
            RemoteOwnerHint {
                owner_id: owner_id.to_string(),
                owner_public_url: owner_public_url.to_string(),
                epoch,
                expires_at,
            },
        );
    }

    pub fn renew_interval(&self) -> Duration {
        renew_interval(self.renew_before)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TakeoverRedirect {
    owner_id: String,
    owner_public_url: Option<String>,
    epoch: i64,
}

fn session_not_owned(owner_id: String, owner_public_url: Option<String>, epoch: i64) -> AppError {
    AppError::SessionNotOwned {
        owner_id,
        owner_public_url,
        epoch,
    }
}

fn preferred_takeover_owner(
    hint: &SessionLeaseTakeoverHint,
    requester_id: &str,
) -> Option<TakeoverRedirect> {
    if hint.expires_at > Utc::now() {
        return None;
    }

    let standby_id = hint.live_standby_node_id.as_ref()?;
    if standby_id == requester_id {
        return None;
    }

    Some(TakeoverRedirect {
        owner_id: standby_id.clone(),
        owner_public_url: hint.live_standby_public_url.clone(),
        epoch: hint.epoch.saturating_add(1),
    })
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
    use super::{
        LocalLease, OwnershipManager, lease_deadline, preferred_takeover_owner, renew_before,
        renew_interval,
    };
    use chrono::{Duration as ChronoDuration, Utc};
    use sqlx::postgres::PgPoolOptions;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    use crate::{error::AppError, repository::SessionLeaseTakeoverHint};

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

    #[test]
    fn expired_lease_redirects_non_standby_to_live_standby() {
        let redirect = preferred_takeover_owner(
            &SessionLeaseTakeoverHint {
                epoch: 7,
                expires_at: Utc::now() - ChronoDuration::seconds(1),
                live_standby_node_id: Some("node-b".to_string()),
                live_standby_public_url: Some("http://node-b:4001".to_string()),
            },
            "node-c",
        )
        .expect("redirect");

        assert_eq!(redirect.owner_id, "node-b");
        assert_eq!(
            redirect.owner_public_url.as_deref(),
            Some("http://node-b:4001")
        );
        assert_eq!(redirect.epoch, 8);
    }

    #[test]
    fn expired_lease_allows_live_standby_to_claim() {
        let redirect = preferred_takeover_owner(
            &SessionLeaseTakeoverHint {
                epoch: 7,
                expires_at: Utc::now() - ChronoDuration::seconds(1),
                live_standby_node_id: Some("node-b".to_string()),
                live_standby_public_url: Some("http://node-b:4001".to_string()),
            },
            "node-b",
        );

        assert_eq!(redirect, None);
    }

    #[test]
    fn expired_lease_without_live_standby_does_not_redirect() {
        let redirect = preferred_takeover_owner(
            &SessionLeaseTakeoverHint {
                epoch: 7,
                expires_at: Utc::now() - ChronoDuration::seconds(1),
                live_standby_node_id: None,
                live_standby_public_url: None,
            },
            "node-c",
        );

        assert_eq!(redirect, None);
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

    #[tokio::test]
    async fn live_cached_lease_stays_valid_inside_renew_window() {
        let manager = manager(Duration::from_secs(10));
        let now = Instant::now();
        {
            let mut leases = manager.leases.lock().await;
            leases.insert(
                "ses_a".to_string(),
                LocalLease {
                    epoch: 4,
                    expires_at: now + Duration::from_secs(2),
                    standby: None,
                },
            );
        }

        let lease = manager
            .live_cached_lease("ses_a")
            .await
            .expect("cached lease");

        assert_eq!(lease.epoch, 4);
        assert!(manager.cached_lease("ses_a").await.is_none());
    }

    #[tokio::test]
    async fn live_or_renew_owned_prefers_live_cached_lease() {
        let manager = manager(Duration::from_secs(10));
        let now = Instant::now();
        {
            let mut leases = manager.leases.lock().await;
            leases.insert(
                "ses_a".to_string(),
                LocalLease {
                    epoch: 4,
                    expires_at: now + Duration::from_secs(2),
                    standby: None,
                },
            );
        }

        let lease = manager
            .live_or_renew_owned("ses_a")
            .await
            .expect("owned lease");

        assert_eq!(lease.epoch, 4);
    }

    #[tokio::test]
    async fn live_or_renew_owned_prefers_cached_remote_owner_hint() {
        let manager = manager(Duration::from_secs(10));
        manager
            .remember_remote_owner_hint("ses_a", "node-b", Some("http://node-b:4001"), 4)
            .await;

        let error = manager
            .live_or_renew_owned("ses_a")
            .await
            .expect_err("remote owner redirect");

        match error {
            AppError::SessionNotOwned {
                owner_id,
                owner_public_url,
                epoch,
            } => {
                assert_eq!(owner_id, "node-b");
                assert_eq!(owner_public_url.as_deref(), Some("http://node-b:4001"));
                assert_eq!(epoch, 4);
            }
            other => panic!("expected session_not_owned, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn live_cached_lease_rejects_expired_entry() {
        let manager = manager(Duration::from_secs(10));
        let now = Instant::now();
        {
            let mut leases = manager.leases.lock().await;
            leases.insert(
                "ses_a".to_string(),
                LocalLease {
                    epoch: 4,
                    expires_at: now - Duration::from_millis(1),
                    standby: None,
                },
            );
        }

        assert!(manager.live_cached_lease("ses_a").await.is_none());
        assert!(manager.cached_lease("ses_a").await.is_none());
    }
}
