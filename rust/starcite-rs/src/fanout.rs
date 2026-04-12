use std::{collections::HashMap, sync::Arc};

use serde::Serialize;
use tokio::sync::{RwLock, broadcast};

use crate::model::{EventResponse, LifecycleResponse};

#[derive(Debug, Clone)]
pub struct SessionFanout {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<EventResponse>>>>,
    capacity: usize,
}

#[derive(Debug, Clone)]
pub struct SessionSubscriptionGuard {
    fanout: SessionFanout,
    session_id: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionFanoutSnapshot {
    pub active_session_count: usize,
    pub sessions: Vec<SessionSubscriptionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SessionSubscriptionSnapshot {
    pub session_id: String,
    pub subscribers: usize,
}

impl SessionFanout {
    pub fn new(capacity: usize) -> Self {
        Self {
            channels: Arc::new(RwLock::new(HashMap::new())),
            capacity,
        }
    }

    pub async fn subscribe(&self, session_id: &str) -> broadcast::Receiver<EventResponse> {
        self.sender(session_id).await.subscribe()
    }

    pub async fn broadcast(&self, event: EventResponse) {
        let session_id = event.session_id.clone();
        let sender = match self.channels.read().await.get(&session_id).cloned() {
            Some(sender) => sender,
            None => return,
        };

        if sender.receiver_count() == 0 {
            remove_idle_sender(&self.channels, &session_id, &sender).await;
            return;
        }

        if sender.send(event).is_err() {
            remove_idle_sender(&self.channels, &session_id, &sender).await;
        }
    }

    pub fn session_guard(&self, session_id: impl Into<String>) -> SessionSubscriptionGuard {
        SessionSubscriptionGuard {
            fanout: self.clone(),
            session_id: session_id.into(),
        }
    }

    async fn sender(&self, session_id: &str) -> broadcast::Sender<EventResponse> {
        if let Some(sender) = self.channels.read().await.get(session_id).cloned() {
            return sender;
        }

        let mut channels = self.channels.write().await;

        if let Some(sender) = channels.get(session_id).cloned() {
            return sender;
        }

        let (sender, _receiver) = broadcast::channel(self.capacity);
        channels.insert(session_id.to_string(), sender.clone());
        sender
    }

    pub async fn snapshot(&self) -> SessionFanoutSnapshot {
        let channels = self.channels.read().await;
        let mut sessions = channels
            .iter()
            .filter(|(_, sender)| sender.receiver_count() > 0)
            .map(|(session_id, sender)| SessionSubscriptionSnapshot {
                session_id: session_id.clone(),
                subscribers: sender.receiver_count(),
            })
            .collect::<Vec<_>>();

        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        SessionFanoutSnapshot {
            active_session_count: sessions.len(),
            sessions,
        }
    }

    pub async fn prune_idle_session(&self, session_id: &str) {
        prune_idle_key(&self.channels, session_id).await;
    }
}

impl Default for SessionFanout {
    fn default() -> Self {
        Self::new(1_024)
    }
}

#[derive(Debug, Clone)]
pub struct LifecycleFanout {
    tenant_channels: Arc<RwLock<HashMap<String, broadcast::Sender<LifecycleResponse>>>>,
    session_channels: Arc<RwLock<HashMap<String, broadcast::Sender<LifecycleResponse>>>>,
    capacity: usize,
}

#[derive(Debug, Clone)]
pub struct LifecycleSubscriptionGuard {
    fanout: LifecycleFanout,
    key: String,
    scope: LifecycleScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LifecycleScope {
    Tenant,
    Session,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct LifecycleFanoutSnapshot {
    pub active_tenant_count: usize,
    pub active_session_count: usize,
    pub tenants: Vec<TenantSubscriptionSnapshot>,
    pub sessions: Vec<SessionSubscriptionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TenantSubscriptionSnapshot {
    pub tenant_id: String,
    pub subscribers: usize,
}

impl LifecycleFanout {
    pub fn new(capacity: usize) -> Self {
        Self {
            tenant_channels: Arc::new(RwLock::new(HashMap::new())),
            session_channels: Arc::new(RwLock::new(HashMap::new())),
            capacity,
        }
    }

    pub async fn subscribe_tenant(
        &self,
        tenant_id: &str,
    ) -> broadcast::Receiver<LifecycleResponse> {
        self.tenant_sender(tenant_id).await.subscribe()
    }

    pub async fn subscribe_session(
        &self,
        session_id: &str,
    ) -> broadcast::Receiver<LifecycleResponse> {
        self.session_sender(session_id).await.subscribe()
    }

    pub async fn broadcast(&self, event: LifecycleResponse) {
        let tenant_id = event.event.tenant_id().to_string();
        let session_id = event.event.session_id().to_string();

        broadcast_existing(&self.tenant_channels, &tenant_id, event.clone()).await;
        broadcast_existing(&self.session_channels, &session_id, event).await;
    }

    pub fn tenant_guard(&self, tenant_id: impl Into<String>) -> LifecycleSubscriptionGuard {
        LifecycleSubscriptionGuard {
            fanout: self.clone(),
            key: tenant_id.into(),
            scope: LifecycleScope::Tenant,
        }
    }

    pub fn session_guard(&self, session_id: impl Into<String>) -> LifecycleSubscriptionGuard {
        LifecycleSubscriptionGuard {
            fanout: self.clone(),
            key: session_id.into(),
            scope: LifecycleScope::Session,
        }
    }

    async fn tenant_sender(&self, tenant_id: &str) -> broadcast::Sender<LifecycleResponse> {
        if let Some(sender) = self.tenant_channels.read().await.get(tenant_id).cloned() {
            return sender;
        }

        let mut channels = self.tenant_channels.write().await;

        if let Some(sender) = channels.get(tenant_id).cloned() {
            return sender;
        }

        let (sender, _receiver) = broadcast::channel(self.capacity);
        channels.insert(tenant_id.to_string(), sender.clone());
        sender
    }

    async fn session_sender(&self, session_id: &str) -> broadcast::Sender<LifecycleResponse> {
        if let Some(sender) = self.session_channels.read().await.get(session_id).cloned() {
            return sender;
        }

        let mut channels = self.session_channels.write().await;

        if let Some(sender) = channels.get(session_id).cloned() {
            return sender;
        }

        let (sender, _receiver) = broadcast::channel(self.capacity);
        channels.insert(session_id.to_string(), sender.clone());
        sender
    }

    pub async fn snapshot(&self) -> LifecycleFanoutSnapshot {
        let tenant_channels = self.tenant_channels.read().await;
        let session_channels = self.session_channels.read().await;
        let mut tenants = tenant_channels
            .iter()
            .filter(|(_, sender)| sender.receiver_count() > 0)
            .map(|(tenant_id, sender)| TenantSubscriptionSnapshot {
                tenant_id: tenant_id.clone(),
                subscribers: sender.receiver_count(),
            })
            .collect::<Vec<_>>();
        let mut sessions = session_channels
            .iter()
            .filter(|(_, sender)| sender.receiver_count() > 0)
            .map(|(session_id, sender)| SessionSubscriptionSnapshot {
                session_id: session_id.clone(),
                subscribers: sender.receiver_count(),
            })
            .collect::<Vec<_>>();

        tenants.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        LifecycleFanoutSnapshot {
            active_tenant_count: tenants.len(),
            active_session_count: sessions.len(),
            tenants,
            sessions,
        }
    }

    pub async fn prune_idle_tenant(&self, tenant_id: &str) {
        prune_idle_key(&self.tenant_channels, tenant_id).await;
    }

    pub async fn prune_idle_session(&self, session_id: &str) {
        prune_idle_key(&self.session_channels, session_id).await;
    }
}

impl Default for LifecycleFanout {
    fn default() -> Self {
        Self::new(1_024)
    }
}

async fn broadcast_existing<T: Clone>(
    channels: &Arc<RwLock<HashMap<String, broadcast::Sender<T>>>>,
    key: &str,
    message: T,
) {
    let sender = match channels.read().await.get(key).cloned() {
        Some(sender) => sender,
        None => return,
    };

    if sender.receiver_count() == 0 {
        remove_idle_sender(channels, key, &sender).await;
        return;
    }

    if sender.send(message).is_err() {
        remove_idle_sender(channels, key, &sender).await;
    }
}

async fn remove_idle_sender<T: Clone>(
    channels: &Arc<RwLock<HashMap<String, broadcast::Sender<T>>>>,
    key: &str,
    sender: &broadcast::Sender<T>,
) {
    let mut channels = channels.write().await;

    if channels
        .get(key)
        .is_some_and(|current| current.same_channel(sender) && current.receiver_count() == 0)
    {
        channels.remove(key);
    }
}

async fn prune_idle_key<T: Clone>(
    channels: &Arc<RwLock<HashMap<String, broadcast::Sender<T>>>>,
    key: &str,
) {
    let mut channels = channels.write().await;

    if channels
        .get(key)
        .is_some_and(|sender| sender.receiver_count() == 0)
    {
        channels.remove(key);
    }
}

impl Drop for SessionSubscriptionGuard {
    fn drop(&mut self) {
        let fanout = self.fanout.clone();
        let session_id = self.session_id.clone();

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                fanout.prune_idle_session(&session_id).await;
            });
        }
    }
}

impl Drop for LifecycleSubscriptionGuard {
    fn drop(&mut self) {
        let fanout = self.fanout.clone();
        let key = self.key.clone();
        let scope = self.scope;

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                match scope {
                    LifecycleScope::Tenant => fanout.prune_idle_tenant(&key).await,
                    LifecycleScope::Session => fanout.prune_idle_session(&key).await,
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tokio::task::yield_now;

    use super::{
        LifecycleFanout, LifecycleFanoutSnapshot, SessionFanout, SessionFanoutSnapshot,
        SessionSubscriptionSnapshot, TenantSubscriptionSnapshot,
    };
    use crate::model::{EventResponse, LifecycleEvent, LifecycleResponse, SessionResponse};

    #[tokio::test]
    async fn broadcasts_to_existing_subscribers() {
        let fanout = SessionFanout::new(8);
        let mut receiver = fanout.subscribe("ses_demo").await;

        fanout.broadcast(sample_event("ses_demo")).await;

        let event = receiver.recv().await.expect("event should arrive");
        assert_eq!(event.seq, 1);
        assert_eq!(event.session_id, "ses_demo");
        assert_eq!(fanout.channels.read().await.len(), 1);
    }

    #[tokio::test]
    async fn ignores_session_broadcasts_without_subscribers() {
        let fanout = SessionFanout::new(8);

        fanout.broadcast(sample_event("ses_demo")).await;

        assert!(fanout.channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn prunes_idle_session_channels_after_last_receiver_disconnects() {
        let fanout = SessionFanout::new(8);
        let receiver = fanout.subscribe("ses_demo").await;

        assert_eq!(fanout.channels.read().await.len(), 1);

        drop(receiver);
        fanout.broadcast(sample_event("ses_demo")).await;

        assert!(fanout.channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn session_snapshot_reports_subscriber_counts() {
        let fanout = SessionFanout::new(8);
        let _receiver_a = fanout.subscribe("ses_a").await;
        let _receiver_b1 = fanout.subscribe("ses_b").await;
        let _receiver_b2 = fanout.subscribe("ses_b").await;

        assert_eq!(
            fanout.snapshot().await,
            SessionFanoutSnapshot {
                active_session_count: 2,
                sessions: vec![
                    SessionSubscriptionSnapshot {
                        session_id: "ses_a".to_string(),
                        subscribers: 1,
                    },
                    SessionSubscriptionSnapshot {
                        session_id: "ses_b".to_string(),
                        subscribers: 2,
                    },
                ],
            }
        );
    }

    #[tokio::test]
    async fn session_guard_prunes_idle_channel_without_broadcast() {
        let fanout = SessionFanout::new(8);

        {
            let _receiver = fanout.subscribe("ses_demo").await;
            let _guard = fanout.session_guard("ses_demo");
        }

        yield_now().await;
        assert!(fanout.channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn broadcasts_lifecycle_to_tenant_subscribers() {
        let fanout = LifecycleFanout::new(8);
        let mut receiver = fanout.subscribe_tenant("acme").await;

        fanout.broadcast(sample_lifecycle()).await;

        let event = receiver.recv().await.expect("event should arrive");

        assert_eq!(event, sample_lifecycle());
        assert_eq!(fanout.tenant_channels.read().await.len(), 1);
        assert!(fanout.session_channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn broadcasts_lifecycle_to_session_subscribers() {
        let fanout = LifecycleFanout::new(8);
        let mut receiver = fanout.subscribe_session("ses_demo").await;

        fanout.broadcast(sample_lifecycle()).await;

        let event = receiver.recv().await.expect("event should arrive");

        assert_eq!(event, sample_lifecycle());
        assert!(fanout.tenant_channels.read().await.is_empty());
        assert_eq!(fanout.session_channels.read().await.len(), 1);
    }

    #[tokio::test]
    async fn ignores_lifecycle_broadcasts_without_subscribers() {
        let fanout = LifecycleFanout::new(8);

        fanout.broadcast(sample_lifecycle()).await;

        assert!(fanout.tenant_channels.read().await.is_empty());
        assert!(fanout.session_channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn prunes_idle_lifecycle_channels_after_last_receivers_disconnect() {
        let fanout = LifecycleFanout::new(8);
        let tenant = fanout.subscribe_tenant("acme").await;
        let session = fanout.subscribe_session("ses_demo").await;

        assert_eq!(fanout.tenant_channels.read().await.len(), 1);
        assert_eq!(fanout.session_channels.read().await.len(), 1);

        drop(tenant);
        drop(session);
        fanout.broadcast(sample_lifecycle()).await;

        assert!(fanout.tenant_channels.read().await.is_empty());
        assert!(fanout.session_channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn lifecycle_snapshot_reports_subscriber_counts() {
        let fanout = LifecycleFanout::new(8);
        let _tenant = fanout.subscribe_tenant("acme").await;
        let _session_a = fanout.subscribe_session("ses_a").await;
        let _session_b1 = fanout.subscribe_session("ses_b").await;
        let _session_b2 = fanout.subscribe_session("ses_b").await;

        assert_eq!(
            fanout.snapshot().await,
            LifecycleFanoutSnapshot {
                active_tenant_count: 1,
                active_session_count: 2,
                tenants: vec![TenantSubscriptionSnapshot {
                    tenant_id: "acme".to_string(),
                    subscribers: 1,
                }],
                sessions: vec![
                    SessionSubscriptionSnapshot {
                        session_id: "ses_a".to_string(),
                        subscribers: 1,
                    },
                    SessionSubscriptionSnapshot {
                        session_id: "ses_b".to_string(),
                        subscribers: 2,
                    },
                ],
            }
        );
    }

    #[tokio::test]
    async fn lifecycle_guard_prunes_idle_channels_without_broadcast() {
        let fanout = LifecycleFanout::new(8);

        {
            let _tenant = fanout.subscribe_tenant("acme").await;
            let _session = fanout.subscribe_session("ses_demo").await;
            let _tenant_guard = fanout.tenant_guard("acme");
            let _session_guard = fanout.session_guard("ses_demo");
        }

        yield_now().await;
        assert!(fanout.tenant_channels.read().await.is_empty());
        assert!(fanout.session_channels.read().await.is_empty());
    }

    fn sample_event(session_id: &str) -> EventResponse {
        EventResponse {
            session_id: session_id.to_string(),
            seq: 1,
            event_type: "content".to_string(),
            payload: json!({"text": "hello"})
                .as_object()
                .cloned()
                .expect("object"),
            actor: "user:user-42".to_string(),
            source: None,
            metadata: Default::default(),
            refs: Default::default(),
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq: 1,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-11T00:00:00.000000Z".to_string(),
            cursor: 1,
        }
    }

    fn sample_lifecycle() -> LifecycleResponse {
        LifecycleResponse {
            cursor: 1,
            inserted_at: "2026-04-11T00:00:00.000000Z".to_string(),
            event: LifecycleEvent::created(
                "acme".to_string(),
                &SessionResponse {
                    id: "ses_demo".to_string(),
                    title: Some("Draft".to_string()),
                    creator_principal: None,
                    metadata: json!({"workflow": "contract"})
                        .as_object()
                        .cloned()
                        .expect("object"),
                    last_seq: 0,
                    created_at: "2026-04-11T00:00:00.000000Z".to_string(),
                    updated_at: "2026-04-11T00:00:00.000000Z".to_string(),
                    version: 1,
                    archived: false,
                },
            ),
        }
    }
}
