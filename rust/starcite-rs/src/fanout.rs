use std::{collections::HashMap, sync::Arc};

use tokio::sync::{RwLock, broadcast};

use crate::model::{EventResponse, LifecycleResponse};

#[derive(Debug, Clone)]
pub struct SessionFanout {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<EventResponse>>>>,
    capacity: usize,
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{LifecycleFanout, SessionFanout};
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
