use std::{sync::Arc, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgListener};
use tokio::{sync::mpsc, time::sleep};

use crate::{
    data_plane,
    data_plane::{HotEventStore, HotSessionStore},
    model::EventResponse,
    runtime::{LifecycleFanout, SessionFanout},
};

pub const EVENT_NOTIFICATION_CHANNEL: &str = "starcite_event_fanout";
pub const LIFECYCLE_NOTIFICATION_CHANNEL: &str = "starcite_lifecycle_fanout";
pub const ARCHIVE_NOTIFICATION_CHANNEL: &str = "starcite_archive_progress";
const DIRECT_EVENT_RELAY_PATH: &str = "/internal/fanout/event";
const DIRECT_EVENT_RELAY_QUEUE_CAPACITY: usize = 4_096;

#[derive(Clone)]
struct ListenerState {
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    hot_store: HotEventStore,
    session_store: HotSessionStore,
    instance_id: Arc<str>,
}

#[derive(Debug, Clone)]
pub struct DirectEventRelay {
    sender: Option<mpsc::Sender<EventResponse>>,
}

#[derive(Clone)]
struct DirectEventRelayState {
    pool: PgPool,
    client: Client,
    instance_id: Arc<str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventNotification {
    pub emitter_id: String,
    pub session_id: String,
    pub seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LifecycleNotification {
    pub emitter_id: String,
    pub cursor: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveNotification {
    pub emitter_id: String,
    pub session_id: String,
    pub tenant_id: String,
    pub archived_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DirectEventRelayRequest {
    pub emitter_id: String,
    pub event: EventResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectEventRelayAck {
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventRelayDisposition {
    Applied,
    Duplicate,
    Archived,
    Gap { expected_seq: i64 },
}

impl DirectEventRelay {
    pub fn new(
        pool: PgPool,
        instance_id: Arc<str>,
        enabled: bool,
        timeout: Duration,
    ) -> Result<Self, String> {
        if !enabled {
            return Ok(Self::disabled());
        }

        let client = Client::builder()
            .pool_max_idle_per_host(8)
            .timeout(timeout)
            .build()
            .map_err(|error| format!("failed to build direct event relay client: {error}"))?;
        let (sender, receiver) = mpsc::channel(DIRECT_EVENT_RELAY_QUEUE_CAPACITY);
        let state = DirectEventRelayState {
            pool,
            client,
            instance_id,
        };

        tokio::spawn(async move {
            run_direct_event_relay(state, receiver).await;
        });

        Ok(Self {
            sender: Some(sender),
        })
    }

    pub fn disabled() -> Self {
        Self { sender: None }
    }

    pub fn enqueue(&self, event: EventResponse) {
        let Some(sender) = &self.sender else {
            return;
        };

        match sender.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(event)) => {
                tracing::warn!(
                    session_id = event.session_id,
                    seq = event.seq,
                    "dropping direct event relay because queue is full"
                );
            }
            Err(mpsc::error::TrySendError::Closed(event)) => {
                tracing::warn!(
                    session_id = event.session_id,
                    seq = event.seq,
                    "dropping direct event relay because worker is closed"
                );
            }
        }
    }
}

pub fn spawn(
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    hot_store: HotEventStore,
    session_store: HotSessionStore,
    instance_id: Arc<str>,
) {
    let state = ListenerState {
        pool,
        fanout,
        lifecycle,
        hot_store,
        session_store,
        instance_id,
    };

    tokio::spawn(async move {
        run_listener_loop(state).await;
    });
}

async fn run_listener_loop(state: ListenerState) {
    loop {
        let mut listener = match PgListener::connect_with(&state.pool).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(error = ?error, "failed to connect postgres relay listener");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if let Err(error) = listen_all(
            &mut listener,
            [
                EVENT_NOTIFICATION_CHANNEL,
                LIFECYCLE_NOTIFICATION_CHANNEL,
                ARCHIVE_NOTIFICATION_CHANNEL,
            ],
        )
        .await
        {
            tracing::error!(error = ?error, "failed to subscribe postgres relay channels");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        loop {
            match listener.recv().await {
                Ok(notification) => {
                    handle_notification(&state, notification.channel(), notification.payload())
                        .await;
                }
                Err(error) => {
                    tracing::error!(error = ?error, "postgres relay listener disconnected");
                    break;
                }
            }
        }

        sleep(Duration::from_secs(1)).await;
    }
}

async fn handle_notification(state: &ListenerState, channel: &str, payload: &str) {
    match channel {
        EVENT_NOTIFICATION_CHANNEL => {
            let payload = match serde_json::from_str::<EventNotification>(payload) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error = ?error, payload, "failed to decode event relay payload");
                    return;
                }
            };

            if should_ignore_emitter(state.instance_id.as_ref(), &payload.emitter_id) {
                return;
            }

            match data_plane::repository::load_event_by_seq(
                &state.pool,
                &payload.session_id,
                payload.seq,
            )
            .await
            {
                Ok(Some(event)) => match apply_relayed_event(
                    &state.hot_store,
                    &state.session_store,
                    &state.fanout,
                    event,
                )
                .await
                {
                    EventRelayDisposition::Applied | EventRelayDisposition::Duplicate => {}
                    EventRelayDisposition::Archived => {
                        tracing::debug!(
                            session_id = payload.session_id,
                            seq = payload.seq,
                            "ignoring relayed event below archived frontier"
                        );
                    }
                    EventRelayDisposition::Gap { expected_seq } => {
                        tracing::warn!(
                            session_id = payload.session_id,
                            seq = payload.seq,
                            expected_seq,
                            "ignoring relayed event due to gap"
                        );
                    }
                },
                Ok(None) => {
                    tracing::warn!(
                        session_id = %payload.session_id,
                        seq = payload.seq,
                        "event relay referenced missing row"
                    );
                }
                Err(error) => {
                    tracing::error!(error = ?error, "failed to load relayed event");
                }
            }
        }
        LIFECYCLE_NOTIFICATION_CHANNEL => {
            let payload = match serde_json::from_str::<LifecycleNotification>(payload) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error = ?error, payload, "failed to decode lifecycle relay payload");
                    return;
                }
            };

            if should_ignore_emitter(state.instance_id.as_ref(), &payload.emitter_id) {
                return;
            }

            match data_plane::repository::load_lifecycle_by_cursor(&state.pool, payload.cursor)
                .await
            {
                Ok(Some(event)) => {
                    if let Err(error) = data_plane::session_store::refresh_from_lifecycle(
                        &state.session_store,
                        &state.pool,
                        &event,
                    )
                    .await
                    {
                        tracing::warn!(error = ?error, cursor = event.cursor, "failed to refresh session cache from lifecycle relay");
                    }
                    state.lifecycle.broadcast(event).await;
                }
                Ok(None) => {
                    tracing::warn!(
                        cursor = payload.cursor,
                        "lifecycle relay referenced missing row"
                    );
                }
                Err(error) => {
                    tracing::error!(error = ?error, "failed to load relayed lifecycle event");
                }
            }
        }
        ARCHIVE_NOTIFICATION_CHANNEL => {
            let payload = match serde_json::from_str::<ArchiveNotification>(payload) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(
                        error = ?error,
                        payload,
                        "failed to decode archive relay payload"
                    );
                    return;
                }
            };

            if should_ignore_emitter(state.instance_id.as_ref(), &payload.emitter_id) {
                return;
            }

            state
                .session_store
                .put_tenant(&payload.session_id, &payload.tenant_id)
                .await;
            state
                .session_store
                .update_archived_seq(&payload.session_id, payload.archived_seq)
                .await;
            state
                .hot_store
                .delete_below(&payload.session_id, payload.archived_seq.saturating_add(1))
                .await;
        }
        _ => {}
    }
}

pub async fn apply_relayed_event(
    hot_store: &HotEventStore,
    session_store: &HotSessionStore,
    fanout: &SessionFanout,
    event: EventResponse,
) -> EventRelayDisposition {
    let archived_seq = session_store
        .get_archived_seq(&event.session_id)
        .await
        .unwrap_or(0);
    if event.seq <= archived_seq {
        return EventRelayDisposition::Archived;
    }

    let current_last_seq = session_store
        .get_last_seq(&event.session_id)
        .await
        .unwrap_or(0)
        .max(hot_store.max_seq(&event.session_id).await.unwrap_or(0));

    if event.seq <= current_last_seq {
        return EventRelayDisposition::Duplicate;
    }

    let expected_seq = current_last_seq + 1;
    if event.seq != expected_seq {
        return EventRelayDisposition::Gap { expected_seq };
    }

    if !hot_store.put_event_if_absent(event.clone()).await {
        return EventRelayDisposition::Duplicate;
    }

    session_store
        .put_tenant(&event.session_id, &event.tenant_id)
        .await;
    session_store
        .bump_last_seq(&event.session_id, &event.tenant_id, event.seq)
        .await;
    session_store
        .bump_producer_seq(
            &event.session_id,
            &event.tenant_id,
            &event.producer_id,
            event.producer_seq,
        )
        .await;
    fanout.broadcast(event).await;

    EventRelayDisposition::Applied
}

fn should_ignore_emitter(instance_id: &str, emitter_id: &str) -> bool {
    instance_id == emitter_id
}

async fn listen_all<'a, I>(listener: &mut PgListener, channels: I) -> Result<(), sqlx::Error>
where
    I: IntoIterator<Item = &'a str>,
{
    for channel in channels {
        listener.listen(channel).await?;
    }

    Ok(())
}

async fn run_direct_event_relay(
    state: DirectEventRelayState,
    mut receiver: mpsc::Receiver<EventResponse>,
) {
    while let Some(event) = receiver.recv().await {
        state.broadcast_event(event).await;
    }
}

impl DirectEventRelayState {
    async fn broadcast_event(&self, event: EventResponse) {
        let peers = match data_plane::repository::load_live_control_peers(
            &self.pool,
            self.instance_id.as_ref(),
        )
        .await
        {
            Ok(peers) => peers,
            Err(error) => {
                tracing::warn!(
                    error = ?error,
                    session_id = event.session_id,
                    seq = event.seq,
                    "failed to load direct event relay peers"
                );
                return;
            }
        };

        if peers.is_empty() {
            return;
        }

        let request = DirectEventRelayRequest {
            emitter_id: self.instance_id.to_string(),
            event,
        };

        for peer in peers {
            let url = format!(
                "{}{}",
                peer.ops_url.trim_end_matches('/'),
                DIRECT_EVENT_RELAY_PATH
            );

            match self.client.post(&url).json(&request).send().await {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    tracing::warn!(
                        peer = peer.node_id,
                        url,
                        status = %response.status(),
                        session_id = request.event.session_id,
                        seq = request.event.seq,
                        "direct event relay request failed"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        error = ?error,
                        peer = peer.node_id,
                        url,
                        session_id = request.event.session_id,
                        seq = request.event.seq,
                        "direct event relay transport failed"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{
        ArchiveNotification, EventNotification, EventRelayDisposition, LifecycleNotification,
        apply_relayed_event, should_ignore_emitter,
    };
    use crate::{
        data_plane::{HotEventStore, HotSessionStore},
        model::EventResponse,
        runtime::SessionFanout,
    };
    use serde_json::Map;

    fn sample_event(seq: i64) -> EventResponse {
        EventResponse {
            session_id: "ses_demo".to_string(),
            seq,
            event_type: "content".to_string(),
            payload: Map::new(),
            actor: "service:test".to_string(),
            source: Some("test".to_string()),
            metadata: Map::new(),
            refs: Map::new(),
            idempotency_key: None,
            producer_id: "writer-1".to_string(),
            producer_seq: seq,
            tenant_id: "acme".to_string(),
            inserted_at: "2026-04-15T00:00:00Z".to_string(),
            epoch: Some(7),
            cursor: seq,
        }
    }

    #[test]
    fn event_notification_round_trips() {
        let payload = serde_json::to_string(&EventNotification {
            emitter_id: "node-a".to_string(),
            session_id: "ses_demo".to_string(),
            seq: 4,
        })
        .expect("serialize event notification");

        let decoded: EventNotification =
            serde_json::from_str(&payload).expect("deserialize event notification");

        assert_eq!(decoded.emitter_id, "node-a");
        assert_eq!(decoded.session_id, "ses_demo");
        assert_eq!(decoded.seq, 4);
    }

    #[test]
    fn lifecycle_notification_round_trips() {
        let payload = serde_json::to_string(&LifecycleNotification {
            emitter_id: "node-a".to_string(),
            cursor: 17,
        })
        .expect("serialize lifecycle notification");

        let decoded: LifecycleNotification =
            serde_json::from_str(&payload).expect("deserialize lifecycle notification");

        assert_eq!(decoded.emitter_id, "node-a");
        assert_eq!(decoded.cursor, 17);
    }

    #[test]
    fn archive_notification_round_trips() {
        let payload = serde_json::to_string(&ArchiveNotification {
            emitter_id: "node-a".to_string(),
            session_id: "ses_demo".to_string(),
            tenant_id: "acme".to_string(),
            archived_seq: 4,
        })
        .expect("serialize archive notification");

        let decoded: ArchiveNotification =
            serde_json::from_str(&payload).expect("deserialize archive notification");

        assert_eq!(decoded.emitter_id, "node-a");
        assert_eq!(decoded.session_id, "ses_demo");
        assert_eq!(decoded.tenant_id, "acme");
        assert_eq!(decoded.archived_seq, 4);
    }

    #[test]
    fn relay_ignores_self_emitted_notifications() {
        assert!(should_ignore_emitter("node-a", "node-a"));
        assert!(!should_ignore_emitter("node-a", "node-b"));
    }

    #[tokio::test]
    async fn apply_relayed_event_broadcasts_once_and_dedupes() {
        let hot_store = HotEventStore::new();
        let session_store = HotSessionStore::new();
        let fanout = SessionFanout::default();
        let mut receiver = fanout.subscribe("ses_demo").await;

        assert_eq!(
            apply_relayed_event(&hot_store, &session_store, &fanout, sample_event(1)).await,
            EventRelayDisposition::Applied
        );

        let delivered = timeout(Duration::from_millis(50), receiver.recv())
            .await
            .expect("first relay should broadcast")
            .expect("broadcast value");
        assert_eq!(delivered.seq, 1);

        assert_eq!(
            apply_relayed_event(&hot_store, &session_store, &fanout, sample_event(1)).await,
            EventRelayDisposition::Duplicate
        );
        assert!(
            timeout(Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn apply_relayed_event_rejects_gaps() {
        let hot_store = HotEventStore::new();
        let session_store = HotSessionStore::new();
        let fanout = SessionFanout::default();

        assert_eq!(
            apply_relayed_event(&hot_store, &session_store, &fanout, sample_event(2)).await,
            EventRelayDisposition::Gap { expected_seq: 1 }
        );
    }
}
