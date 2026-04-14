use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgListener};
use tokio::time::sleep;

use crate::{
    data_plane,
    data_plane::{ArchiveQueue, HotEventStore, HotSessionStore},
    runtime::{LifecycleFanout, SessionFanout},
};

pub const EVENT_NOTIFICATION_CHANNEL: &str = "starcite_event_fanout";
pub const LIFECYCLE_NOTIFICATION_CHANNEL: &str = "starcite_lifecycle_fanout";
pub const ARCHIVE_NOTIFICATION_CHANNEL: &str = "starcite_archive_progress";

#[derive(Clone)]
struct ListenerState {
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    hot_store: HotEventStore,
    archive_queue: ArchiveQueue,
    session_store: HotSessionStore,
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

pub fn spawn(
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    hot_store: HotEventStore,
    archive_queue: ArchiveQueue,
    session_store: HotSessionStore,
    instance_id: Arc<str>,
) {
    let state = ListenerState {
        pool,
        fanout,
        lifecycle,
        hot_store,
        archive_queue,
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
                Ok(Some(event)) => {
                    state.hot_store.put_event(event.clone()).await;
                    state.archive_queue.enqueue(&event.session_id).await;
                    state
                        .session_store
                        .bump_last_seq(&event.session_id, &event.tenant_id, event.seq)
                        .await;
                    state
                        .session_store
                        .bump_producer_seq(
                            &event.session_id,
                            &event.tenant_id,
                            &event.producer_id,
                            event.producer_seq,
                        )
                        .await;
                    state.fanout.broadcast(event).await;
                }
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

#[cfg(test)]
mod tests {
    use super::{
        ArchiveNotification, EventNotification, LifecycleNotification, should_ignore_emitter,
    };

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
}
