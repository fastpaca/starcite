use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgListener};
use tokio::time::sleep;

use crate::{
    fanout::{LifecycleFanout, SessionFanout},
    repository,
};

pub const EVENT_NOTIFICATION_CHANNEL: &str = "starcite_event_fanout";
pub const LIFECYCLE_NOTIFICATION_CHANNEL: &str = "starcite_lifecycle_fanout";

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

pub fn spawn(
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    instance_id: Arc<str>,
) {
    tokio::spawn(async move {
        run_listener_loop(pool, fanout, lifecycle, instance_id).await;
    });
}

async fn run_listener_loop(
    pool: PgPool,
    fanout: SessionFanout,
    lifecycle: LifecycleFanout,
    instance_id: Arc<str>,
) {
    loop {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(listener) => listener,
            Err(error) => {
                tracing::error!(error = ?error, "failed to connect postgres relay listener");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        if let Err(error) =
            listen_all(&mut listener, [EVENT_NOTIFICATION_CHANNEL, LIFECYCLE_NOTIFICATION_CHANNEL])
                .await
        {
            tracing::error!(error = ?error, "failed to subscribe postgres relay channels");
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        loop {
            match listener.recv().await {
                Ok(notification) => {
                    handle_notification(
                        &pool,
                        &fanout,
                        &lifecycle,
                        instance_id.as_ref(),
                        notification.channel(),
                        notification.payload(),
                    )
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

async fn handle_notification(
    pool: &PgPool,
    fanout: &SessionFanout,
    lifecycle: &LifecycleFanout,
    instance_id: &str,
    channel: &str,
    payload: &str,
) {
    match channel {
        EVENT_NOTIFICATION_CHANNEL => {
            let payload = match serde_json::from_str::<EventNotification>(payload) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error = ?error, payload, "failed to decode event relay payload");
                    return;
                }
            };

            if should_ignore_emitter(instance_id, &payload.emitter_id) {
                return;
            }

            match repository::load_event_by_seq(pool, &payload.session_id, payload.seq).await {
                Ok(Some(event)) => {
                    fanout.broadcast(event).await;
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

            if should_ignore_emitter(instance_id, &payload.emitter_id) {
                return;
            }

            match repository::load_lifecycle_by_cursor(pool, payload.cursor).await {
                Ok(Some(event)) => {
                    lifecycle.broadcast(event).await;
                }
                Ok(None) => {
                    tracing::warn!(cursor = payload.cursor, "lifecycle relay referenced missing row");
                }
                Err(error) => {
                    tracing::error!(error = ?error, "failed to load relayed lifecycle event");
                }
            }
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
    use super::{EventNotification, LifecycleNotification, should_ignore_emitter};

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
    fn relay_ignores_self_emitted_notifications() {
        assert!(should_ignore_emitter("node-a", "node-a"));
        assert!(!should_ignore_emitter("node-a", "node-b"));
    }
}
