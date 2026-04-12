use std::{collections::HashMap, sync::Arc, time::Duration};

use serde::Serialize;
use sqlx::PgPool;
use tokio::{sync::Mutex, time::sleep};

use crate::{
    fanout::LifecycleFanout,
    model::LifecycleEvent,
    repository,
    telemetry::{SessionOutcome, SessionReason, Telemetry},
};

#[derive(Debug, Clone)]
pub struct SessionRuntime {
    sessions: Arc<Mutex<HashMap<String, ActiveSession>>>,
    pool: Option<PgPool>,
    lifecycle: LifecycleFanout,
    telemetry: Telemetry,
    idle_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    pub idle_timeout_ms: u64,
    pub active_session_count: usize,
    pub sessions: Vec<ActiveSessionSnapshot>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ActiveSessionSnapshot {
    pub session_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone)]
struct ActiveSession {
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Activation {
    Resumed,
    AlreadyActive,
}

impl SessionRuntime {
    pub fn new(
        pool: Option<PgPool>,
        lifecycle: LifecycleFanout,
        telemetry: Telemetry,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            pool,
            lifecycle,
            telemetry,
            idle_timeout,
        }
    }

    pub async fn session_created(&self, session_id: &str, tenant_id: &str) {
        let generation = self.mark_active(session_id).await;
        self.schedule_freeze(session_id.to_string(), tenant_id.to_string(), generation);
        self.emit(LifecycleEvent::activated(
            session_id.to_string(),
            tenant_id.to_string(),
        ))
        .await;
        self.telemetry.record_session_create(tenant_id);
    }

    pub async fn touch_existing(&self, session_id: &str, tenant_id: &str) {
        let (generation, activation) = self.resume(session_id).await;
        self.schedule_freeze(session_id.to_string(), tenant_id.to_string(), generation);

        if activation == Activation::Resumed {
            self.emit(LifecycleEvent::hydrating(
                session_id.to_string(),
                tenant_id.to_string(),
            ))
            .await;
            self.emit(LifecycleEvent::activated(
                session_id.to_string(),
                tenant_id.to_string(),
            ))
            .await;
            self.telemetry.record_session_hydrate(
                tenant_id,
                SessionOutcome::Ok,
                SessionReason::Hydrate,
            );
        }
    }

    pub async fn snapshot(&self) -> RuntimeSnapshot {
        let sessions = self.sessions.lock().await;
        let mut active_sessions = sessions
            .iter()
            .map(|(session_id, session)| ActiveSessionSnapshot {
                session_id: session_id.clone(),
                generation: session.generation,
            })
            .collect::<Vec<_>>();

        active_sessions.sort_by(|left, right| left.session_id.cmp(&right.session_id));

        RuntimeSnapshot {
            idle_timeout_ms: self.idle_timeout.as_millis().min(u64::MAX as u128) as u64,
            active_session_count: active_sessions.len(),
            sessions: active_sessions,
        }
    }

    async fn emit(&self, event: LifecycleEvent) {
        match &self.pool {
            Some(pool) => match repository::append_lifecycle_event(pool, event).await {
                Ok(event) => {
                    self.lifecycle.broadcast(event).await;
                }
                Err(error) => {
                    tracing::error!(error = ?error, "failed to persist runtime lifecycle event");
                }
            },
            None => {
                self.lifecycle
                    .broadcast(crate::model::LifecycleResponse {
                        cursor: 0,
                        inserted_at: String::new(),
                        event,
                    })
                    .await;
            }
        }
    }

    async fn mark_active(&self, session_id: &str) -> u64 {
        let mut sessions = self.sessions.lock().await;
        let generation = sessions
            .get(session_id)
            .map(|session| session.generation + 1)
            .unwrap_or(1);

        sessions.insert(session_id.to_string(), ActiveSession { generation });
        generation
    }

    async fn resume(&self, session_id: &str) -> (u64, Activation) {
        let mut sessions = self.sessions.lock().await;

        match sessions.get_mut(session_id) {
            Some(session) => {
                session.generation += 1;
                (session.generation, Activation::AlreadyActive)
            }
            None => {
                sessions.insert(session_id.to_string(), ActiveSession { generation: 1 });
                (1, Activation::Resumed)
            }
        }
    }

    fn schedule_freeze(&self, session_id: String, tenant_id: String, generation: u64) {
        let runtime = self.clone();

        tokio::spawn(async move {
            sleep(runtime.idle_timeout).await;
            runtime
                .freeze_if_idle(session_id, tenant_id, generation)
                .await;
        });
    }

    async fn freeze_if_idle(&self, session_id: String, tenant_id: String, generation: u64) {
        let should_emit = {
            let mut sessions = self.sessions.lock().await;

            match sessions.get(&session_id) {
                Some(session) if session.generation == generation => {
                    sessions.remove(&session_id);
                    true
                }
                _ => false,
            }
        };

        if should_emit {
            self.emit(LifecycleEvent::freezing(
                session_id.clone(),
                tenant_id.clone(),
            ))
            .await;
            self.telemetry.record_session_freeze(
                &tenant_id,
                SessionOutcome::Ok,
                SessionReason::IdleTimeout,
            );
            self.emit(LifecycleEvent::frozen(session_id, tenant_id))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::{RuntimeSnapshot, SessionRuntime};
    use crate::{fanout::LifecycleFanout, model::LifecycleEvent, telemetry::Telemetry};

    #[tokio::test]
    async fn emits_activation_freeze_and_hydration_lifecycle() {
        let lifecycle = LifecycleFanout::new(16);
        let runtime = SessionRuntime::new(
            None,
            lifecycle.clone(),
            Telemetry::default(),
            Duration::from_millis(20),
        );
        let mut receiver = lifecycle.subscribe_tenant("acme").await;

        runtime.session_created("ses_demo", "acme").await;

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("activation")
                .expect("event")
                .event,
            LifecycleEvent::activated("ses_demo".to_string(), "acme".to_string())
        );

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("freezing")
                .expect("event")
                .event,
            LifecycleEvent::freezing("ses_demo".to_string(), "acme".to_string())
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("frozen")
                .expect("event")
                .event,
            LifecycleEvent::frozen("ses_demo".to_string(), "acme".to_string())
        );

        runtime.touch_existing("ses_demo", "acme").await;

        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("hydrating")
                .expect("event")
                .event,
            LifecycleEvent::hydrating("ses_demo".to_string(), "acme".to_string())
        );
        assert_eq!(
            timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("reactivated")
                .expect("event")
                .event,
            LifecycleEvent::activated("ses_demo".to_string(), "acme".to_string())
        );
    }

    #[tokio::test]
    async fn snapshot_reports_sorted_active_sessions() {
        let runtime = SessionRuntime::new(
            None,
            LifecycleFanout::new(16),
            Telemetry::default(),
            Duration::from_secs(30),
        );

        runtime.session_created("ses_b", "acme").await;
        runtime.session_created("ses_a", "acme").await;

        let snapshot = runtime.snapshot().await;

        assert_eq!(
            snapshot,
            RuntimeSnapshot {
                idle_timeout_ms: 30_000,
                active_session_count: 2,
                sessions: vec![
                    super::ActiveSessionSnapshot {
                        session_id: "ses_a".to_string(),
                        generation: 1,
                    },
                    super::ActiveSessionSnapshot {
                        session_id: "ses_b".to_string(),
                        generation: 1,
                    },
                ],
            }
        );
    }
}
