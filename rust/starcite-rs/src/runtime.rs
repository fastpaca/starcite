use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

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
    instance_id: Arc<str>,
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
    pub tenant_id: String,
    pub generation: u64,
    pub last_touch_reason: RuntimeTouchReason,
    pub idle_expires_in_ms: u64,
}

#[derive(Debug, Clone)]
struct ActiveSession {
    tenant_id: String,
    generation: u64,
    last_touch_at: Instant,
    last_touch_reason: RuntimeTouchReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Activation {
    Resumed,
    AlreadyActive,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTouchReason {
    Create,
    HttpRead,
    HttpWrite,
    HttpLifecycle,
    RawTail,
    RawLifecycle,
    PhoenixTail,
    PhoenixLifecycle,
}

impl SessionRuntime {
    pub fn new(
        pool: Option<PgPool>,
        lifecycle: LifecycleFanout,
        telemetry: Telemetry,
        instance_id: Arc<str>,
        idle_timeout: Duration,
    ) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            pool,
            lifecycle,
            telemetry,
            instance_id,
            idle_timeout,
        }
    }

    pub async fn session_created(&self, session_id: &str, tenant_id: &str) {
        let generation = self
            .mark_active(session_id, tenant_id, RuntimeTouchReason::Create)
            .await;
        self.schedule_freeze(session_id.to_string(), tenant_id.to_string(), generation);
        self.emit(LifecycleEvent::activated(
            session_id.to_string(),
            tenant_id.to_string(),
        ))
        .await;
        self.telemetry.record_session_create(tenant_id);
    }

    pub async fn touch_existing(
        &self,
        session_id: &str,
        tenant_id: &str,
        reason: RuntimeTouchReason,
    ) {
        let (generation, activation) = self.resume(session_id, tenant_id, reason).await;
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
        let now = Instant::now();
        let mut active_sessions = sessions
            .iter()
            .map(|(session_id, session)| ActiveSessionSnapshot {
                session_id: session_id.clone(),
                tenant_id: session.tenant_id.clone(),
                generation: session.generation,
                last_touch_reason: session.last_touch_reason,
                idle_expires_in_ms: idle_expires_in_ms(
                    session.last_touch_at,
                    self.idle_timeout,
                    now,
                ),
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
            Some(pool) => {
                match repository::append_lifecycle_event(pool, event, &self.instance_id).await {
                Ok(event) => {
                    self.lifecycle.broadcast(event).await;
                }
                Err(error) => {
                    tracing::error!(error = ?error, "failed to persist runtime lifecycle event");
                }
                }
            }
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

    async fn mark_active(
        &self,
        session_id: &str,
        tenant_id: &str,
        reason: RuntimeTouchReason,
    ) -> u64 {
        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();
        let generation = sessions
            .get(session_id)
            .map(|session| session.generation + 1)
            .unwrap_or(1);

        sessions.insert(
            session_id.to_string(),
            ActiveSession {
                tenant_id: tenant_id.to_string(),
                generation,
                last_touch_at: now,
                last_touch_reason: reason,
            },
        );
        generation
    }

    async fn resume(
        &self,
        session_id: &str,
        tenant_id: &str,
        reason: RuntimeTouchReason,
    ) -> (u64, Activation) {
        let mut sessions = self.sessions.lock().await;
        let now = Instant::now();

        match sessions.get_mut(session_id) {
            Some(session) => {
                session.generation += 1;
                session.tenant_id = tenant_id.to_string();
                session.last_touch_at = now;
                session.last_touch_reason = reason;
                (session.generation, Activation::AlreadyActive)
            }
            None => {
                sessions.insert(
                    session_id.to_string(),
                    ActiveSession {
                        tenant_id: tenant_id.to_string(),
                        generation: 1,
                        last_touch_at: now,
                        last_touch_reason: reason,
                    },
                );
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

fn idle_expires_in_ms(last_touch_at: Instant, idle_timeout: Duration, now: Instant) -> u64 {
    let Some(deadline) = last_touch_at.checked_add(idle_timeout) else {
        return idle_timeout.as_millis().min(u64::MAX as u128) as u64;
    };

    deadline
        .saturating_duration_since(now)
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::time::timeout;

    use super::{RuntimeTouchReason, SessionRuntime};
    use crate::{fanout::LifecycleFanout, model::LifecycleEvent, telemetry::Telemetry};

    #[tokio::test]
    async fn emits_activation_freeze_and_hydration_lifecycle() {
        let lifecycle = LifecycleFanout::new(16);
        let runtime = SessionRuntime::new(
            None,
            lifecycle.clone(),
            Telemetry::default(),
            Arc::from("test-runtime"),
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

        runtime
            .touch_existing("ses_demo", "acme", RuntimeTouchReason::HttpRead)
            .await;

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
            Arc::from("test-runtime"),
            Duration::from_secs(30),
        );

        runtime.session_created("ses_b", "acme").await;
        runtime.session_created("ses_a", "beta").await;
        runtime
            .touch_existing("ses_b", "acme", RuntimeTouchReason::HttpWrite)
            .await;

        let snapshot = runtime.snapshot().await;

        assert_eq!(snapshot.idle_timeout_ms, 30_000);
        assert_eq!(snapshot.active_session_count, 2);
        assert_eq!(snapshot.sessions.len(), 2);

        let first = &snapshot.sessions[0];
        assert_eq!(first.session_id, "ses_a");
        assert_eq!(first.tenant_id, "beta");
        assert_eq!(first.generation, 1);
        assert_eq!(first.last_touch_reason, RuntimeTouchReason::Create);
        assert!(first.idle_expires_in_ms > 0);
        assert!(first.idle_expires_in_ms <= 30_000);

        let second = &snapshot.sessions[1];
        assert_eq!(second.session_id, "ses_b");
        assert_eq!(second.tenant_id, "acme");
        assert_eq!(second.generation, 2);
        assert_eq!(second.last_touch_reason, RuntimeTouchReason::HttpWrite);
        assert!(second.idle_expires_in_ms > 0);
        assert!(second.idle_expires_in_ms <= 30_000);
    }
}
