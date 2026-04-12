use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct OpsState {
    draining: Arc<AtomicBool>,
    drain_source: Arc<AtomicU8>,
    shutdown_deadline: Arc<Mutex<Option<Instant>>>,
    draining_tx: watch::Sender<bool>,
    shutdown_drain_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpsSnapshot {
    pub mode: &'static str,
    pub draining: bool,
    pub drain_source: Option<&'static str>,
    pub retry_after_ms: Option<u64>,
    pub shutdown_drain_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainSource {
    Manual,
    Shutdown,
}

impl DrainSource {
    fn code(self) -> u8 {
        match self {
            Self::Manual => 1,
            Self::Shutdown => 2,
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Manual),
            2 => Some(Self::Shutdown),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Shutdown => "shutdown",
        }
    }
}

impl OpsState {
    pub fn new(shutdown_drain_timeout_ms: u64) -> Self {
        let (draining_tx, _) = watch::channel(false);

        Self {
            draining: Arc::new(AtomicBool::new(false)),
            drain_source: Arc::new(AtomicU8::new(0)),
            shutdown_deadline: Arc::new(Mutex::new(None)),
            draining_tx,
            shutdown_drain_timeout_ms,
        }
    }

    pub fn begin_shutdown_drain(&self) {
        self.begin_drain(DrainSource::Shutdown);
    }

    pub fn begin_manual_drain(&self) {
        self.begin_drain(DrainSource::Manual);
    }

    pub fn clear_drain(&self) {
        self.drain_source.store(0, Ordering::SeqCst);
        *self
            .shutdown_deadline
            .lock()
            .expect("shutdown deadline lock") = None;

        if self.draining.swap(false, Ordering::SeqCst) {
            let _ = self.draining_tx.send(false);
        }
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn subscribe_draining(&self) -> watch::Receiver<bool> {
        self.draining_tx.subscribe()
    }

    pub fn snapshot(&self) -> OpsSnapshot {
        let draining = self.is_draining();
        let drain_source = if draining {
            DrainSource::from_code(self.drain_source.load(Ordering::SeqCst))
        } else {
            None
        };
        let retry_after_ms = match drain_source {
            Some(DrainSource::Shutdown) => self
                .shutdown_deadline
                .lock()
                .expect("shutdown deadline lock")
                .as_ref()
                .map(|deadline| {
                    deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis() as u64
                }),
            _ => None,
        };

        OpsSnapshot {
            mode: if draining { "draining" } else { "ready" },
            draining,
            drain_source: drain_source.map(DrainSource::as_str),
            retry_after_ms,
            shutdown_drain_timeout_ms: self.shutdown_drain_timeout_ms,
        }
    }

    fn begin_drain(&self, source: DrainSource) {
        self.drain_source.store(source.code(), Ordering::SeqCst);
        *self
            .shutdown_deadline
            .lock()
            .expect("shutdown deadline lock") = match source {
            DrainSource::Manual => None,
            DrainSource::Shutdown => {
                Some(Instant::now() + drain_timeout(self.shutdown_drain_timeout_ms))
            }
        };

        if !self.draining.swap(true, Ordering::SeqCst) {
            let _ = self.draining_tx.send(true);
        }
    }
}

fn drain_timeout(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms)
}

#[cfg(test)]
mod tests {
    use super::{OpsSnapshot, OpsState};

    #[test]
    fn snapshot_starts_ready() {
        let ops = OpsState::new(30_000);

        assert_eq!(
            ops.snapshot(),
            OpsSnapshot {
                mode: "ready",
                draining: false,
                drain_source: None,
                retry_after_ms: None,
                shutdown_drain_timeout_ms: 30_000,
            }
        );
    }

    #[test]
    fn shutdown_drain_flips_snapshot() {
        let ops = OpsState::new(5_000);
        ops.begin_shutdown_drain();

        assert_eq!(ops.snapshot().drain_source, Some("shutdown"));
        assert!(
            ops.snapshot()
                .retry_after_ms
                .is_some_and(|retry_after_ms| retry_after_ms <= 5_000)
        );
    }

    #[test]
    fn manual_drain_sets_manual_source() {
        let ops = OpsState::new(5_000);
        ops.begin_manual_drain();

        assert_eq!(
            ops.snapshot(),
            OpsSnapshot {
                mode: "draining",
                draining: true,
                drain_source: Some("manual"),
                retry_after_ms: None,
                shutdown_drain_timeout_ms: 5_000,
            }
        );
    }

    #[test]
    fn clearing_manual_drain_restores_ready_snapshot() {
        let ops = OpsState::new(5_000);
        ops.begin_manual_drain();
        ops.clear_drain();

        assert_eq!(
            ops.snapshot(),
            OpsSnapshot {
                mode: "ready",
                draining: false,
                drain_source: None,
                retry_after_ms: None,
                shutdown_drain_timeout_ms: 5_000,
            }
        );
    }

    #[tokio::test]
    async fn draining_subscribers_observe_shutdown_drain() {
        let ops = OpsState::new(5_000);
        let mut draining = ops.subscribe_draining();

        assert!(!*draining.borrow());

        ops.begin_shutdown_drain();
        draining.changed().await.expect("drain change");

        assert!(*draining.borrow());
    }
}
