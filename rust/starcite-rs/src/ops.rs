use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde::Serialize;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct OpsState {
    draining: Arc<AtomicBool>,
    draining_tx: watch::Sender<bool>,
    shutdown_drain_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct OpsSnapshot {
    pub mode: &'static str,
    pub draining: bool,
    pub drain_source: Option<&'static str>,
    pub shutdown_drain_timeout_ms: u64,
}

impl OpsState {
    pub fn new(shutdown_drain_timeout_ms: u64) -> Self {
        let (draining_tx, _) = watch::channel(false);

        Self {
            draining: Arc::new(AtomicBool::new(false)),
            draining_tx,
            shutdown_drain_timeout_ms,
        }
    }

    pub fn begin_shutdown_drain(&self) {
        if self
            .draining
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let _ = self.draining_tx.send(true);
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

        OpsSnapshot {
            mode: if draining { "draining" } else { "ready" },
            draining,
            drain_source: draining.then_some("shutdown"),
            shutdown_drain_timeout_ms: self.shutdown_drain_timeout_ms,
        }
    }
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
                shutdown_drain_timeout_ms: 30_000,
            }
        );
    }

    #[test]
    fn shutdown_drain_flips_snapshot() {
        let ops = OpsState::new(5_000);
        ops.begin_shutdown_drain();

        assert_eq!(
            ops.snapshot(),
            OpsSnapshot {
                mode: "draining",
                draining: true,
                drain_source: Some("shutdown"),
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
