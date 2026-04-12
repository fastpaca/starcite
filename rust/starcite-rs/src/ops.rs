use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use serde::Serialize;

#[derive(Debug, Clone)]
pub struct OpsState {
    draining: Arc<AtomicBool>,
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
        Self {
            draining: Arc::new(AtomicBool::new(false)),
            shutdown_drain_timeout_ms,
        }
    }

    pub fn begin_shutdown_drain(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
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
}
