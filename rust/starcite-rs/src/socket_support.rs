use std::time::Instant;

use tokio::time::sleep;

use crate::{
    AppState,
    telemetry::{ReadOperation, ReadOutcome, ReadPhase},
};

pub(crate) fn record_read_result(
    state: &AppState,
    operation: ReadOperation,
    started_at: Instant,
    result: Result<(), ()>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(()) => {
            state
                .telemetry
                .record_read(operation, ReadPhase::Deliver, ReadOutcome::Ok, duration_ms)
        }
        Err(()) => state.telemetry.record_read(
            operation,
            ReadPhase::Deliver,
            ReadOutcome::Error,
            duration_ms,
        ),
    }
}

pub(crate) async fn wait_for_drain(ops: &crate::ops::OpsState) {
    if ops.is_draining() {
        return;
    }

    loop {
        sleep(std::time::Duration::from_millis(100)).await;

        if ops.is_draining() {
            return;
        }
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}
