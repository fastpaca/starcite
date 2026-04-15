use std::{collections::HashMap, time::Instant};

use axum::http::HeaderMap;

use crate::{
    AppState, auth,
    error::AppError,
    telemetry::{
        AuthOutcome, AuthSource, AuthStage, IngestOperation, IngestOutcome, RequestOperation,
        RequestOutcome, RequestPhase,
    },
};

pub(crate) async fn authenticate_http(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<auth::AuthContext, AppError> {
    let started_at = Instant::now();
    let result = auth::authenticate_http(headers, &state.auth).await;
    record_auth_result(&state.telemetry, state.auth.mode(), started_at, &result);
    result
}

pub(crate) async fn authenticate_socket(
    state: &AppState,
    params: &HashMap<String, String>,
) -> Result<auth::AuthContext, AppError> {
    let started_at = Instant::now();
    let result = auth::authenticate_socket(params, &state.auth).await;
    record_auth_result(&state.telemetry, state.auth.mode(), started_at, &result);
    result
}

pub(crate) async fn authenticate_raw_socket(
    state: &AppState,
    headers: &HeaderMap,
    params: &HashMap<String, String>,
) -> Result<auth::AuthContext, AppError> {
    let started_at = Instant::now();
    let result = auth::authenticate_raw_socket(headers, params, &state.auth).await;
    record_auth_result(&state.telemetry, state.auth.mode(), started_at, &result);
    result
}

pub(crate) fn record_ingest_result<T>(
    state: &AppState,
    operation: IngestOperation,
    tenant_id: &str,
    result: &Result<T, AppError>,
) {
    match result {
        Ok(_) => {
            state
                .telemetry
                .record_ingest_edge(operation, tenant_id, IngestOutcome::Ok, "none")
        }
        Err(error) => state.telemetry.record_ingest_edge(
            operation,
            tenant_id,
            IngestOutcome::Error,
            error.error_code(),
        ),
    }
}

pub(crate) fn record_request_result<T>(
    state: &AppState,
    phase: RequestPhase,
    started_at: Instant,
    result: Result<T, &AppError>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(_) => state.telemetry.record_request(
            RequestOperation::AppendEvent,
            phase,
            RequestOutcome::Ok,
            duration_ms,
            "none",
        ),
        Err(error) => state.telemetry.record_request(
            RequestOperation::AppendEvent,
            phase,
            request_outcome(error),
            duration_ms,
            error.error_code(),
        ),
    }
}

fn record_auth_result(
    telemetry: &crate::telemetry::Telemetry,
    auth_mode: crate::config::AuthMode,
    started_at: Instant,
    result: &Result<auth::AuthContext, AppError>,
) {
    let duration_ms = elapsed_ms(started_at);
    match result {
        Ok(_) => telemetry.record_auth(
            AuthStage::Plug,
            auth_mode,
            AuthOutcome::Ok,
            duration_ms,
            "none",
            AuthSource::None,
        ),
        Err(error) => telemetry.record_auth(
            AuthStage::Plug,
            auth_mode,
            AuthOutcome::Error,
            duration_ms,
            error.error_code(),
            AuthSource::None,
        ),
    }
}

fn request_outcome(_error: &AppError) -> RequestOutcome {
    RequestOutcome::Error
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis() as u64
}
