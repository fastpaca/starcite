use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};

use crate::{
    AppState, api, auth, data_plane,
    data_plane::repository,
    error::AppError,
    model::{CreateSessionRequest, LifecycleEvent, SessionResponse, UpdateSessionRequest},
    runtime::RuntimeTouchReason,
    telemetry::IngestOperation,
};

pub async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Result<Json<CreateSessionRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let mut tenant_id = "unknown".to_string();
    let result: Result<(StatusCode, Json<crate::model::SessionResponse>), AppError> = async {
        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        let validated = auth::validate_create_request(request, &auth)?;
        tenant_id = validated.tenant_id.clone();
        let session = repository::create_session(&state.pool, validated.clone()).await?;
        cache_session(&state, &validated.tenant_id, &session, Some(0)).await;

        state
            .runtime
            .session_created(&session.id, &validated.tenant_id)
            .await;
        crate::runtime::publish_catalog_lifecycle(
            &state.pool,
            &state.session_store,
            &state.lifecycle,
            &state.instance_id,
            LifecycleEvent::created(validated.tenant_id, &session),
        )
        .await;

        Ok((StatusCode::CREATED, Json(session)))
    }
    .await;

    api::request_metrics::record_ingest_result(
        &state,
        IngestOperation::CreateSession,
        &tenant_id,
        &result,
    );
    result
}

pub async fn list_sessions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<crate::model::SessionsPage>, AppError> {
    let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
    let options = auth::apply_list_scope(&auth, api::query_options::parse_list_options(params)?)?;
    let page = repository::list_sessions(&state.pool, options).await?;
    Ok(Json(page))
}

pub async fn show_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
    let tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
    auth::allow_read_session(&auth, &session_id, &tenant_id)?;
    let session =
        data_plane::session_store::resolve_session(&state.session_store, &state.pool, &session_id)
            .await?;

    touch_existing_session(
        &state,
        &session_id,
        &tenant_id,
        RuntimeTouchReason::HttpRead,
    )
    .await;

    Ok(Json(session))
}

pub async fn update_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
    body: Result<Json<UpdateSessionRequest>, JsonRejection>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    api::request_validation::validate_session_id(&session_id)?;
    let mut tenant_id = "unknown".to_string();
    let result: Result<Json<crate::model::SessionResponse>, AppError> = async {
        let auth = api::request_metrics::authenticate_http(&state, &headers).await?;
        let Json(request) = body.map_err(|_| AppError::InvalidSession)?;
        tenant_id = resolve_session_tenant_id(&state, &session_id).await?;
        auth::allow_manage_session(&auth, &session_id, &tenant_id)?;
        touch_existing_session(
            &state,
            &session_id,
            &tenant_id,
            RuntimeTouchReason::HttpWrite,
        )
        .await;

        let session =
            repository::update_session(&state.pool, &session_id, request.validate()?).await?;
        cache_session(&state, &tenant_id, &session, None).await;

        crate::runtime::publish_catalog_lifecycle(
            &state.pool,
            &state.session_store,
            &state.lifecycle,
            &state.instance_id,
            LifecycleEvent::updated(tenant_id.clone(), &session),
        )
        .await;

        Ok(Json(session))
    }
    .await;

    api::request_metrics::record_ingest_result(
        &state,
        IngestOperation::UpdateSession,
        &tenant_id,
        &result,
    );
    result
}

pub async fn archive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    change_archive_state(&state, &headers, &session_id, true).await
}

pub async fn unarchive_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<crate::model::SessionResponse>, AppError> {
    change_archive_state(&state, &headers, &session_id, false).await
}

async fn resolve_session_tenant_id(state: &AppState, session_id: &str) -> Result<String, AppError> {
    data_plane::session_store::resolve_session_tenant_id(
        &state.session_store,
        &state.pool,
        session_id,
    )
    .await
}

async fn touch_existing_session(
    state: &AppState,
    session_id: &str,
    tenant_id: &str,
    reason: RuntimeTouchReason,
) {
    state
        .runtime
        .touch_existing(session_id, tenant_id, reason)
        .await;
}

async fn cache_session(
    state: &AppState,
    tenant_id: &str,
    session: &SessionResponse,
    archived_seq: Option<i64>,
) {
    state
        .session_store
        .put_session(tenant_id, session.clone(), archived_seq)
        .await;
}

async fn change_archive_state(
    state: &AppState,
    headers: &HeaderMap,
    session_id: &str,
    archived: bool,
) -> Result<Json<SessionResponse>, AppError> {
    api::request_validation::validate_session_id(session_id)?;

    let auth = api::request_metrics::authenticate_http(state, headers).await?;
    let tenant_id = resolve_session_tenant_id(state, session_id).await?;
    auth::allow_manage_session(&auth, session_id, &tenant_id)?;
    touch_existing_session(state, session_id, &tenant_id, RuntimeTouchReason::HttpWrite).await;

    let outcome = repository::set_archive_state(&state.pool, session_id, archived).await?;
    cache_session(state, &outcome.tenant_id, &outcome.session, None).await;
    publish_archive_lifecycle(state, archived, &outcome).await;

    Ok(Json(outcome.session))
}

async fn publish_archive_lifecycle(
    state: &AppState,
    archived: bool,
    outcome: &repository::ArchiveStateOutcome,
) {
    if !outcome.changed {
        return;
    }

    let event = if archived {
        LifecycleEvent::archived(outcome.tenant_id.clone(), &outcome.session)
    } else {
        LifecycleEvent::unarchived(outcome.tenant_id.clone(), &outcome.session)
    };

    crate::runtime::publish_catalog_lifecycle(
        &state.pool,
        &state.session_store,
        &state.lifecycle,
        &state.instance_id,
        event,
    )
    .await;
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{
        extract::{Path, State},
        http::HeaderMap,
    };
    use sqlx::postgres::PgPoolOptions;

    use super::show_session;
    use crate::{
        AppState,
        auth::AuthService,
        cluster::{ControlPlaneState, OwnerProxy, OwnershipManager, ReplicationCoordinator},
        config::{AuthMode, Config},
        data_plane::{ArchiveQueue, HotEventStore, HotSessionStore, PendingFlushQueue},
        error::AppError,
        model::SessionResponse,
        runtime::{
            LifecycleFanout, OpsState, RuntimeTouchReason, SessionFanout, SessionManager,
            SessionManagerDeps, SessionRuntime,
        },
        telemetry::Telemetry,
    };

    fn test_state() -> AppState {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/starcite_test")
            .expect("lazy pool");
        let fanout = SessionFanout::default();
        let lifecycle = LifecycleFanout::default();
        let hot_store = HotEventStore::new();
        let archive_queue = ArchiveQueue::new();
        let pending_flush = PendingFlushQueue::new();
        let session_store = HotSessionStore::new();
        let telemetry = Telemetry::new(true);
        let ops = OpsState::new(30_000);
        let instance_id = Arc::<str>::from("node-a");
        let ownership =
            OwnershipManager::new(pool.clone(), instance_id.clone(), Duration::from_secs(5));
        let control_plane = ControlPlaneState::new(None, None, Duration::from_secs(5));
        let owner_proxy = OwnerProxy::new(Duration::from_millis(100), None);
        let replication =
            ReplicationCoordinator::new(instance_id.clone(), false, Duration::from_millis(100))
                .expect("replication");
        let session_manager = SessionManager::new(SessionManagerDeps {
            pool: pool.clone(),
            fanout: fanout.clone(),
            hot_store: hot_store.clone(),
            pending_flush: pending_flush.clone(),
            session_store: session_store.clone(),
            ownership: ownership.clone(),
            replication: replication.clone(),
            ops: ops.clone(),
            telemetry: telemetry.clone(),
            idle_timeout: Duration::from_secs(30),
        });
        let runtime = SessionRuntime::new(
            None,
            lifecycle.clone(),
            telemetry.clone(),
            instance_id.clone(),
            Duration::from_secs(30),
        );
        let auth = AuthService::new(&test_config()).expect("auth");

        AppState {
            pool,
            fanout,
            lifecycle,
            hot_store,
            archive_queue,
            pending_flush,
            session_store,
            session_manager,
            ownership,
            control_plane,
            owner_proxy,
            replication,
            runtime,
            ops,
            auth_mode: AuthMode::None,
            auth,
            telemetry,
            instance_id,
        }
    }

    fn test_config() -> Config {
        Config {
            listen_addr: "127.0.0.1:4001".parse().expect("listen addr"),
            ops_listen_addr: "127.0.0.1:4101".parse().expect("ops listen addr"),
            database_url: "postgres://postgres:postgres@localhost/starcite_test".to_string(),
            max_connections: 1,
            archive_flush_interval_ms: 5_000,
            migrate_on_boot: false,
            auth_mode: AuthMode::None,
            auth_issuer: None,
            auth_audience: None,
            auth_jwks_url: None,
            auth_jwt_leeway_seconds: 1,
            auth_jwks_refresh_ms: 60_000,
            auth_jwks_hard_expiry_ms: 60_000,
            telemetry_enabled: true,
            shutdown_drain_timeout_ms: 30_000,
            session_runtime_idle_timeout_ms: 30_000,
            commit_flush_interval_ms: 100,
            local_async_lease_ttl_ms: 5_000,
            local_async_node_public_url: None,
            local_async_node_ops_url: None,
            local_async_node_ttl_ms: 2_000,
            local_async_owner_proxy_timeout_ms: 100,
            local_async_replication_timeout_ms: 100,
        }
    }

    fn sample_session(session_id: &str, archived: bool) -> SessionResponse {
        SessionResponse {
            id: session_id.to_string(),
            title: Some("Draft".to_string()),
            creator_principal: None,
            metadata: serde_json::Map::new(),
            last_seq: 3,
            created_at: "2026-04-13T00:00:00.000000Z".to_string(),
            updated_at: "2026-04-13T00:00:00.000000Z".to_string(),
            version: 2,
            archived,
        }
    }

    #[tokio::test]
    async fn show_session_keeps_archived_sessions_readable_by_id() {
        let state = test_state();
        state
            .session_store
            .put_session("acme", sample_session("ses_archived", true), Some(0))
            .await;

        let axum::Json(session) = show_session(
            State(state.clone()),
            HeaderMap::new(),
            Path("ses_archived".to_string()),
        )
        .await
        .expect("archived session should stay readable");

        assert_eq!(session.id, "ses_archived");
        assert!(session.archived);

        let runtime = state.runtime.snapshot().await;
        assert_eq!(runtime.active_session_count, 1);
        assert_eq!(runtime.sessions[0].session_id, "ses_archived");
        assert_eq!(
            runtime.sessions[0].last_touch_reason,
            RuntimeTouchReason::HttpRead
        );
    }

    #[tokio::test]
    async fn show_session_rejects_empty_session_id() {
        let error = show_session(State(test_state()), HeaderMap::new(), Path(String::new()))
            .await
            .expect_err("empty session ids must fail loudly");

        assert!(matches!(error, AppError::InvalidSessionId));
    }
}
