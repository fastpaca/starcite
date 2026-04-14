use std::collections::HashMap;

use super::{
    query_options::{
        LifecycleOptions, parse_events_options, parse_lifecycle_options, parse_optional_session_id,
    },
    request_validation::validate_session_id,
};
use crate::{
    AppState, auth, data_plane::session_store::resolve_session_tenant_id, error::AppError,
};

pub(crate) async fn resolve_lifecycle_options(
    state: &AppState,
    auth: &auth::AuthContext,
    params: &HashMap<String, String>,
) -> Result<LifecycleOptions, AppError> {
    let mut lifecycle = match auth.kind {
        crate::config::AuthMode::None => parse_lifecycle_options(params.clone())?,
        crate::config::AuthMode::Jwt => {
            auth::can_subscribe_lifecycle(auth)?;
            LifecycleOptions {
                tenant_id: auth.principal.tenant_id.clone(),
                cursor: parse_events_options(params.clone())?.cursor,
                session_id: parse_optional_session_id(params)?,
            }
        }
    };

    validate_lifecycle_scope(state, auth, &mut lifecycle).await?;
    Ok(lifecycle)
}

pub(crate) async fn resolve_session_lifecycle(
    state: &AppState,
    auth: &auth::AuthContext,
    session_id: &str,
    cursor: i64,
) -> Result<LifecycleOptions, AppError> {
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, session_id).await?;
    auth::allow_read_session(auth, session_id, &tenant_id)?;

    Ok(LifecycleOptions {
        tenant_id,
        cursor,
        session_id: Some(session_id.to_string()),
    })
}

async fn validate_lifecycle_scope(
    state: &AppState,
    auth: &auth::AuthContext,
    lifecycle: &mut LifecycleOptions,
) -> Result<(), AppError> {
    let Some(session_id) = lifecycle.session_id.as_ref() else {
        return Ok(());
    };

    validate_session_id(session_id)?;
    let tenant_id =
        resolve_session_tenant_id(&state.session_store, &state.pool, session_id).await?;

    if tenant_id != lifecycle.tenant_id {
        return Err(AppError::ForbiddenTenant);
    }

    auth::allow_read_session(auth, session_id, &tenant_id)?;
    Ok(())
}
