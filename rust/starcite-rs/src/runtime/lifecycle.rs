use std::sync::Arc;

use sqlx::PgPool;

use crate::{
    data_plane::{self, HotSessionStore},
    model::LifecycleEvent,
};

use super::LifecycleFanout;

pub(crate) async fn publish_lifecycle(
    pool: &PgPool,
    lifecycle: &LifecycleFanout,
    instance_id: &Arc<str>,
    event: LifecycleEvent,
) {
    publish_inner(pool, lifecycle, instance_id, event, None).await;
}

pub(crate) async fn publish_catalog_lifecycle(
    pool: &PgPool,
    session_store: &HotSessionStore,
    lifecycle: &LifecycleFanout,
    instance_id: &Arc<str>,
    event: LifecycleEvent,
) {
    publish_inner(pool, lifecycle, instance_id, event, Some(session_store)).await;
}

async fn publish_inner(
    pool: &PgPool,
    lifecycle: &LifecycleFanout,
    instance_id: &Arc<str>,
    event: LifecycleEvent,
    session_store: Option<&HotSessionStore>,
) {
    match data_plane::repository::append_lifecycle_event(pool, event, instance_id).await {
        Ok(event) => {
            if let Some(session_store) = session_store {
                session_store.apply_lifecycle_hint(&event.event).await;
            }

            lifecycle.broadcast(event).await;
        }
        Err(error) => {
            tracing::error!(error = ?error, "failed to persist lifecycle event");
        }
    }
}
