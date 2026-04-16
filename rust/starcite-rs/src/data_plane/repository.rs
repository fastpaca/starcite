use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool, Postgres, QueryBuilder, types::Json};

use crate::{
    cluster::relay::{
        ARCHIVE_NOTIFICATION_CHANNEL, ArchiveNotification, EVENT_NOTIFICATION_CHANNEL,
        EventNotification, LIFECYCLE_NOTIFICATION_CHANNEL, LifecycleNotification,
    },
    error::AppError,
    model::{
        AppendReply, ArchivedFilter, EventResponse, EventRow, EventsOptions, EventsPage, JsonMap,
        LifecycleEvent, LifecyclePage, LifecycleResponse, LifecycleRow, ListOptions,
        SessionResponse, SessionRow, SessionsPage, ValidatedCreateSession, ValidatedUpdateSession,
        merge_metadata,
    },
};

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionLeaseRow {
    pub owner_id: String,
    pub owner_public_url: Option<String>,
    pub epoch: i64,
    pub expires_at: DateTime<Utc>,
    pub replica_peers: Json<Vec<SessionLeasePeer>>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ControlNodeRow {
    pub draining: bool,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct SessionLeaseTakeoverHint {
    pub epoch: i64,
    pub expires_at: DateTime<Utc>,
    pub live_replicas: Json<Vec<SessionLeasePublicPeer>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionLeasePeer {
    pub node_id: String,
    pub ops_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionLeasePublicPeer {
    pub node_id: String,
    pub public_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProducerSequenceCheck {
    AcceptFirst,
    AcceptNext,
    ReplayOrConflict { expected: i64 },
    Gap { expected: i64 },
}

#[derive(Debug, Clone)]
pub struct AppendOutcome {
    pub reply: AppendReply,
    pub event: Option<EventResponse>,
    pub tenant_id: String,
}

#[derive(Debug, Clone)]
pub struct ArchiveStateOutcome {
    pub session: SessionResponse,
    pub tenant_id: String,
    pub changed: bool,
}

#[derive(Debug, Clone)]
pub struct SessionSnapshot {
    pub tenant_id: String,
    pub session: SessionResponse,
    pub archived_seq: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct SessionSnapshotRow {
    id: String,
    title: Option<String>,
    tenant_id: String,
    creator_id: Option<String>,
    creator_type: Option<String>,
    metadata: serde_json::Value,
    last_seq: i64,
    archived_seq: i64,
    archived: bool,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ArchiveState {
    pub tenant_id: String,
    pub last_seq: i64,
    pub archived_seq: i64,
}

pub async fn create_session(
    pool: &PgPool,
    input: ValidatedCreateSession,
) -> Result<SessionResponse, AppError> {
    let row = sqlx::query_as::<_, SessionRow>(
        r#"
        INSERT INTO sessions (
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        "#,
    )
    .bind(input.id)
    .bind(input.title)
    .bind(input.tenant_id)
    .bind(input.creator_principal.id)
    .bind(input.creator_principal.principal_type)
    .bind(serde_json::Value::Object(input.metadata))
    .fetch_one(pool)
    .await
    .map_err(map_insert_error)?;

    row.try_into()
}

pub async fn list_sessions(pool: &PgPool, opts: ListOptions) -> Result<SessionsPage, AppError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        FROM sessions
        WHERE TRUE
        "#,
    );

    if let Some(cursor) = opts.cursor.as_ref() {
        builder.push(" AND id > ").push_bind(cursor);
    }

    match opts.archived {
        ArchivedFilter::Active => {
            builder.push(" AND archived = FALSE");
        }
        ArchivedFilter::Archived => {
            builder.push(" AND archived = TRUE");
        }
        ArchivedFilter::All => {}
    }

    if let Some(tenant_id) = opts.tenant_id.as_ref() {
        builder.push(" AND tenant_id = ").push_bind(tenant_id);
    }

    if let Some(session_id) = opts.session_id.as_ref() {
        builder.push(" AND id = ").push_bind(session_id);
    }

    if !opts.metadata.is_empty() {
        builder
            .push(" AND metadata @> ")
            .push_bind(serde_json::Value::Object(opts.metadata.clone()));
    }

    builder
        .push(" ORDER BY id ASC LIMIT ")
        .push_bind(i64::from(opts.limit));

    let rows = builder
        .build_query_as::<SessionRow>()
        .fetch_all(pool)
        .await
        .map_err(AppError::from)?;

    let next_cursor = if rows.len() == opts.limit as usize {
        rows.last().map(|row| row.id.clone())
    } else {
        None
    };

    let sessions = rows
        .into_iter()
        .map(SessionResponse::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(SessionsPage {
        sessions,
        next_cursor,
    })
}

pub async fn get_session_snapshot(
    pool: &PgPool,
    session_id: &str,
) -> Result<SessionSnapshot, AppError> {
    let row = load_session_snapshot_row(pool, session_id).await?;
    let tenant_id = row.tenant_id.clone();
    let archived_seq = row.archived_seq;
    let session = SessionResponse::try_from(SessionRow {
        id: row.id,
        title: row.title,
        tenant_id: row.tenant_id,
        creator_id: row.creator_id,
        creator_type: row.creator_type,
        metadata: row.metadata,
        last_seq: row.last_seq,
        archived: row.archived,
        created_at: row.created_at,
        updated_at: row.updated_at,
        version: row.version,
    })?;

    Ok(SessionSnapshot {
        tenant_id,
        session,
        archived_seq,
    })
}

pub async fn update_session(
    pool: &PgPool,
    session_id: &str,
    patch: ValidatedUpdateSession,
) -> Result<SessionResponse, AppError> {
    let mut tx = pool.begin().await?;

    let current = load_session_row_tx(&mut tx, session_id).await?;

    if let Some(expected_version) = patch.expected_version
        && current.version != expected_version
    {
        return Err(AppError::ExpectedVersionConflict {
            expected: expected_version,
            current: current.version,
        });
    }

    let current_metadata = value_to_object(&current.metadata)?;
    let merged_metadata = patch
        .metadata
        .as_ref()
        .map(|incoming| merge_metadata(&current_metadata, incoming))
        .unwrap_or(current_metadata);

    let title = match patch.title {
        Some(value) => value,
        None => current.title,
    };

    let row = sqlx::query_as::<_, SessionRow>(
        r#"
        UPDATE sessions
        SET
          title = $2,
          metadata = $3,
          updated_at = now(),
          version = version + 1
        WHERE id = $1
        RETURNING
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        "#,
    )
    .bind(session_id)
    .bind(title)
    .bind(serde_json::Value::Object(merged_metadata))
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;

    row.try_into()
}

pub async fn set_archive_state(
    pool: &PgPool,
    session_id: &str,
    archived: bool,
) -> Result<ArchiveStateOutcome, AppError> {
    let current = load_session_row(pool, session_id).await?;
    let tenant_id = current.tenant_id.clone();

    if current.archived == archived {
        return Ok(ArchiveStateOutcome {
            session: current.try_into()?,
            tenant_id,
            changed: false,
        });
    }

    let row = sqlx::query_as::<_, SessionRow>(
        r#"
        UPDATE sessions
        SET
          archived = $2,
          updated_at = now()
        WHERE id = $1
        RETURNING
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        "#,
    )
    .bind(session_id)
    .bind(archived)
    .fetch_one(pool)
    .await?;

    Ok(ArchiveStateOutcome {
        session: row.try_into()?,
        tenant_id,
        changed: true,
    })
}

pub async fn get_archive_state(pool: &PgPool, session_id: &str) -> Result<ArchiveState, AppError> {
    sqlx::query_as::<_, ArchiveState>(
        r#"
        SELECT
          tenant_id,
          last_seq,
          archived_seq
        FROM sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?
    .ok_or(AppError::SessionNotFound)
}

pub async fn mark_archived_seq(
    pool: &PgPool,
    session_id: &str,
    upto_seq: i64,
) -> Result<i64, AppError> {
    if upto_seq < 0 {
        return Err(AppError::Internal);
    }

    let row = sqlx::query_as::<_, ArchiveState>(
        r#"
        UPDATE sessions
        SET archived_seq = GREATEST(archived_seq, LEAST(last_seq, $2))
        WHERE id = $1
        RETURNING
          tenant_id,
          last_seq,
          archived_seq
        "#,
    )
    .bind(session_id)
    .bind(upto_seq)
    .fetch_optional(pool)
    .await?
    .ok_or(AppError::SessionNotFound)?;

    Ok(row.archived_seq)
}

pub async fn publish_archive_progress(
    pool: &PgPool,
    emitter_id: &str,
    session_id: &str,
    tenant_id: &str,
    archived_seq: i64,
) -> Result<(), AppError> {
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(ARCHIVE_NOTIFICATION_CHANNEL)
        .bind(
            serde_json::to_string(&ArchiveNotification {
                emitter_id: emitter_id.to_string(),
                session_id: session_id.to_string(),
                tenant_id: tenant_id.to_string(),
                archived_seq,
            })
            .map_err(|_| AppError::Internal)?,
        )
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_events(
    pool: &PgPool,
    session_id: &str,
    opts: EventsOptions,
) -> Result<EventsPage, AppError> {
    let _ = load_session_row(pool, session_id).await?;

    let rows = sqlx::query_as::<_, EventRow>(
        r#"
        SELECT
          session_id,
          seq,
          type,
          payload,
          actor,
          source,
          metadata,
          refs,
          idempotency_key,
          producer_id,
          producer_seq,
          tenant_id,
          inserted_at
        FROM events
        WHERE session_id = $1
          AND seq > $2
        ORDER BY seq ASC
        LIMIT $3
        "#,
    )
    .bind(session_id)
    .bind(opts.cursor)
    .bind(i64::from(opts.limit))
    .fetch_all(pool)
    .await?;

    let next_cursor = rows.last().map(|row| row.seq);
    let events = rows
        .into_iter()
        .map(EventResponse::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(EventsPage {
        events,
        next_cursor,
    })
}

pub async fn load_event_by_seq(
    pool: &PgPool,
    session_id: &str,
    seq: i64,
) -> Result<Option<EventResponse>, AppError> {
    let row = sqlx::query_as::<_, EventRow>(
        r#"
        SELECT
          session_id,
          seq,
          type,
          payload,
          actor,
          source,
          metadata,
          refs,
          idempotency_key,
          producer_id,
          producer_seq,
          tenant_id,
          inserted_at
        FROM events
        WHERE session_id = $1
          AND seq = $2
        "#,
    )
    .bind(session_id)
    .bind(seq)
    .fetch_optional(pool)
    .await?;

    row.map(EventResponse::try_from).transpose()
}

pub async fn load_first_event_seq(
    pool: &PgPool,
    session_id: &str,
) -> Result<Option<i64>, AppError> {
    sqlx::query_scalar::<_, i64>(
        r#"
        SELECT seq
        FROM events
        WHERE session_id = $1
        ORDER BY seq ASC
        LIMIT 1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
    .map_err(AppError::from)
}

pub async fn acquire_session_lease(
    pool: &PgPool,
    session_id: &str,
    owner_id: &str,
    ttl_ms: i64,
    desired_replica_count: u32,
) -> Result<SessionLeaseRow, AppError> {
    let mut tx = pool.begin().await?;
    let live_nodes = load_live_control_nodes(tx.as_mut(), session_id).await?;
    let live_node_ids = live_nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<HashSet<_>>();
    let existing = load_session_lease_record(tx.as_mut(), session_id).await?;
    let now = Utc::now();
    let designated_owner = live_nodes
        .first()
        .map(|node| node.node_id.clone())
        .unwrap_or_else(|| owner_id.to_string());
    let target_owner = choose_target_owner(
        existing.as_ref(),
        owner_id,
        &designated_owner,
        &live_node_ids,
        now,
    );
    let should_write = existing
        .as_ref()
        .is_none_or(|lease| lease.owner_id == owner_id || lease.expires_at <= now);

    if let Some(existing) = existing.as_ref() {
        if should_write {
            let next_epoch = if existing.owner_id == owner_id {
                existing.epoch
            } else {
                existing.epoch.saturating_add(1)
            };
            let replica_node_ids = rebalance_replica_nodes(
                normalized_replica_nodes(existing),
                &target_owner,
                &live_nodes,
                desired_replica_count,
            );

            sqlx::query(
                r#"
                UPDATE session_leases
                SET
                  owner_id = $2,
                  epoch = $3,
                  expires_at = now() + ($4 * interval '1 millisecond'),
                  replica_node_ids = $5,
                  updated_at = now()
                WHERE session_id = $1
                "#,
            )
            .bind(session_id)
            .bind(&target_owner)
            .bind(next_epoch)
            .bind(ttl_ms)
            .bind(&replica_node_ids)
            .execute(&mut *tx)
            .await?;
        }
    } else {
        let replica_node_ids = rebalance_replica_nodes(
            Vec::new(),
            &target_owner,
            &live_nodes,
            desired_replica_count,
        );

        sqlx::query(
            r#"
            INSERT INTO session_leases (
              session_id,
              owner_id,
              epoch,
              expires_at,
              replica_node_ids
            )
            VALUES (
              $1,
              $2,
              1,
              now() + ($3 * interval '1 millisecond'),
              $4
            )
            ON CONFLICT (session_id) DO NOTHING
            "#,
        )
        .bind(session_id)
        .bind(&target_owner)
        .bind(ttl_ms)
        .bind(&replica_node_ids)
        .execute(&mut *tx)
        .await?;
    }

    let row = load_session_lease_row(tx.as_mut(), session_id).await?;
    tx.commit().await?;
    Ok(row)
}

pub async fn load_session_lease_takeover_hint(
    pool: &PgPool,
    session_id: &str,
) -> Result<Option<SessionLeaseTakeoverHint>, AppError> {
    Ok(sqlx::query_as::<_, SessionLeaseTakeoverHint>(
        r#"
        SELECT
          session_leases.epoch,
          session_leases.expires_at,
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'node_id', live_replica.node_id,
                'public_url', live_replica.public_url
              )
              ORDER BY replica.ord
            ) FILTER (
              WHERE live_replica.node_id IS NOT NULL
                AND replica.node_id <> session_leases.owner_id
            ),
            '[]'::jsonb
          ) AS live_replicas
        FROM session_leases
        LEFT JOIN LATERAL unnest(session_leases.replica_node_ids) WITH ORDINALITY AS replica(node_id, ord)
          ON TRUE
        LEFT JOIN control_nodes AS live_replica
          ON live_replica.node_id = replica.node_id
         AND live_replica.draining = FALSE
         AND live_replica.expires_at > now()
        WHERE session_leases.session_id = $1
        GROUP BY
          session_leases.epoch,
          session_leases.expires_at
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?)
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct LiveControlNodeRow {
    node_id: String,
    active_lease_count: i64,
    owner_rank: String,
}

#[derive(Debug, Clone)]
struct LiveControlNode {
    node_id: String,
    active_lease_count: i64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct SessionLeaseRecord {
    owner_id: String,
    epoch: i64,
    expires_at: DateTime<Utc>,
    replica_node_ids: Vec<String>,
}

async fn load_live_control_nodes(
    tx: &mut PgConnection,
    session_id: &str,
) -> Result<Vec<LiveControlNode>, AppError> {
    let rows = sqlx::query_as::<_, LiveControlNodeRow>(
        r#"
        WITH active_replica_load AS (
          SELECT
            replica.node_id,
            COUNT(*)::bigint AS active_lease_count
          FROM session_leases
          CROSS JOIN LATERAL unnest(replica_node_ids) AS replica(node_id)
          WHERE expires_at > now()
          GROUP BY replica.node_id
        )
        SELECT
          control_nodes.node_id,
          COALESCE(active_replica_load.active_lease_count, 0) AS active_lease_count,
          md5($1 || ':' || COALESCE(control_nodes.public_url, control_nodes.node_id)) AS owner_rank
        FROM control_nodes
        LEFT JOIN active_replica_load
          ON active_replica_load.node_id = control_nodes.node_id
        WHERE control_nodes.draining = FALSE
          AND control_nodes.expires_at > now()
        ORDER BY owner_rank ASC, control_nodes.node_id ASC
        "#,
    )
    .bind(session_id)
    .fetch_all(&mut *tx)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let _ = row.owner_rank;
            LiveControlNode {
                node_id: row.node_id,
                active_lease_count: row.active_lease_count,
            }
        })
        .collect())
}

async fn load_session_lease_record(
    tx: &mut PgConnection,
    session_id: &str,
) -> Result<Option<SessionLeaseRecord>, AppError> {
    sqlx::query_as::<_, SessionLeaseRecord>(
        r#"
        SELECT
          owner_id,
          epoch,
          expires_at,
          replica_node_ids
        FROM session_leases
        WHERE session_id = $1
        FOR UPDATE
        "#,
    )
    .bind(session_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(AppError::from)
}

async fn load_session_lease_row(
    tx: &mut PgConnection,
    session_id: &str,
) -> Result<SessionLeaseRow, AppError> {
    sqlx::query_as::<_, SessionLeaseRow>(
        r#"
        SELECT
          session_leases.owner_id,
          owner_control.public_url AS owner_public_url,
          session_leases.epoch,
          session_leases.expires_at,
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'node_id', replica_control.node_id,
                'ops_url', replica_control.ops_url
              )
              ORDER BY replica.ord
            ) FILTER (
              WHERE replica_control.node_id IS NOT NULL
                AND replica.node_id <> session_leases.owner_id
            ),
            '[]'::jsonb
          ) AS replica_peers
        FROM session_leases
        LEFT JOIN control_nodes AS owner_control
          ON owner_control.node_id = session_leases.owner_id
        LEFT JOIN LATERAL unnest(session_leases.replica_node_ids) WITH ORDINALITY AS replica(node_id, ord)
          ON TRUE
        LEFT JOIN control_nodes AS replica_control
          ON replica_control.node_id = replica.node_id
         AND replica_control.draining = FALSE
         AND replica_control.expires_at > now()
        WHERE session_leases.session_id = $1
        GROUP BY
          session_leases.owner_id,
          owner_control.public_url,
          session_leases.epoch,
          session_leases.expires_at
        "#,
    )
    .bind(session_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(AppError::from)
}

fn choose_target_owner(
    existing: Option<&SessionLeaseRecord>,
    requester_id: &str,
    designated_owner: &str,
    live_node_ids: &HashSet<String>,
    now: DateTime<Utc>,
) -> String {
    match existing {
        Some(existing) if existing.owner_id == requester_id => requester_id.to_string(),
        Some(existing) if existing.expires_at <= now => {
            let live_replica = normalized_replica_nodes(existing)
                .into_iter()
                .find(|node_id| node_id != &existing.owner_id && live_node_ids.contains(node_id));

            if live_replica.as_deref() == Some(requester_id) {
                requester_id.to_string()
            } else if let Some(live_replica) = live_replica {
                live_replica
            } else {
                designated_owner.to_string()
            }
        }
        Some(existing) => existing.owner_id.clone(),
        None => designated_owner.to_string(),
    }
}

fn normalized_replica_nodes(existing: &SessionLeaseRecord) -> Vec<String> {
    let mut replica_node_ids = existing
        .replica_node_ids
        .iter()
        .filter(|node_id| !node_id.is_empty())
        .cloned()
        .collect::<Vec<_>>();

    if !replica_node_ids
        .iter()
        .any(|node_id| node_id == &existing.owner_id)
    {
        replica_node_ids.insert(0, existing.owner_id.clone());
    }

    dedupe_nodes(replica_node_ids)
}

fn rebalance_replica_nodes(
    current_replicas: Vec<String>,
    target_owner: &str,
    live_nodes: &[LiveControlNode],
    desired_replica_count: u32,
) -> Vec<String> {
    let desired_replica_count = desired_replica_count.max(1) as usize;
    let live_node_ids = live_nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<HashSet<_>>();

    let mut replicas = vec![target_owner.to_string()];
    for node_id in current_replicas {
        if node_id != target_owner
            && live_node_ids.contains(node_id.as_str())
            && !replicas.iter().any(|current| current == &node_id)
        {
            replicas.push(node_id);
        }
    }

    let mut additional = live_nodes
        .iter()
        .filter(|node| node.node_id != target_owner)
        .filter(|node| !replicas.iter().any(|current| current == &node.node_id))
        .collect::<Vec<_>>();
    additional.sort_by(|left, right| {
        left.active_lease_count
            .cmp(&right.active_lease_count)
            .then_with(|| left.node_id.cmp(&right.node_id))
    });

    for node in additional {
        replicas.push(node.node_id.clone());
        if replicas.len() >= desired_replica_count {
            break;
        }
    }

    dedupe_nodes(replicas)
}

fn dedupe_nodes(nodes: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();

    nodes
        .into_iter()
        .filter(|node_id| seen.insert(node_id.clone()))
        .collect()
}

pub async fn release_session_lease(
    pool: &PgPool,
    session_id: &str,
    owner_id: &str,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        UPDATE session_leases
        SET
          expires_at = now(),
          updated_at = now()
        WHERE session_id = $1
          AND owner_id = $2
        "#,
    )
    .bind(session_id)
    .bind(owner_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn upsert_control_node(
    pool: &PgPool,
    node_id: &str,
    public_url: Option<&str>,
    ops_url: &str,
    draining: bool,
    ttl_ms: i64,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        WITH pruned AS (
          DELETE FROM control_nodes
          WHERE node_id <> $1
            AND expires_at <= now()
        )
        INSERT INTO control_nodes (
          node_id,
          public_url,
          ops_url,
          draining,
          expires_at
        )
        VALUES (
          $1,
          $2,
          $3,
          $4,
          now() + ($5 * interval '1 millisecond')
        )
        ON CONFLICT (node_id)
        DO UPDATE
        SET
          public_url = EXCLUDED.public_url,
          ops_url = EXCLUDED.ops_url,
          draining = EXCLUDED.draining,
          expires_at = EXCLUDED.expires_at,
          updated_at = now()
        "#,
    )
    .bind(node_id)
    .bind(public_url)
    .bind(ops_url)
    .bind(draining)
    .bind(ttl_ms)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn control_plane_table_exists(pool: &PgPool) -> Result<bool, AppError> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT to_regclass('public.control_nodes') IS NOT NULL
        "#,
    )
    .fetch_one(pool)
    .await?)
}

pub async fn load_control_node(
    pool: &PgPool,
    node_id: &str,
) -> Result<Option<ControlNodeRow>, AppError> {
    Ok(sqlx::query_as::<_, ControlNodeRow>(
        r#"
        SELECT draining, expires_at
        FROM control_nodes
        WHERE node_id = $1
        "#,
    )
    .bind(node_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn count_live_control_nodes(pool: &PgPool) -> Result<u32, AppError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM control_nodes
        WHERE draining = FALSE
          AND expires_at > now()
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(count.clamp(0, i64::from(u32::MAX)) as u32)
}

pub async fn load_producer_cursor(
    pool: &PgPool,
    session_id: &str,
    producer_id: &str,
) -> Result<Option<i64>, AppError> {
    Ok(sqlx::query_scalar::<_, i64>(
        r#"
        SELECT last_producer_seq
        FROM session_producers
        WHERE session_id = $1
          AND producer_id = $2
        "#,
    )
    .bind(session_id)
    .bind(producer_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn load_event_for_producer_seq(
    pool: &PgPool,
    session_id: &str,
    producer_id: &str,
    producer_seq: i64,
) -> Result<Option<EventResponse>, AppError> {
    let row = sqlx::query_as::<_, EventRow>(
        r#"
        SELECT
          session_id,
          seq,
          type,
          payload,
          actor,
          source,
          metadata,
          refs,
          idempotency_key,
          producer_id,
          producer_seq,
          tenant_id,
          inserted_at
        FROM events
        WHERE session_id = $1
          AND producer_id = $2
          AND producer_seq = $3
        "#,
    )
    .bind(session_id)
    .bind(producer_id)
    .bind(producer_seq)
    .fetch_optional(pool)
    .await?;

    row.map(EventResponse::try_from).transpose()
}

pub async fn persist_flushed_events(
    pool: &PgPool,
    events: &[EventResponse],
    emitter_id: &str,
) -> Result<(), AppError> {
    let Some(first_event) = events.first() else {
        return Ok(());
    };

    let mut tx = pool.begin().await?;
    let session = load_session_row_tx(&mut tx, &first_event.session_id).await?;
    let tenant_id = session.tenant_id.clone();
    let mut last_seq = session.last_seq;
    let mut inserted_event_seqs = Vec::new();

    for event in events {
        if event.session_id != first_event.session_id || event.tenant_id != tenant_id {
            return Err(AppError::Internal);
        }

        if last_seq >= event.seq {
            let existing = load_event_by_seq_tx(&mut tx, &event.session_id, event.seq).await?;

            match existing {
                Some(existing) if matches_event_response(&existing, event)? => continue,
                _ => return Err(AppError::Internal),
            }
        }

        if last_seq + 1 != event.seq {
            return Err(AppError::Internal);
        }

        let inserted_at = parse_inserted_at(&event.inserted_at)?;

        sqlx::query(
            r#"
            INSERT INTO events (
              session_id,
              seq,
              type,
              payload,
              actor,
              source,
              metadata,
              refs,
              idempotency_key,
              producer_id,
              producer_seq,
              tenant_id,
              inserted_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(&event.session_id)
        .bind(event.seq)
        .bind(&event.event_type)
        .bind(serde_json::Value::Object(event.payload.clone()))
        .bind(&event.actor)
        .bind(&event.source)
        .bind(serde_json::Value::Object(event.metadata.clone()))
        .bind(serde_json::Value::Object(event.refs.clone()))
        .bind(&event.idempotency_key)
        .bind(&event.producer_id)
        .bind(event.producer_seq)
        .bind(&event.tenant_id)
        .bind(inserted_at)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO session_producers (
              session_id,
              producer_id,
              last_producer_seq
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (session_id, producer_id)
            DO UPDATE
            SET
              last_producer_seq = GREATEST(
                session_producers.last_producer_seq,
                EXCLUDED.last_producer_seq
              ),
              updated_at = now()
            "#,
        )
        .bind(&event.session_id)
        .bind(&event.producer_id)
        .bind(event.producer_seq)
        .execute(&mut *tx)
        .await?;

        last_seq = event.seq;
        inserted_event_seqs.push(event.seq);
    }

    if last_seq > session.last_seq {
        sqlx::query("UPDATE sessions SET last_seq = $2 WHERE id = $1")
            .bind(&first_event.session_id)
            .bind(last_seq)
            .execute(&mut *tx)
            .await?;
    }

    for seq in inserted_event_seqs {
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(EVENT_NOTIFICATION_CHANNEL)
            .bind(
                serde_json::to_string(&EventNotification {
                    emitter_id: emitter_id.to_string(),
                    session_id: first_event.session_id.clone(),
                    seq,
                })
                .map_err(|_| AppError::Internal)?,
            )
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn append_lifecycle_event(
    pool: &PgPool,
    event: LifecycleEvent,
    emitter_id: &str,
) -> Result<LifecycleResponse, AppError> {
    let mut tx = pool.begin().await?;

    let row = sqlx::query_as::<_, LifecycleRow>(
        r#"
        INSERT INTO lifecycle_events (
          tenant_id,
          session_id,
          event
        )
        VALUES ($1, $2, $3)
        RETURNING
          seq,
          tenant_id,
          session_id,
          event,
          inserted_at
        "#,
    )
    .bind(event.tenant_id())
    .bind(event.session_id())
    .bind(serde_json::to_value(&event).map_err(|_| AppError::Internal)?)
    .fetch_one(&mut *tx)
    .await?;

    let response = LifecycleResponse::try_from(row)?;

    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(LIFECYCLE_NOTIFICATION_CHANNEL)
        .bind(
            serde_json::to_string(&LifecycleNotification {
                emitter_id: emitter_id.to_string(),
                cursor: response.cursor,
            })
            .map_err(|_| AppError::Internal)?,
        )
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;

    Ok(response)
}

pub async fn read_lifecycle_events(
    pool: &PgPool,
    tenant_id: &str,
    session_id: Option<&str>,
    opts: EventsOptions,
) -> Result<LifecyclePage, AppError> {
    let mut builder = QueryBuilder::<Postgres>::new(
        r#"
        SELECT
          seq,
          tenant_id,
          session_id,
          event,
          inserted_at
        FROM lifecycle_events
        WHERE tenant_id =
        "#,
    );

    builder.push_bind(tenant_id);
    builder.push(" AND seq > ").push_bind(opts.cursor);

    if let Some(session_id) = session_id {
        builder.push(" AND session_id = ").push_bind(session_id);
    }

    builder
        .push(" ORDER BY seq ASC LIMIT ")
        .push_bind(i64::from(opts.limit));

    let rows = builder
        .build_query_as::<LifecycleRow>()
        .fetch_all(pool)
        .await?;

    let next_cursor = rows.last().map(|row| row.seq);
    let events = rows
        .into_iter()
        .map(LifecycleResponse::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(LifecyclePage {
        events,
        next_cursor,
    })
}

pub async fn lifecycle_head_seq(
    pool: &PgPool,
    tenant_id: &str,
    session_id: Option<&str>,
) -> Result<i64, AppError> {
    let mut builder =
        QueryBuilder::<Postgres>::new("SELECT MAX(seq) FROM lifecycle_events WHERE tenant_id = ");
    builder.push_bind(tenant_id);

    if let Some(session_id) = session_id {
        builder.push(" AND session_id = ").push_bind(session_id);
    }

    Ok(builder
        .build_query_scalar::<Option<i64>>()
        .fetch_one(pool)
        .await?
        .unwrap_or(0))
}

pub async fn load_lifecycle_by_cursor(
    pool: &PgPool,
    cursor: i64,
) -> Result<Option<LifecycleResponse>, AppError> {
    let row = sqlx::query_as::<_, LifecycleRow>(
        r#"
        SELECT
          seq,
          tenant_id,
          session_id,
          event,
          inserted_at
        FROM lifecycle_events
        WHERE seq = $1
        "#,
    )
    .bind(cursor)
    .fetch_optional(pool)
    .await?;

    row.map(LifecycleResponse::try_from).transpose()
}

async fn load_session_row(pool: &PgPool, session_id: &str) -> Result<SessionRow, AppError> {
    let row = sqlx::query_as::<_, SessionRow>(
        r#"
        SELECT
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        FROM sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;

    row.ok_or(AppError::SessionNotFound)
}

async fn load_session_snapshot_row(
    pool: &PgPool,
    session_id: &str,
) -> Result<SessionSnapshotRow, AppError> {
    let row = sqlx::query_as::<_, SessionSnapshotRow>(
        r#"
        SELECT
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived_seq,
          archived,
          created_at,
          updated_at,
          version
        FROM sessions
        WHERE id = $1
        "#,
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await?;

    row.ok_or(AppError::SessionNotFound)
}

async fn load_session_row_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: &str,
) -> Result<SessionRow, AppError> {
    let row = sqlx::query_as::<_, SessionRow>(
        r#"
        SELECT
          id,
          title,
          tenant_id,
          creator_id,
          creator_type,
          metadata,
          last_seq,
          archived,
          created_at,
          updated_at,
          version
        FROM sessions
        WHERE id = $1
        FOR UPDATE
        "#,
    )
    .bind(session_id)
    .fetch_optional(&mut **tx)
    .await?;

    row.ok_or(AppError::SessionNotFound)
}

async fn load_event_by_seq_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    session_id: &str,
    seq: i64,
) -> Result<Option<EventRow>, AppError> {
    sqlx::query_as::<_, EventRow>(
        r#"
        SELECT
          session_id,
          seq,
          type,
          payload,
          actor,
          source,
          metadata,
          refs,
          idempotency_key,
          producer_id,
          producer_seq,
          tenant_id,
          inserted_at
        FROM events
        WHERE session_id = $1
          AND seq = $2
        "#,
    )
    .bind(session_id)
    .bind(seq)
    .fetch_optional(&mut **tx)
    .await
    .map_err(AppError::from)
}

pub(crate) fn classify_producer_sequence(
    last_producer_seq: Option<i64>,
    incoming_producer_seq: i64,
) -> ProducerSequenceCheck {
    match last_producer_seq {
        None if incoming_producer_seq == 1 => ProducerSequenceCheck::AcceptFirst,
        None => ProducerSequenceCheck::Gap { expected: 1 },
        Some(last_producer_seq) if incoming_producer_seq == last_producer_seq + 1 => {
            ProducerSequenceCheck::AcceptNext
        }
        Some(last_producer_seq) if incoming_producer_seq <= last_producer_seq => {
            ProducerSequenceCheck::ReplayOrConflict {
                expected: last_producer_seq + 1,
            }
        }
        Some(last_producer_seq) => ProducerSequenceCheck::Gap {
            expected: last_producer_seq + 1,
        },
    }
}

fn matches_event_response(existing: &EventRow, event: &EventResponse) -> Result<bool, AppError> {
    Ok(existing.session_id == event.session_id
        && existing.seq == event.seq
        && existing.event_type == event.event_type
        && value_to_object(&existing.payload)? == event.payload
        && existing.actor == event.actor
        && existing.source == event.source
        && value_to_object(&existing.metadata)? == event.metadata
        && value_to_object(&existing.refs)? == event.refs
        && existing.idempotency_key == event.idempotency_key
        && existing.producer_id == event.producer_id
        && existing.producer_seq == event.producer_seq
        && existing.tenant_id == event.tenant_id)
}

fn parse_inserted_at(raw: &str) -> Result<DateTime<Utc>, AppError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| AppError::Internal)
}

fn value_to_object(value: &serde_json::Value) -> Result<JsonMap, AppError> {
    match value {
        serde_json::Value::Object(map) => Ok(map.clone()),
        _ => Err(AppError::Internal),
    }
}

fn map_insert_error(error: sqlx::Error) -> AppError {
    if let sqlx::Error::Database(database_error) = &error
        && database_error.code().as_deref() == Some("23505")
    {
        return AppError::SessionExists;
    }

    AppError::Sqlx(error)
}

#[cfg(test)]
mod tests {
    use super::{ProducerSequenceCheck, classify_producer_sequence};

    #[test]
    fn producer_sequence_accepts_first_append() {
        assert_eq!(
            classify_producer_sequence(None, 1),
            ProducerSequenceCheck::AcceptFirst
        );
    }

    #[test]
    fn producer_sequence_rejects_gaps_from_empty_state() {
        assert_eq!(
            classify_producer_sequence(None, 3),
            ProducerSequenceCheck::Gap { expected: 1 }
        );
    }

    #[test]
    fn producer_sequence_accepts_next_seq_from_cursor_table() {
        assert_eq!(
            classify_producer_sequence(Some(4), 5),
            ProducerSequenceCheck::AcceptNext
        );
    }

    #[test]
    fn producer_sequence_sends_older_seq_to_replay_path() {
        assert_eq!(
            classify_producer_sequence(Some(4), 4),
            ProducerSequenceCheck::ReplayOrConflict { expected: 5 }
        );
    }

    #[test]
    fn producer_sequence_rejects_forward_gap() {
        assert_eq!(
            classify_producer_sequence(Some(4), 7),
            ProducerSequenceCheck::Gap { expected: 5 }
        );
    }
}
