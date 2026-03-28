# Session Metadata Store Spec

## Summary

Starcite should keep persistent storage split into three concerns:

- Khepri for routing and epoch
- a Postgres session catalog for durable session metadata and `archived_seq`
- an S3 event archive for archived event payloads only

The session catalog should be one row per session. We do not need separate
"header" and "progress" records.

## Catalog Shape

Each session row should contain:

- `id`
- `tenant_id`
- `title`
- `metadata`
- `created_at`
- optional creator columns for queryable ownership views
  - `creator_id`
  - `creator_type`
- `archived_seq`

Rules:

- `archived_seq` is monotonic
- the archiver updates `archived_seq` in bulk
- `epoch` does not live in the catalog; it comes from routing
- hot-only log state like `producer_cursors` does not live in the catalog

## Why

This avoids two bad shapes:

- storing session metadata in S3 blobs
- deriving `archived_seq` by scanning archived event chunks

S3 is fine for immutable event chunks. It is the wrong place for mutable session
metadata.

## Cold Hydrate

For a fully archived frozen session:

1. read the session row from the catalog
2. set `last_seq = archived_seq`
3. get `epoch` from routing
4. start the session log with empty hot-only state

This keeps cold hydrate `O(1)` in metadata reads regardless of archived event
count.

## Write Paths

Session create:

- insert one catalog row
- do not write session metadata to the event archive

Archive flush cadence:

- write archived events to the event archive
- batch update `archived_seq` for touched sessions in the catalog

## Storage Boundaries

- `Starcite.Storage.SessionCatalog`
  - durable session metadata
  - `archived_seq`
  - list/lookups

- `Starcite.Storage.EventArchive`
  - archived event reads/writes only

- `Starcite.Archive`
  - runtime flusher that bridges hot state into the event archive and catalog

## Creator Identity

If we support "list sessions created by this principal", the catalog must store
creator identity in queryable scalar columns, not a JSON blob.

Required shape:

- `creator_id`
- `creator_type`

Query shape:

- `WHERE tenant_id = ? AND creator_id = ?`

Required index:

- `(tenant_id, creator_id, id)`
