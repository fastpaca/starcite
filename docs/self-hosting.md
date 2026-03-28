# Self-hosting Starcite

> **Want to skip the ops work?** [starcite.ai](https://starcite.ai) is the hosted
> alternative — same API, no infrastructure to manage. Everything below is for teams
> who want full control over their deployment.

## How it works

Starcite runs as a cluster of Starcite nodes (typically 3 or 5). When a client appends
an event, the event is replicated to an in-memory group before the client gets an
acknowledgment. That improves failover safety, but only durability comes from your
configured persistent stores.

A background archiver flushes committed events to S3 every few seconds and advances
`archived_seq` in the Postgres session catalog. This is fully asynchronous — it never
touches the hot path, so storage performance doesn't affect append latency.

Sessions are distributed across the cluster automatically. Clients don't need to know
which node owns a session — any node can route the request to the active owner.

For the full picture of how sharding, replication, and recovery work internally, see
[Architecture](architecture.md).

## Persistent storage

Starcite now has an opinionated split:

- Postgres stores the durable session catalog
- S3 stores archived event payloads

The session catalog is one row per session. It stores static metadata plus the mutable
`archived_seq` frontier used for cold hydrate. S3 stores immutable archived event
chunks only.

This split matters because:

- cold hydrate reads metadata in `O(1)` from Postgres
- archived event storage stays blob-friendly
- we do not scan S3 prefixes to discover archive progress
- we do not rewrite session metadata blobs on every archive flush

### S3 event archive

Works with any S3-compatible storage: AWS S3, MinIO, Cloudflare R2, Google Cloud
Storage (via interop), etc.

S3 is used only for archived event chunks:

- archive writes are async, so S3 latency does not affect append ack latency
- cold reads for old history tolerate blob-store latency
- the storage layout is append-friendly and operationally simple

### Postgres session catalog

Postgres is required for durable session metadata and archive progress:

- session headers
- tenant lookup
- `archived_seq`
- session listing/filtering

This is not the archived event store. It is the metadata store that makes cold
rehydrate and queryable session listing cheap.

## Configuration

| Variable | Purpose |
| --- | --- |
| `RELEASE_NODE` | Erlang node identity (`name@host`). Must survive restarts. |
| `SECRET_KEY_BASE` | Session encryption key. Generate with `mix phx.gen.secret`. |
| `CLUSTER_NODES` | Comma-separated cluster peers. Same value on every node. This is the cluster membership list. |
| `STARCITE_ROUTING_REPLICATION_FACTOR` | Replicas per group — normally `3`. |
| `STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS` | How long a node waits for drain completion before shutdown continues. Default `30000`. Increase this if a node can own hundreds of active sessions during rollouts. |
| `STARCITE_NUM_GROUPS` | Session sharding groups — normally `256`. Don't change this without reading [Architecture](architecture.md). |
| `STARCITE_RAFT_DATA_DIR` | Persistent state data path. Must be on a persistent volume. |
| `STARCITE_RA_WAL_DATA_DIR` | Optional separate Ra WAL path. Put this on the lowest-latency persistent volume you have. |
| `STARCITE_RA_WAL_WRITE_STRATEGY` | Optional Ra WAL write strategy override. Default is `o_sync`; other values are `default` and `sync_after_notify`. |
| `STARCITE_RA_WAL_SYNC_METHOD` | Optional Ra WAL sync method override. Default is `datasync`; other values are `sync` and `none`. |
| `STARCITE_AUTH_MODE` | `jwt` (default) or `none` for development. |
| `PORT` | HTTP listen port. Default `4000`. |

**JWT auth** (when `STARCITE_AUTH_MODE=jwt`) additionally requires:
`STARCITE_AUTH_JWT_ISSUER`, `STARCITE_AUTH_JWT_AUDIENCE`, `STARCITE_AUTH_JWKS_URL`.

Optional JWT cache controls:
- `STARCITE_AUTH_JWKS_REFRESH_MS` controls background JWKS refresh cadence.
- `STARCITE_AUTH_JWKS_HARD_EXPIRY_MS` controls how long a cached signing key remains
  valid without a successful refresh. Default: same as `STARCITE_AUTH_JWKS_REFRESH_MS`.

**Postgres metadata store** requires: `DATABASE_URL` or `STARCITE_POSTGRES_URL`.

**S3 event archive** additionally requires: `STARCITE_S3_BUCKET`, `STARCITE_S3_REGION`.
Optional: `STARCITE_S3_ENDPOINT`, `STARCITE_S3_ACCESS_KEY_ID`,
`STARCITE_S3_SECRET_ACCESS_KEY`, `STARCITE_S3_PATH_STYLE`.

**Raft WAL tuning:** `sync_after_notify` lowers observed latency by notifying the
caller before the local WAL sync completes, and `none` disables the local sync
entirely. Both trade durability semantics for latency and should be treated as
explicit operational choices, not transparent optimizations.

### S3 schema migration

When upgrading S3 archive payload schemas, run:

- `mix starcite.archive.migrate_s3_schema --dry-run`
- `mix starcite.archive.migrate_s3_schema`

Run on one node at a time during a maintenance window.

Startup never auto-migrates S3 schema. It validates `<prefix>/schema/meta.json`
and returns startup errors when migration is required or schema versions are
unsupported.

## Bootstrap

First-time cluster setup:

1. Provision three cluster nodes with persistent volumes.
2. Set a stable `RELEASE_NODE` on each — this identity must survive restarts.
3. Set identical `CLUSTER_NODES` on all nodes.
4. Start all cluster nodes.
5. Verify:

```bash
curl -sS http://<node>:4001/health/ready
bin/starcite rpc "Starcite.Operations.status()"
bin/starcite rpc "Starcite.Operations.ready_nodes()"
```

You should see each node report ready, and the ready node set should match your
configured `CLUSTER_NODES`. If a node isn't ready, check its logs — the most common
issue is misconfigured `CLUSTER_NODES` or unreachable peers.

Set `STARCITE_OPS_PORT` on each node and keep that listener private to your cluster
or admin network. `/health/live`, `/health/ready`, `/metrics`, and `pprof` are
served there instead of the public API port.

## Rolling restarts

The key constraint: never restart more than one node at a time. Starcite uses
in-memory replication with quorum writes — taking two nodes down simultaneously means
some groups lose quorum and can't accept writes.

For production rollouts, operate one node at a time:

1. **Drain** — tells the cluster to stop routing new requests to this node:
   ```bash
   bin/starcite rpc "Starcite.Operations.drain_node()"
   ```

2. **Wait for drain convergence** — the cluster needs a moment to re-route in-flight
   traffic. This command blocks until all nodes agree the target is drained:
   ```bash
   bin/starcite rpc "Starcite.Operations.wait_local_drained(30000)"
   ```

   For production rollouts, keep the node's stop grace period at or above
   `STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS`. If the node is terminated earlier than that,
   drain can be interrupted mid-transfer.

3. **Restart/redeploy** the node — a restarted drained node rejoins as `ready`
   automatically once the release is healthy again.

4. **Wait for readiness** — the node needs to rejoin the Khepri routing store and
   restore its local runtime before it can accept fresh ownership:
   ```bash
   bin/starcite rpc "Starcite.Operations.wait_local_ready(60000)"
   curl -sS http://<node>:4001/health/ready
   ```

5. **Verify** the cluster looks healthy before moving on:
   ```bash
   bin/starcite rpc "Starcite.Operations.ready_nodes()"
   bin/starcite rpc "Starcite.Operations.status()"
   ```

6. Move to the next node.

Use `Starcite.Operations.undrain_node()` only when you want to return a still-running
node to service without restarting it.

## Node replacement

The simplest approach: keep the same logical identity.

1. Drain the node.
2. Stop the old instance.
3. Bring up the replacement with the same `RELEASE_NODE` and the same persistent data
   directory (or a restored backup).
4. Undrain, wait for readiness.
5. Verify with `bin/starcite rpc "Starcite.Operations.status()"`.

If you need to change a node's identity (different hostname, different rack), that's a
topology migration — a more involved procedure. Don't mix it with routine rollouts.

## Failure scenarios

**Single node failure:** This is the expected failure mode. The remaining replicas can
continue serving sessions whose owners still have quorum, and lease expiry will
authoritatively move ownership away from a catastrophically failed node. Clients may
see a brief availability gap while ownership is reassigned.

**Two-node failure (3-node cluster):** In-memory replication quorum is lost for many
sessions. Reads may still work from surviving local state, but new appends are
rejected for affected sessions until the cluster is restored.

**During incidents:** Don't attempt membership changes. Focus on getting the existing
topology back to health. Membership changes during a partition can make things worse.

## Operational validation

Run these periodically — they're cheap and catch problems before your users do:

1. **Restart drill** — restart a single cluster node under load. Verify no churn in
   session distribution and that clients reconnect cleanly.
2. **Drain/undrain drill** — verify the ready-node set transitions correctly and that
   traffic stops/resumes as expected.
3. **Readiness gate drill** — after a restart, confirm that the health endpoint
   returns `starting` until sync is complete, and `ok` after.
