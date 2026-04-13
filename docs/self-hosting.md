# Self-hosting Starcite

> **Want to skip the ops work?** [starcite.ai](https://starcite.ai) is the hosted
> alternative — same API, no infrastructure to manage. Everything below is for teams
> who want full control over their deployment.

## How it works

Starcite runs as a cluster of Starcite nodes (typically 3 or 5). When a client appends
an event, the event is replicated to an in-memory group before the client gets an
acknowledgment. That improves failover safety, but only durability comes from your
configured Postgres database.

A background archiver flushes committed events to Postgres every few seconds and
advances `archived_seq` in the durable session catalog. This is fully asynchronous —
it never touches the hot path, so archive writes do not affect append latency.

Sessions are distributed across the cluster automatically. Clients don't need to know
which node owns a session — any node can route the request to the active owner.

For the full picture of how sharding, replication, and recovery work internally, see
[Architecture](architecture.md).

## Persistent storage

Postgres is the only durable store Starcite needs:

- session headers
- tenant lookup
- `archived_seq`
- archived event rows
- session listing/filtering

The session catalog is one row per session. It stores static metadata plus the
mutable `archived_seq` frontier used for cold hydrate. Archived event rows live
in the same database and are written asynchronously by the archiver.

This matters because:

- cold hydrate still reads metadata in `O(1)`
- archived reads and session listing share one operational dependency
- archive writes remain off the append ack path
- standard Ecto migrations cover both metadata and archived events

## Configuration

These are the current runtime and release knobs the implementation actually
reads. Older docs sometimes mentioned `STARCITE_NUM_GROUPS`,
`STARCITE_RAFT_DATA_DIR`, and `STARCITE_RA_WAL_*`; the current runtime does not
read those variables, so they are intentionally omitted here.

### Release and network

| Variable | Purpose |
| --- | --- |
| `PHX_SERVER` | Required for Phoenix to serve HTTP in releases. The official Docker image sets this to `true`. |
| `SECRET_KEY_BASE` | Required Phoenix secret. Generate with `mise exec -- mix phx.gen.secret` when running from a repo checkout under the pinned local toolchain. |
| `PORT` | Public API listen port. Default `4000`. |
| `PHX_HOST` | External host used in endpoint URL config. Default `example.com` in prod runtime. |
| `STARCITE_OPS_PORT` | Separate ops listener for `/health/live`, `/health/ready`, `/metrics`, and `pprof`. These endpoints are not served on `PORT`. |
| `STARCITE_PPROF_PORT` | Fallback ops port if `STARCITE_OPS_PORT` is unset. |
| `STARCITE_PPROF_TIMEOUT_MS` | `pprof` profile capture timeout. Default `60000`. |
| `STARCITE_ENABLE_TELEMETRY` | Enables or disables telemetry/PromEx. Default `true`. |

### Cluster and routing

| Variable | Purpose |
| --- | --- |
| `RELEASE_NODE` | Stable Erlang node identity (`name@host`). Must survive restarts. |
| `RELEASE_COOKIE` | Erlang distribution cookie. Required for multi-node clusters. |
| `CLUSTER_NODES` | Comma-separated static cluster peers. Use this or `DNS_CLUSTER_QUERY`. |
| `DNS_CLUSTER_QUERY` | Headless DNS name for `DNSPoll` discovery. Use this or `CLUSTER_NODES`. |
| `DNS_CLUSTER_NODE_BASENAME` | Node basename used with `DNS_CLUSTER_QUERY`. Default `starcite`. |
| `DNS_POLL_INTERVAL_MS` | DNS poll interval in milliseconds. Default `5000`. |
| `STARCITE_ROUTING_REPLICATION_FACTOR` | Replicas per routing group. Default `3`. |
| `STARCITE_ROUTING_STORE_DIR` | Khepri routing store path. Default `priv/khepri`. |
| `STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS` | How long shutdown drain waits before the node stops anyway. Default `30000`. |

### Auth

| Variable | Purpose |
| --- | --- |
| `STARCITE_AUTH_MODE` | `jwt` (default) or `none` for explicit no-auth runs. |
| `STARCITE_AUTH_JWT_ISSUER` | Required when `STARCITE_AUTH_MODE=jwt`. |
| `STARCITE_AUTH_JWT_AUDIENCE` | Required when `STARCITE_AUTH_MODE=jwt`. |
| `STARCITE_AUTH_JWKS_URL` | Required when `STARCITE_AUTH_MODE=jwt`. |
| `STARCITE_AUTH_JWT_LEEWAY_SECONDS` | JWT clock-skew leeway. Default `1`. |
| `STARCITE_AUTH_JWKS_REFRESH_MS` | Background JWKS refresh cadence. Default `60000`. |
| `STARCITE_AUTH_JWKS_HARD_EXPIRY_MS` | Maximum age of cached JWKS data without a successful refresh. Defaults to `STARCITE_AUTH_JWKS_REFRESH_MS`. |

### Database and archiver

| Variable | Purpose |
| --- | --- |
| `STARCITE_ARCHIVE_FLUSH_INTERVAL_MS` | Archive flush cadence. Default `5000`. |
| `STARCITE_ARCHIVE_DB_WRITE_BATCH_SIZE` | Maximum rows per Postgres archive insert statement before the archiver splits the batch. Default `1000`, further clamped below Postgres parameter limits. |
| `STARCITE_ARCHIVE_DB_READ_BATCH_SIZE` | Maximum rows per Postgres archived-read query before replay splits the range. Default `1000`. |
| `STARCITE_ARCHIVE_LEGACY_S3_ENABLED` | Enables the temporary legacy-S3 cutover path that double-writes archive batches to Postgres and S3, and falls back to S3 for cold reads when Postgres is incomplete. Default `false`. |
| `STARCITE_ARCHIVE_LEGACY_S3_BOOT_BACKFILL_ENABLED` | Starts a one-node boot sweep that backfills archived sessions from legacy S3 into Postgres when legacy-S3 cutover is enabled. Default `false`. |
| `STARCITE_ARCHIVE_LEGACY_S3_BATCH_SIZE` | Maximum rows read from legacy S3 per session backfill batch during lazy reads and boot backfills. Default `1000`. |
| `STARCITE_ARCHIVE_LEGACY_S3_BUCKET` | Legacy archive bucket used only during cutover. Required when legacy-S3 cutover is enabled. |
| `STARCITE_ARCHIVE_LEGACY_S3_PREFIX` | Archive prefix inside the legacy bucket. Default `starcite`. |
| `STARCITE_ARCHIVE_LEGACY_S3_REGION` | Region for the legacy archive bucket. |
| `STARCITE_ARCHIVE_LEGACY_S3_ACCESS_KEY_ID` | Access key for the legacy archive bucket. |
| `STARCITE_ARCHIVE_LEGACY_S3_SECRET_ACCESS_KEY` | Secret key for the legacy archive bucket. |
| `STARCITE_ARCHIVE_LEGACY_S3_SESSION_TOKEN` | Optional session token for temporary credentials. |
| `STARCITE_ARCHIVE_LEGACY_S3_ENDPOINT` | Optional S3-compatible endpoint such as MinIO. |
| `STARCITE_ARCHIVE_LEGACY_S3_PATH_STYLE` | Enables path-style requests for S3-compatible endpoints. Default `true`. |
| `STARCITE_ARCHIVE_LEGACY_S3_MAX_WRITE_RETRIES` | Maximum optimistic retry count per S3 chunk write during the cutover window. Default `4`. |
| `DATABASE_URL` | Primary Postgres URL. Required in prod unless you set `STARCITE_POSTGRES_URL` instead. |
| `STARCITE_POSTGRES_URL` | Alternate Postgres URL. |
| `DB_POOL_SIZE` | Ecto pool size. Default `10`. |

### Runtime storage and cache tuning

| Variable | Purpose |
| --- | --- |
| `STARCITE_EVENT_STORE_MAX_SIZE` | Hot event-store memory budget. Default `2147483648` bytes. Accepts units such as `256MB` or `4G`. |
| `STARCITE_EVENT_STORE_CAPACITY_CHECK_INTERVAL` | Event-store capacity poll interval. Default `4`. |
| `STARCITE_ARCHIVE_READ_CACHE_MAX_SIZE` | Archive-read cache memory budget. Default `536870912` bytes. Accepts the same size units as `STARCITE_EVENT_STORE_MAX_SIZE`. |
| `STARCITE_ARCHIVE_READ_CACHE_RECLAIM_FRACTION` | Fraction reclaimed when the archive-read cache trims. Default `0.25`. |
| `STARCITE_ARCHIVE_READ_CACHE_COMPRESSED` | Enables compressed archive-read cache entries. Default `true`. |
| `STARCITE_SESSION_STORE_TTL_MS` | Session-store TTL. Default `21600000`. |
| `STARCITE_SESSION_STORE_PURGE_INTERVAL_MS` | Session-store purge interval. Default `60000`. |
| `STARCITE_SESSION_STORE_COMPRESSED` | Enables compressed session-store entries. Default `true`. |
| `STARCITE_SESSION_STORE_TOUCH_ON_READ` | Refreshes session-store TTL on reads. Default `true`. |

Archive schema changes now ship through the normal Ecto migration path.

## Migrating legacy S3 archives

For a clean rolling cutover, use one temporary legacy-S3 cutover release and keep the
offline importer as an optional bulk accelerator.

### Rolling legacy-S3 cutover release

1. Deploy a cutover release with `STARCITE_ARCHIVE_LEGACY_S3_ENABLED=true` and
   the legacy S3 connection variables populated.
2. Optionally enable `STARCITE_ARCHIVE_LEGACY_S3_BOOT_BACKFILL_ENABLED=true` so one
   node sweeps archived sessions on startup and backfills Postgres from S3. The
   sweep is guarded by a Postgres advisory lock, so only one node runs it at a
   time.
3. While this cutover release is live:
   archive writes succeed only after both Postgres and S3 are updated
   archived reads try Postgres first
   if Postgres is missing archived rows for a session, the read falls back to
   S3, migrates that session’s full archived prefix into Postgres, and then
   serves the requested range
4. Keep the cutover release live through your cold window and for the burn-in
   period you want for stragglers or long-tail sessions.
5. Before removing S3, verify that Postgres now contains every archived prefix:
   `mise exec -- mix starcite.archive.verify_cutover`
6. Deploy the Postgres-only release with the legacy-S3 env vars removed.
7. Keep the old archive bucket until you have validated the rollout and no
   rollback path is needed.

### Optional offline bulk backfill

If you want a faster initial catch-up than lazy reads and boot sweep alone,
export the legacy archive prefix to local disk with your usual S3 tooling, then
run the importer against that export:

`aws s3 sync s3://<bucket>/<prefix> /tmp/starcite-archive-export`
`mc cp --recursive local/<bucket>/<prefix> /tmp/starcite-archive-export`
`mise exec -- mix starcite.archive.import_s3_export --root /tmp/starcite-archive-export --dry-run`
`mise exec -- mix starcite.archive.import_s3_export --root /tmp/starcite-archive-export`

The importer is idempotent on `(session_id, seq)`, so reruns are safe. It only
copies archive rows into Postgres; it does not mutate `sessions.archived_seq`.
That makes it safe to run alongside the cutover release or ahead of it.

`mix starcite.archive.reconcile_progress` remains available for offline
migrations into a fresh database or for repairing catalog metadata, but you
should not need it for the rolling cutover path because the live session catalog
already owns `archived_seq`. `verify_cutover` is the final guardrail before you
switch to the Postgres-only release. None of the offline tasks talk to S3
directly; they read exported NDJSON files from disk.

## Bootstrap

First-time cluster setup:

1. Provision three cluster nodes with persistent volumes.
2. Set a stable `RELEASE_NODE` on each — this identity must survive restarts.
3. Configure discovery with either identical `CLUSTER_NODES` on all nodes or a shared `DNS_CLUSTER_QUERY` setup.
4. Start all cluster nodes.
5. Verify:

```bash
curl -sS http://<node>:${STARCITE_OPS_PORT}/health/ready
bin/starcite rpc "Starcite.Operations.status()"
bin/starcite rpc "Starcite.Operations.ready_nodes()"
```

You should see each node report ready, and the ready node set should match your
configured discovery mode. If a node isn't ready, check its logs — the most common
issue is misconfigured cluster discovery or unreachable peers.

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
   curl -sS http://<node>:${STARCITE_OPS_PORT}/health/ready
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
see a brief availability gap while ownership is reassigned. Fresh session claims
continue on the surviving ready nodes once the failed node's lease expires; the
replica set stays degraded until the node returns.

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
