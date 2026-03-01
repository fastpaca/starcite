# Self-hosting Starcite

> **Want to skip the ops work?** [starcite.ai](https://starcite.ai) is the hosted
> alternative — same API, no infrastructure to manage. Everything below is for teams
> who want full control over their deployment.

## How it works

Starcite runs as a cluster of write nodes (typically 3 or 5). When a client appends
an event, the event is replicated across multiple nodes before the client gets an
acknowledgment. This means no acknowledged event is ever lost — even if a node dies
mid-write.

A background archiver flushes committed events to your chosen storage backend (S3 or
Postgres) every few seconds. This is fully asynchronous — it never touches the hot
path, so archive performance doesn't affect append latency.

Sessions are distributed across the cluster automatically. Clients don't need to know
which node owns a session — any node can route the request to the right place.

For the full picture of how sharding, replication, and recovery work internally, see
[Architecture](architecture.md).

## Choosing an archive backend

The archive backend stores committed events for long-term durability and historical
replay. Appends never touch the archive — they commit to replicated in-memory state
and return immediately. Tail replay reads from a two-tier store: recent events come
from memory (hot), older evicted events are fetched from the archive (cold).

In practice, most tail connections replay recent history and never hit the archive.
But if a client reconnects with a very old cursor, archive read latency matters. Pick
your backend based on operational simplicity and query needs.

### S3 (recommended)

Works with any S3-compatible storage: AWS S3, MinIO, Cloudflare R2, Google Cloud
Storage (via interop), etc.

S3 is the right default because:
- Archive writes are async — S3's write latency doesn't affect appends
- Cold reads (old cursor replay) are rare and tolerate S3's read latency
- No HA infrastructure to manage — the provider handles it
- Scales to any data volume without schema migrations or connection pool tuning
- Cheapest option at every scale

Use this unless you have a specific reason not to.

### Postgres

Supported, but think carefully before choosing it.

Postgres makes sense if you need direct SQL access to archived event data — analytics
dashboards, ad-hoc queries across sessions, that kind of thing. It also makes sense
if you already operate a Postgres cluster and want to avoid adding another dependency.

The trade-offs are real though. You're now operating two distributed systems (Starcite
+ Postgres). Postgres gives you faster cold reads when clients replay old history, but
for most workloads the hot tier serves everything. And under write-heavy workloads,
you'll need to tune connection pools and potentially deal with Postgres replication
lag separately from Starcite's own replication.

## Configuration

| Variable | Purpose |
| --- | --- |
| `RELEASE_NODE` | Erlang node identity (`name@host`). Must survive restarts. |
| `SECRET_KEY_BASE` | Session encryption key. Generate with `mix phx.gen.secret`. |
| `CLUSTER_NODES` | Comma-separated cluster peers. Same value on every node. |
| `STARCITE_WRITE_NODE_IDS` | Comma-separated static write-node identities. Same value on every node. |
| `STARCITE_WRITE_REPLICATION_FACTOR` | Replicas per group — normally `3`. |
| `STARCITE_NUM_GROUPS` | Session sharding groups — normally `256`. Don't change this without reading [Architecture](architecture.md). |
| `STARCITE_RAFT_DATA_DIR` | Persistent state data path. Must be on a persistent volume. |
| `STARCITE_ARCHIVE_ADAPTER` | `s3` (default) or `postgres`. |
| `STARCITE_AUTH_MODE` | `jwt` (default) or `none` for development. |
| `PORT` | HTTP listen port. Default `4000`. |

**JWT auth** (when `STARCITE_AUTH_MODE=jwt`) additionally requires:
`STARCITE_AUTH_JWT_ISSUER`, `STARCITE_AUTH_JWT_AUDIENCE`, `STARCITE_AUTH_JWKS_URL`.

**S3 backend** additionally requires: `STARCITE_S3_BUCKET`, `STARCITE_S3_REGION`.
Optional: `STARCITE_S3_ENDPOINT`, `STARCITE_S3_ACCESS_KEY_ID`,
`STARCITE_S3_SECRET_ACCESS_KEY`, `STARCITE_S3_PATH_STYLE`.

**Postgres backend** additionally requires: `DATABASE_URL`.

## Bootstrap

First-time cluster setup:

1. Provision three write nodes with persistent volumes.
2. Set a stable `RELEASE_NODE` on each — this identity must survive restarts.
3. Set identical `CLUSTER_NODES` and `STARCITE_WRITE_NODE_IDS` on all nodes.
4. Start all write nodes.
5. Verify:

```bash
curl -sS http://<node>:4000/health/ready
bin/starcite rpc "Starcite.ControlPlane.Ops.status()"
bin/starcite rpc "Starcite.ControlPlane.Ops.ready_nodes()"
```

You should see each node report ready, and the ready write-node set should match your
configured `STARCITE_WRITE_NODE_IDS`. If a node isn't ready, check its logs — the
most common issue is misconfigured `CLUSTER_NODES` or unreachable peers.

## Rolling restarts

The key constraint: never restart more than one write node at a time. Starcite uses
3-way replication with quorum writes — taking two nodes down simultaneously means
some groups lose quorum and can't accept writes.

For each write node, one at a time:

1. **Drain** — tells the cluster to stop routing new requests to this node:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.drain_node()"
   ```

2. **Wait for drain convergence** — the cluster needs a moment to re-route in-flight
   traffic. This command blocks until all nodes agree the target is drained:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_drained(30000)"
   ```

3. **Restart/redeploy** the node.

4. **Undrain** — tells the cluster this node is available again:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.undrain_node()"
   ```

5. **Wait for readiness** — the node needs to sync its Raft state before it can serve
   traffic. This blocks until sync is complete:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_ready(60000)"
   curl -sS http://<node>:4000/health/ready
   ```

6. **Verify** the cluster looks healthy before moving on:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.ready_nodes()"
   bin/starcite rpc "Starcite.ControlPlane.Ops.status()"
   ```

7. Move to the next node.

## Node replacement

The simplest approach: keep the same logical identity.

1. Drain the node.
2. Stop the old instance.
3. Bring up the replacement with the same `RELEASE_NODE` and the same persistent data
   directory (or a restored backup).
4. Undrain, wait for readiness.
5. Verify with `bin/starcite rpc "Starcite.ControlPlane.Ops.status()"`.

If you need to change a node's identity (different hostname, different rack), that's a
topology migration — a more involved procedure. Don't mix it with routine rollouts.

## Failure scenarios

**Single node failure:** This is the expected failure mode, and it's handled
gracefully. The remaining 2 of 3 replicas maintain quorum. Writes continue. Leaders
on the failed node are re-elected on surviving nodes within seconds. Clients see a
brief latency spike, not data loss.

**Two-node failure (3-node cluster):** Quorum is lost for affected groups. Reads
(tail replay from local state) may still work on the surviving node, but new appends
are rejected. Priority is restoring the failed nodes, not making membership changes.

**During incidents:** Don't attempt membership changes. Focus on getting the existing
topology back to health. Membership changes during a partition can make things worse.

## Operational validation

Run these periodically — they're cheap and catch problems before your users do:

1. **Restart drill** — restart a single write node under load. Verify no churn in
   session distribution and that clients reconnect cleanly.
2. **Drain/undrain drill** — verify the ready-node set transitions correctly and that
   traffic stops/resumes as expected.
3. **Readiness gate drill** — after a restart, confirm that the health endpoint
   returns `starting` until sync is complete, and `ok` after.
