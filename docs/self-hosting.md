# Self-hosting Starcite

> **Want to skip the ops work?** [starcite.ai](https://starcite.ai) is the hosted
> alternative — zero infrastructure, free tier, same API.

## How it works

Starcite runs as a cluster of N nodes (typically 3 or 5). Appends are replicated
across multiple nodes before acknowledgment, so no acknowledged event is ever lost —
even if a node dies mid-write.

A background archiver flushes committed events to your chosen storage backend. This
happens asynchronously; archive writes never block the append path.

Sessions are distributed across the cluster automatically. Reads can be served by any
node. Clients don't need to know which node owns a session.

## Choosing an archive backend

The archive backend is where committed events are durably stored for long-term replay
and recovery. Starcite's hot path serves from replicated in-memory state, so archive
latency doesn't affect append or tail performance.

### S3 (recommended)

Works with any S3-compatible storage: AWS S3, MinIO, Cloudflare R2, Google Cloud
Storage (via interop), etc.

**Why S3 is the default:**
- Archive writes are asynchronous — latency doesn't matter
- Scales without operational overhead
- No high-availability infrastructure to manage
- Cheapest option at any data volume

Use this unless you have a specific reason not to.

### Postgres

Supported but not recommended for most deployments.

**When to choose Postgres:**
- You need direct SQL access to archived event data (analytics, ad-hoc queries)
- You already operate a Postgres cluster and want to consolidate

**Trade-offs:**
- Adds operational complexity — you now have two distributed systems to manage
- Starcite's hot path never reads from the archive, so Postgres query speed doesn't
  help runtime performance
- Requires connection pool tuning under write-heavy workloads

## Configuration reference

| Variable | Purpose |
| --- | --- |
| `NODE_NAME` | Stable node identity (`name@host`) |
| `CLUSTER_NODES` | Comma-separated cluster peers |
| `STARCITE_WRITE_NODE_IDS` | Comma-separated static write-node identities |
| `STARCITE_WRITE_REPLICATION_FACTOR` | Replicas per group (normally `3`) |
| `STARCITE_NUM_GROUPS` | Session sharding groups (normally `256`) |
| `STARCITE_RAFT_DATA_DIR` | Persistent state data path |
| `STARCITE_ARCHIVE_ADAPTER` | Archive backend (`s3` or `postgres`) |
| `STARCITE_AUTH_MODE` | Optional auth mode override (`jwt` or `none`) |

**S3 mode** requires: `STARCITE_S3_BUCKET`, `STARCITE_S3_REGION`, and optionally
`STARCITE_S3_ENDPOINT`, `STARCITE_S3_ACCESS_KEY_ID`, `STARCITE_S3_SECRET_ACCESS_KEY`,
`STARCITE_S3_PATH_STYLE`.

**Postgres mode** requires: `DATABASE_URL`.

## Bootstrap

1. Provision three write nodes with persistent volumes.
2. Set stable `NODE_NAME` on each write node.
3. Set identical `CLUSTER_NODES` and `STARCITE_WRITE_NODE_IDS` on all nodes.
4. Start all write nodes.
5. Verify readiness:

```bash
# Check node health
curl -sS http://<node>:4000/health/ready

# Check cluster status via release RPC
bin/starcite rpc "Starcite.ControlPlane.Ops.status()"
bin/starcite rpc "Starcite.ControlPlane.Ops.ready_nodes()"
```

Expected result: node reports ready, and the ready write-node set matches the
configured static set.

## Rolling restarts

Run this sequence for each write node, one at a time. Do not restart multiple write
nodes concurrently.

1. **Drain** the target node:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.drain_node()"
   ```

2. **Wait** until drained (waits for drain convergence across visible cluster nodes):
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_drained(30000)"
   ```

3. **Restart/redeploy** the node.

4. **Undrain** the node:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.undrain_node()"
   ```

5. **Wait** for local readiness:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_ready(60000)"
   curl -sS http://<node>:4000/health/ready
   ```

6. **Verify** cluster state:
   ```bash
   bin/starcite rpc "Starcite.ControlPlane.Ops.ready_nodes()"
   bin/starcite rpc "Starcite.ControlPlane.Ops.status()"
   ```

7. Move to the next node.

## Node replacement

Preferred replacement keeps the same logical node identity:

1. Drain the node.
2. Stop the old instance.
3. Bring up the replacement with the same `NODE_NAME` and the same persistent data
   directory (or a restored equivalent).
4. Undrain, then wait for readiness.
5. Verify with `bin/starcite rpc "Starcite.ControlPlane.Ops.status()"`.

Changing write-node identities is a topology migration. Treat it as an explicit
maintenance procedure — do not mix it with routine rollouts.

## Failure scenarios

- **Single node loss:** writes continue as long as quorum (majority) remains.
- **Two-node loss in a three-node cluster:** write availability is at risk. Restore
  the failed node before attempting changes.
- **During incidents:** do not attempt membership changes. Focus on restoring the
  existing topology.

## Operational validation

Run these periodically:

1. Single write-node restart under load — confirm no churn in session distribution.
2. Drain/undrain drill — verify ready-node transitions.
3. Readiness-gate drill — ensure traffic only reaches ready nodes after restart.
