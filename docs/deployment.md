# Deployment

This document defines provider-agnostic deployment and operations for Starcite's static write-node model.

## Operating model

- Write path: fixed Raft voter set on static write nodes.
- Read/API path: stateless router/API nodes may scale independently.
- Liveness affects routing only; liveness never mutates Raft membership.
- Rollouts are one write node at a time to preserve quorum.

## Preconditions

- Each write node has a stable node identity (`NODE_NAME`).
- All nodes share the same cluster peer list (`CLUSTER_NODES`).
- All nodes share the same static write-node set (`STARCITE_WRITE_NODE_IDS`).
- `STARCITE_WRITE_REPLICATION_FACTOR` is less than or equal to write-node count.
- Write nodes have persistent local disk for `STARCITE_RAFT_DATA_DIR`.
- Archive backend is configured (`STARCITE_ARCHIVE_ADAPTER=s3` or `postgres`).

## Required configuration

| Variable | Purpose |
| --- | --- |
| `NODE_NAME` | Stable node identity (`name@host`) |
| `CLUSTER_NODES` | Comma-separated cluster peers |
| `STARCITE_WRITE_NODE_IDS` | Comma-separated static write-node identities |
| `STARCITE_WRITE_REPLICATION_FACTOR` | Voters per group (normally `3`) |
| `STARCITE_NUM_GROUPS` | Session sharding groups (normally `256`) |
| `STARCITE_RAFT_DATA_DIR` | Persistent Raft state path |
| `STARCITE_ARCHIVE_ADAPTER` | Archive backend (`s3` or `postgres`) |
| `STARCITE_AUTH_MODE` | Auth mode (`jwt` recommended in production) |

S3 mode requires: `STARCITE_S3_BUCKET`, `STARCITE_S3_REGION`, optional endpoint/credentials overrides.

Postgres mode requires: `DATABASE_URL`.

## Bootstrap checklist

1. Provision three write nodes with persistent volumes.
2. Set stable `NODE_NAME` on each write node.
3. Set identical `CLUSTER_NODES` and `STARCITE_WRITE_NODE_IDS` on all nodes.
4. Start all write nodes.
5. Verify readiness and routing state:
   - `mix starcite.ops status`
   - `mix starcite.ops ready-nodes`
   - `curl -sS http://<node>:4000/health/ready`

Expected result: node reports ready, and ready write-node set matches the configured static set.

## Rollout runbook (write nodes)

Run this sequence for each write node, one at a time.

1. Drain target node:
   - `mix starcite.ops drain <node>`
2. Wait until drained:
   - `mix starcite.ops wait-drained 30000`
   - this gate waits for observer drain convergence across visible cluster nodes
3. Restart/redeploy that node.
4. Undrain node:
   - `mix starcite.ops undrain <node>`
5. Wait for local readiness (post-undrain gate):
   - `mix starcite.ops wait-ready 60000`
   - `curl -sS http://<node>:4000/health/ready`
6. Recheck cluster:
   - `mix starcite.ops ready-nodes`
   - `mix starcite.ops status`
7. Move to the next node.

Do not restart multiple write nodes concurrently.

## Release RPC equivalents

Use release RPC when `mix` is unavailable in production:

```bash
bin/starcite rpc "Starcite.ControlPlane.Ops.drain_node()"
bin/starcite rpc "Starcite.ControlPlane.Ops.undrain_node()"
bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_drained(30000)"
bin/starcite rpc "Starcite.ControlPlane.Ops.wait_local_ready(60000)"
```

## Node replacement

Preferred replacement keeps the same logical node identity.

1. Drain the node.
2. Stop the old instance.
3. Bring up the replacement with the same `NODE_NAME` and same persistent Raft data (or restored equivalent).
4. Undrain, then wait for readiness.
5. Verify with `mix starcite.ops status`.

Changing write-node identities is a topology migration. Treat it as an explicit maintenance procedure and do not mix it with routine rollouts.

## Failure handling

- Single write-node loss: keep serving writes if quorum remains.
- Two write-node loss in a three-node voter set: write availability is at risk; restore quorum first.
- During incidents, do not attempt liveness-driven membership changes.

## Validation drills

Run these periodically:

1. Single write-node restart under load and confirm no churn in group assignment.
2. Drain/undrain drill and verify `ready-nodes` transitions.
3. Readiness-gate drill: ensure traffic only targets ready nodes after restart.
