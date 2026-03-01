# Architecture

Starcite is a clustered session-stream service. Each session is an ordered,
append-only event log. The system is optimized for one thing: low-latency durable
appends with cursor-based replay.

This page explains how the internals work, anchored to the architecture diagram
below.

![Starcite Architecture](img/architecture.png)

The system is split into three layers: **edge**, **data plane**, and **control
plane**.

## Edge

The edge handles client connections, auth, and protocol translation.

**Session API** — REST controller for `POST /v1/sessions` (create), `POST
/v1/sessions/:id/append` (append), and `GET /v1/sessions` (list). Validates input,
enforces tenant fencing, and delegates to WritePath or ReadPath on the data plane.

**Tailer** — WebSocket handler for `GET /v1/sessions/:id/tail?cursor=N`. Upgrades
the HTTP connection to a long-lived WebSocket process (TailSocket) that manages
replay and live streaming. The tailer is the only stateful process per client
connection.

**Auth** — A plug pipeline that validates bearer JWTs on every `/v1` request.
Verifies signatures against JWKS keys fetched from a configured URL (cached with
TTL), validates standard claims (`iss`, `aud`, `exp`, `tenant_id`, `sub`, scopes),
and builds an auth context with a principal and scope set. WebSocket connections
accept the token as either an `Authorization` header or an `access_token` query
param.

## Data plane

The data plane owns session state, event ordering, and durable storage. Writes run on
consensus (write nodes only). Reads run on every node.

### Router

Every request needs to reach the right Raft group. The Router computes the group for
a session by hashing the session ID (`phash2(session_id, num_groups)`), then looks up
the replica set for that group. Replica assignment is deterministic — for each group,
every write node gets a score via `phash2({group_id, node})`, and the top N nodes by
score become replicas (where N is the replication factor, typically 3).

The ReplicaRouter picks a target: local node if it's a replica and ready, otherwise
RPC to a remote replica. For writes, it routes to the Raft leader (with a leader hint
cache, 10s TTL). For reads, any ready replica works. If the target returns
`:not_leader`, the router retries with the leader hint from the response.

### WritePath

Handles `create_session` and `append_event`. Sends Raft commands to the leader via
Ra's pipeline command interface — the caller enqueues the command and blocks waiting
for the `:ra_event` reply with the committed result. No intermediate helper process.

On commit, the Raft FSM applies the command, assigns the next monotonic `seq`, and
produces a side effect: a PubSub broadcast to the session's cursor update topic. This
is how tail subscribers learn about new events.

After a successful append, the write path also upserts the session in the archive
index and warms the local SessionStore cache. Neither of these is on the critical
path — if they fail, the append still succeeds.

**Deduplication:** The FSM tracks `(producer_id, producer_seq)` pairs per session. A
retry with the same pair returns the previously committed `seq` with `deduped: true`.

**Optimistic concurrency:** If `expected_seq` doesn't match the session's current
`seq`, the append is rejected with a `409`.

### ReadPath

Serves session metadata and event queries. Reads are routed to any available replica
(not necessarily the leader) via RPC.

Event reads use a two-tier model:

1. **Hot tier** — the EventStore's pending event queue, an ETS table holding events
   that haven't been archived yet. This is the fast path.
2. **Cold tier** — the archive backend (S3 or Postgres), queried only when there's a
   gap between the cursor and the oldest hot event (i.e., older events were archived
   and evicted from memory).

Results from both tiers are merged, deduplicated by `seq`, and verified to be
gap-free. If the archive is unavailable, the read path falls back to hot events only.

### Raft FSM

The state machine at the heart of the data plane. Each Raft group runs one FSM
instance holding state for all sessions assigned to that group:

- Session metadata (id, title, tenant, creation time)
- Ordered event log per session
- Producer deduplication state (`producer_id` → last seen `producer_seq`)
- Archive progress (`archived_seq` — how far the archiver has flushed)

The FSM handles four commands: `create_session`, `append_event` (single),
`append_events` (batch), and `ack_archived` (advance the archive cursor and allow
EventStore eviction).

Checkpoints are taken every 2048 log entries for log compaction and recovery.

### Event Store

An in-memory two-tier event facade:

- **EventQueue** — two ETS tables: one mapping `{session_id, seq}` → event, and an
  index tracking the max `seq` per session. This holds unarchived ("pending") events.
- **Archive read cache** — a Cachex cache that holds recently read archived events,
  so repeated cold reads don't hit the archive backend every time.

The EventStore has memory pressure management. A configurable byte limit (default 2GB)
triggers LRU eviction of the archive read cache when exceeded.

### Session Store

A Cachex cache holding session metadata (title, creator, tenant, etc.) with a 6-hour
TTL and touch-on-read. On cache miss, loads from the archive backend. This avoids
hitting the archive on every session access for the read path and tail connections.

Does not cache event data — that's the EventStore's job.

### Archiver

A background GenServer that pulls committed events from the EventStore and flushes
them to the archive backend on a ~5 second tick. Pull-based, not push-based.

On each tick, the archiver scans active sessions, compares the EventStore's max `seq`
against the Raft FSM's `archived_seq`, and flushes any pending events in batches
(5000 events per cycle). Only the Raft leader for a group archives its sessions —
this prevents duplicate writes across replicas.

After a successful flush, the archiver sends an `ack_archived` command back through
Raft, which advances `archived_seq` and allows the EventStore to evict those events
from the hot tier.

Archive writes are idempotent: S3 uses ETag-based conditional writes, Postgres uses
`ON CONFLICT DO NOTHING` on `(session_id, seq)`.

### Adapter

The archive backend abstraction. Two implementations:

**S3** — stores events as NDJSON objects (one line per event, 256 events per chunk)
under a session-scoped prefix. Session metadata is a single JSON object per session.
Uses conditional PUTs with ETags for idempotency. Works with any S3-compatible
storage.

**Postgres** — stores events in a table with a `(session_id, seq)` composite primary
key. Inserts in batches of 5000 rows. Session metadata in a separate table.

## Control plane

The control plane manages cluster topology, node liveness, and operational tooling.
It does not touch the request path — it only affects routing eligibility.

### Write Nodes

Static configuration. The set of write node identities is defined at deploy time via
`STARCITE_WRITE_NODE_IDS` and cached in `persistent_term`. The group count
(`STARCITE_NUM_GROUPS`, default 256) and replication factor
(`STARCITE_WRITE_REPLICATION_FACTOR`, default 3) are also static.

Group-to-replica assignment is fully deterministic from this configuration — no
coordination needed, no rebalancing, every node computes the same answer.

### Observer

A GenServer that tracks write-node liveness via Erlang distribution events
(`:nodeup` / `:nodedown`). Each node has a status:

- **ready** — visible and eligible for routing
- **suspect** — lost visibility (nodedown event)
- **lost** — suspect for more than 120 seconds
- **draining** — operator-initiated drain (sticky, survives nodeup)

The Observer provides `route_candidates(replicas)` to the ReplicaRouter — the
intersection of observer-ready, Raft-ready, and configured write nodes.

### Ops

Operator-facing commands for cluster management. Available via `mix starcite.ops`
(development) or `bin/starcite rpc` (production releases).

- `status` — full diagnostic snapshot
- `ready-nodes` — which write nodes are currently eligible for routing
- `drain` / `undrain` — mark a node as draining (excluded from routing) or ready
- `wait-ready` / `wait-drained` — blocking gates for rolling restart scripts

Drain is non-destructive. It excludes a node from routing but doesn't remove it from
Raft groups. Undrain restores routing immediately.

### Cluster discovery

Nodes discover each other via [libcluster](https://github.com/bitwalker/libcluster).
Two strategies are supported:

- **EPMD** (static) — configured via `CLUSTER_NODES` env var. Nodes connect via
  Erlang's built-in port mapper.
- **DNS polling** (Kubernetes) — configured via `DNS_CLUSTER_QUERY` env var. Polls a
  headless service DNS record every 5 seconds.

Once connected, Erlang distribution handles the transport. The Observer subscribes to
distribution events to track liveness.

### Raft bootstrap

On startup, each write node computes which groups it should participate in (based on
the deterministic replica assignment), starts a Ra server for each local group, and
triggers leader election. A readiness probe caches consensus health (quorum
availability, leader presence) with a 3-second TTL — the node reports as not-ready
until all local groups are healthy.

## Failure modes

**Single node failure:** The remaining 2 of 3 replicas maintain quorum. Appends
continue. Leaders on the failed node are re-elected on surviving nodes within
seconds. The Observer marks the failed node as suspect, then lost after 120 seconds.
Clients see a brief latency spike during leader election, not data loss.

**Two-node failure (3-node cluster):** Quorum is lost for affected groups. Tail
replay from local state may still work on the surviving node, but new appends are
rejected. Priority: restore the failed nodes.

**Network partition:** Raft leader election prevents split-brain. A leader that can't
reach quorum steps down. Appends are unavailable on the minority side.

**Full cluster restart:** Nodes recover from their local Raft log and checkpoints. No
data is lost as long as persistent volumes survive. The bootstrap process restarts all
local Raft groups and waits for leader election before reporting ready.

**PubSub gap during leader transition:** If a tail subscriber misses a PubSub message
during a leadership change, the 5-second catchup timer detects the gap and re-enters
replay mode. Clients never silently miss events.
