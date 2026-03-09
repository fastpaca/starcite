# Architecture

Starcite is a clustered session-stream service. Each session is an ordered,
append-only event stream with cursor-based replay and low-latency live delivery.

![Starcite Architecture](img/architecture.png)

The system has three layers: **edge**, **data plane**, and **control plane**.

## Edge

The edge handles client protocol, auth, and policy enforcement.

**Session API** - REST endpoints for create/append/list. Validates payloads,
enforces tenant/session scope, and delegates to the data plane.

**Tailer** - WebSocket handler for `/tail`. Replays historical events from cursor,
then streams live updates. Buffers live updates during replay and flushes when
replay completes.

**Auth** - Validates JWT bearer tokens against JWKS and derives principal context
(tenant, scopes, subject/session constraints) for policy checks.

## Data plane

The data plane owns session sequencing, replay semantics, hot event retention, and
async archival.

### Session owners

One GenServer owner process exists per active session on a node.

Owner responsibilities:
- Assign monotonic `seq` per session.
- Enforce producer dedupe and optimistic concurrency (`expected_seq`).
- Write committed events to hot event store.
- Broadcast cursor updates to PubSub for tail subscribers.
- Track archive progress (`archived_seq`) and apply archive acknowledgements.

The append hot path is owner-based and does not call Raft.

### Write path

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API
    participant O as SessionOwner
    participant E as EventStore
    participant P as PubSub

    C->>A: POST /append
    A->>O: append_event
    O->>O: assign seq + dedupe checks
    O->>E: store committed event
    O->>P: broadcast cursor update
    O-->>A: {seq, cursor, committed_cursor}
    A-->>C: 201
```

### Read path (tail)

Tail has two phases:

**Replay** - reads events where `seq > cursor` from hot store and cold archive,
merges, deduplicates, and verifies gap-free order.

**Live** - subscribes to cursor-update PubSub and pushes new events as they arrive.

Cursor contract:
- Cursor includes `(epoch, seq)`.
- Reconnects are resumable by cursor.
- If replay continuity cannot be guaranteed, server emits explicit `gap` frames
  with `reason`, `from_cursor`, and `next_cursor`.

### Event store and session store

**Event store** - in-memory hot storage for unarchived events plus archived-read
cache for replay acceleration.

**Session store** - cache of session metadata/state used by API/auth/read paths and
owner rehydration after owner restarts.

### Archiver

```mermaid
graph LR
    E["Event Store (hot)"] -- "pull pending events" --> AR["Archiver"]
    AR -- "write batch" --> S["S3 / Postgres"]
    AR -- "ack_archived" --> O["Session Owner"]
```

A background worker periodically flushes committed hot events to archive storage.
After a successful flush, it calls archive ack on the session owner, which advances
`archived_seq` and evicts archived hot entries.

Archive writes are idempotent. Archiver failures do not block append/tail hot paths.

## Control plane

The control plane manages cluster lifecycle/readiness and operator workflows.
It does not participate in append sequencing.

Current responsibilities:
- Static write-node topology configuration.
- Node liveness and drain/undrain intent.
- Raft runtime/bootstrap/readiness management for control-plane operations.
- Cluster status and operational tooling.

Raft remains in the system as control-plane/lifecycle infrastructure, but it is not
on the append/read hot path.

## Failure modes

**Session owner crash:** Owner process restarts and rehydrates from session cache.
Short blips may return temporary timeout; retries succeed after restart.

**Node failure:** Sessions owned on that node become unavailable until clients
reconnect/reroute to available capacity based on deployment topology.

**Archive lag/failure:** Live delivery continues; archival watermark stops advancing
until storage recovers.

**Missed PubSub update:** Tail catchup timer re-enters replay when hot store cursor
is ahead of socket cursor.
