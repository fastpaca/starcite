# Starcite Realtime Coordination Contract (Internal)

Status: Draft v0.1  
Visibility: Internal only

## Product Identity
Starcite is a realtime coordination plane for AI agent systems.

Starcite guarantees ordered, resumable session streams for participants such as agents, UIs, tools, and voice components. Storage is pluggable and asynchronous. Delivery is the product.

Starcite is not a general-purpose database.

## Control-Plane / Data-Plane Split
Starcite uses consensus for control-plane coordination, not per-event payload commit.

Control plane:
- Ownership and fencing (`epoch`) for writable session shards.
- Routing metadata publication.
- Node lifecycle coordination (drain, add, remove).
- Consensus-backed durable frontier publication (`committed_cursor` / current `archived_seq` shape).
- One Raft-backed shard lease group per routing shard; these groups elect leaders but do not store session payload state.

Data plane:
- Owner-assigned `(epoch, seq)` event ordering.
- In-memory replication quorum for fast ack.
- Async fan-out and async archival.

Non-goal for control-plane consensus:
- Per-event payload consensus commit on append hot path.

## Core Contract
1. Ack means replicated to in-memory quorum (`2/3` replicas) for the session group.
2. Ack does not mean fsync and does not mean durable commit.
3. Every event is identified by `session_id`, `epoch`, and `seq`.
4. `seq` is monotonic within a session on the active lineage.
5. Clients reconnect using cursor tuple `(epoch, seq)`.
6. System exposes `committed_cursor` per session, meaning the highest cursor durably archived to configured storage.
7. Current implementation tracks this frontier as `archived_seq` (integer `seq` only). Target external shape is `(epoch, seq)`.
8. Events above `committed_cursor` may be lost under correlated failures.
9. Any discontinuity is explicit via a `gap` signal; discontinuities are never silent.

## Terms
- `session`: Ordered stream boundary.
- `epoch`: Leader lineage/version for a session group.
- `seq`: Monotonic sequence number assigned by leader.
- `cursor`: `(epoch, seq)` pair used for resume.
- `head_cursor`: Highest delivered cursor currently known.
- `committed_cursor`: Highest durably archived cursor.
- `earliest_available_cursor`: Oldest cursor currently replayable from Starcite hot tier.
- `archived_seq`: Current internal durable frontier (`seq` only) used before full epoch-aware cursor migration.

## Write Path Semantics
1. Client appends event to a session.
2. Active owner is validated against control-plane ownership and current fencing epoch.
3. Owner assigns `(epoch, seq)`.
4. Event replicates to in-memory quorum.
5. Ack is returned to client.
6. Event is fanned out to subscribers.
7. Event is archived asynchronously in the background.
8. `committed_cursor` advances after storage confirmation.

## Epoch Semantics
1. `epoch` represents writer lineage and should map directly to Ra leader term for the owning group.
2. `epoch` increments on each successful leader election or term change, including clean failovers.
3. Clean failover with no lineage break should not emit a gap if requested cursor remains on active lineage.
4. `rollback` gap reason is required when requested cursor is no longer on active lineage after term change.
5. `epoch_stale` indicates cursor references an older lineage context and requires server-provided resume cursor.

## Read and Resume Semantics
1. Tail connection may start live-only or from a provided cursor.
2. If cursor is valid and available, replay starts from the next event after that cursor.
3. If cursor is unavailable or not on active lineage, server emits explicit `gap` details and the next valid cursor.
4. Client resumes from server-provided next cursor.

## Discontinuity and Gap Rules
Gap reasons:
- `cursor_expired`: requested cursor is below retention floor.
- `rollback`: requested cursor is not on active lineage after failover or rollback.
- `epoch_stale`: requested epoch is stale relative to active writer lineage.

Gap signal must include:
- reason
- requested cursor (`from_cursor`)
- next valid resume cursor (`next_cursor`)
- current `committed_cursor`
- current `earliest_available_cursor`

## Durability Model
1. Durability is asynchronous and backend-dependent.
2. `committed_cursor` is the durable frontier for a session.
3. Bounded loss window includes all events greater than `committed_cursor` at failure time.
4. Bounded loss window size is influenced by:
- archiver flush interval
- storage backend write latency
- background backlog depth

Default target:
- Flush interval baseline: 5 seconds (subject to configuration and load).

Watermark publication:
- The control plane persists watermark advancement metadata with monotonic semantics.
- Watermark updates must never regress.

## Availability Model
Session groups run with configurable replication factor (`RF`, default `3`) and quorum `floor(RF/2) + 1`.

Guarantees:
- Up to `RF - quorum` replica losses in a group remain writable.
- Correlated failures can make a subset of groups unavailable.

Operational example (non-contractual):
- With 10 nodes, RF=3, and 2 failed nodes, expected unavailable groups are approximately 6.67 percent.

Control-plane note:
- Control-plane consensus availability governs ownership/routing transitions.
- Data-plane append availability additionally depends on active owner and replica health for the session group.

## Control-Plane Responsibilities
Consensus-backed responsibilities:
1. Membership state for writable nodes.
2. Ownership map for session shards (or equivalent routing partition).
3. Fencing epoch issuance and validation on owner changes.
4. Drain coordination state.
5. Watermark (`committed_cursor` / `archived_seq`) publication metadata.
6. Routing table publication for ingress and tail services.

Non-consensus data-plane responsibilities:
1. Per-event append processing.
2. Per-event in-memory replication and ack.
3. PubSub fan-out.
4. Archive batching and write execution.

## Operational Lifecycle Flows
Node drain flow:
1. Mark node as `draining` in control plane.
2. Stop assigning new ownership to draining node.
3. Transfer ownership off draining node in bounded batches.
4. Bump epoch for each transferred ownership domain.
5. Verify in-flight activity and replication lag are below thresholds.
6. Mark node as `drained`.

Node add flow:
1. Add node to control-plane membership.
2. Mark node as `joining` until health checks pass.
3. Include node in rebalancing plans.
4. Publish updated routing after ownership transitions commit.

Node remove flow:
1. Require node `drained` state before removal unless emergency procedure is invoked.
2. Remove node from control-plane membership.
3. Publish updated routing map and ownership state.

## Ordering and Delivery Model
1. Delivery is at-least-once on active lineage, subject to bounded-loss behavior above `committed_cursor` in correlated failures.
2. Ordering is strict by `(epoch, seq)` on active lineage.
3. Clients must deduplicate by stable event identity.
4. Clients must treat cursor lineage changes as authoritative when a gap is emitted.

## Idempotency Contract
Append requests must support idempotent retry using producer identity and producer sequence.

Recommended identity tuple:
- `session_id`
- `producer_id`
- `producer_seq`

Current behavior:
1. Same producer tuple and same event fingerprint: return prior success as duplicate.
2. Same producer tuple and different event fingerprint: reject as replay conflict.
3. Fingerprint is hash-based (not byte-for-byte payload comparison), so collision risk is non-zero and accepted as an implementation tradeoff.

Target behavior:
1. Keep compatibility with current hash-based dedupe until exact-match semantics are explicitly introduced.

## API Surface (Contract-Level)
Session lifecycle and stream primitives remain:
1. Session create.
2. Session append.
3. Session tail with optional resume cursor.

Transport details, field naming, and wire format are implementation details as long as the core contract above is preserved.

## SDK Behavior Contract (No Implementation)
Any official client SDK must provide:
1. Session creation.
2. Idempotent append with producer identity support.
3. Tail subscription with resume cursor support.
4. Automatic reconnect with backoff and resume from last processed cursor.
5. Gap callback with reason and next cursor.
6. Access to `head_cursor`, `committed_cursor`, and `earliest_available_cursor`.
7. Optional wait-for-commit helper that blocks until `committed_cursor` reaches or exceeds a target cursor.

## Backpressure Model
1. Backpressure scope is node-local memory pressure in the hot event store.
2. Current write-path behavior primarily emits telemetry when memory pressure exceeds limits and cache reclaim cannot recover headroom.
3. If write APIs surface explicit `event_store_backpressure`, clients must treat it as a hard reject and retry with bounded jittered backoff.
4. Backpressure is not currently a per-session fairness guarantee; producers should self-throttle when repeated backpressure signals occur.

## Error Semantics
Clients must receive explicit error classes for:
1. Invalid request shape.
2. Authentication or authorization failure.
3. Session not found.
4. Idempotency conflict.
5. Cursor unavailable due to retention or lineage change.
6. Backpressure or throttling.
7. Group unavailable or no active leader.

## SLO Targets (Initial)
1. Delivery latency target: single-digit milliseconds in-region under normal load.
2. Resume correctness: explicit gap signaling, no silent stream discontinuities.
3. Availability: per-group tolerance of one replica loss.
4. Durability: asynchronous archival with observable commit frontier.

## Non-Goals
1. General-purpose database queries and indexing.
2. Topic-based broker semantics outside session-scoped ordered streams.
3. Claiming ack-level durable commit in fast-path operation.

## Messaging Guidance (Internal)
Use this external framing:
- Ordered realtime delivery.
- Cursor resume with explicit discontinuity handling.
- Bounded loss window above durable frontier.
- Pluggable storage archival.

Avoid this external framing:
- Never loses messages.
- Durable-on-ack behavior.
- General-purpose database replacement.
