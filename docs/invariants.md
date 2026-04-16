# Starcite Invariants

This document is the explicit invariant inventory for the current Khepri control plane
plus batched `SessionLog` data plane.

## Purpose
- Define the states that must never be violated.
- Anchor each invariant to the current runtime behavior.
- Make it obvious which guarantees are contractual, which are implementation details,
  and which gaps still need coverage.

## Control-Plane Invariants

### Node lifecycle
- A node in `:draining` must not receive new ownership claims.
- A node in `:drained` must not report locally ready.
- Restart must not move a node from `:draining` or `:drained` to `:ready` unless
  drain work is actually complete.
- `nodedown` is not authoritative for ownership movement.
- Lease expiry is authoritative for catastrophic failover.

### Claim placement
- New claims must only consider nodes that are both:
  - `status == :ready`
  - lease not expired
- New claims must exclude draining nodes.
- New claims must exclude ready-but-expired nodes.
- New claims must stay evenly balanced across the eligible ready set.
- New claims must continue to exclude an expired owner while that owner's existing
  sessions are concurrently being failed over.
- Concurrent create traffic must remain available while lease-expiry failover is
  reassigning old owners.

### Transfer state
- `:moving` is a per-session transfer state, not a node state.
- `:moving` may only exist while an ownership transfer is in progress.
- `commit_transfer` or failover must normalize a session back to:
  - `status: :active`
  - incremented `epoch`
  - cleared `target_owner`
  - cleared `transfer_id`
- Manual `undrain` must be rejected while:
  - `active_owned_sessions > 0`, or
  - `moving_sessions > 0`

## Data-Plane Invariants

### Ownership fencing
- A stale former owner must reject appends after authoritative ownership moves.
- A stale former owner must also reject appends while the session is still `:moving`.
- Followers must never accept owner append commands.
- Ownership movement must fence writes by `epoch`, not by local liveness hints.
- Concurrent append traffic must remain available while lease-expiry failover is
  reassigning an expired owner.

### Sequence and ordering
- `seq` is monotonic within an active `(session_id, epoch)` lineage.
- Successful appends produce contiguous visible sequence numbers.
- `expected_seq` requests act as ordering barriers.
- Producer replay conflicts must not create duplicate visible events.

### Ack visibility contract
- If replication quorum is not met, append must not leak into session state.
- If replication quorum is not met, append must not leak into replay.
- If replication quorum is not met, append must not leak into cursor snapshot.
- Crash before quorum must not create a ghost write.
- Crash after quorum but before reply must not create a duplicate visible write on retry.

### Replica monotonicity
- Lower-epoch replica state must never overwrite higher-epoch state.
- Same-epoch replica application may move forward, but must never regress:
  - `last_seq`
  - `archived_seq`
- Replica convergence is monotonic.

### Archive watermark
- `archived_seq` / committed frontier must never regress.
- Repeated archive acks at the same cursor are idempotent.
- Archive ack above `last_seq` clamps to `last_seq`.
- Hot event eviction may only move forward with the archive frontier.

## Read / Resume Invariants

### Tail visibility
- If a committed event is delivered on the embedded cursor-update fast path, tail must
  not need replay fallback to surface it.
- Replay queue and live buffer must remain explicit; no silent loss or skip is allowed.

### Gap semantics
- Resume discontinuity must be explicit via a `gap` frame.
- Public gap reasons are:
  - `cursor_expired`
  - `resume_invalidated`
- Internal causes currently include stale-epoch resumes and rollback-style invalidation.
- Gap metadata must include:
  - `from_cursor`
  - `next_cursor`
  - `committed_cursor`
  - `earliest_available_cursor`

## Security Invariants

### Session and tenant isolation
- A token locked to session `Y` must never append to session `X`.
- Cross-tenant append must never succeed.
- Owner-side append authorization must fence tenant mismatch even if ingress skips a
  pre-read for performance.

## Operational Invariants

### Shutdown and restart
- Shutdown drain must emit explicit lifecycle telemetry.
- Restart must not silently reintroduce an incompletely drained node into the ready set.
- Mixed state after restart must be visible as `:draining` / `:drained`, never faked
  as `:ready`.

### Audit telemetry
- Transfer start and commit must emit counters.
- Lease-expiry failover must emit counters.
- Stale-owner / transfer fences must emit counters.
- Impossible routing-state mutations must emit counters, including:
  - assignment update with a regressed epoch
  - transfer commit against a non-moving assignment
  - failover against a non-ready target
- Publication-time watermark regression must emit a counter and preserve the higher
  committed frontier instead of regressing it locally.
- Append boundary telemetry must exist at the two critical crash boundaries:
  - `before_quorum_replicate`
  - `after_commit_before_reply`

## Known Gaps
- No long-running multi-hour soak with rolling restart/drain/create/append/tail invariants.
- No full websocket tail consumer in the rolling drain/restart chaos drill yet; the current
  short drill proves internal live cursor delivery, final `last_seq` monotonicity,
  and latest-event hot visibility from the active owner.

## Rule
If a property is important enough to mention in the contract or architecture docs, it
should have either:
- an executable test, or
- a named known-gap entry here explaining why it does not yet.
