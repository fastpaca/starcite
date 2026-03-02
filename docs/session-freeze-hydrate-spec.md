# Session Freeze/Hydrate Spec

Status: implemented (v1, March 2, 2026)

## Problem

Session metadata is currently retained in Raft FSM state indefinitely. Event tails
are compacted (`ack_archived` trims hot event entries), but session structs are
never evicted from `RaftFSM.state.sessions`.

That creates unbounded Raft state growth as total historical session count grows,
even when most sessions are cold.

## Goals

1. Bound hot session metadata in Raft to a configured working set.
2. Never evict sessions that are actively being written.
3. Preserve append correctness after hydration:
   producer dedupe, optimistic concurrency, and tenant/session auth checks.
4. Keep freeze policy deterministic and Raft-authoritative.
5. Keep archive writes off the append critical path.

## Non-goals

1. Global session ordering across groups.
2. Hard real-time inactivity based on wall clock.
3. Deleting archived sessions from cold storage.

## Terms

1. `hot session`: session currently present in `RaftFSM.state.sessions`.
2. `frozen session`: session evicted from Raft hot map, represented by archive
   snapshot only.
3. `drained session`: `last_seq == archived_seq` (no unarchived tail).
4. `poll epoch`: monotonic per-group counter advanced by archiver poll command.

## High-level model

1. Raft FSM is source of truth for "when a session is eligible for freeze".
2. Archiver drives polling cadence.
3. Freeze is two-phase:
   - FSM selects inactivity candidates.
   - Archiver persists snapshots, then calls guarded freeze command.
4. Hydrate is on-demand in write path:
   append misses hot map, hydrate from archive snapshot, retry append once.
5. Session lifecycle discovery (`session_created`, `session_frozen`,
   `session_hydrated`) is emitted from Raft command side-effects so stream
   subscribers observe commit-authoritative transitions.

## Policy

### Inactivity-first freeze (v1)

`v1` intentionally avoids pressure-watermark logic. A session is frozen based on
inactivity only, measured in archiver poll epochs.

1. Candidate eligibility:
   - session is drained (`last_seq == archived_seq`)
   - session has no progress for `min_idle_polls`
   - if session was hydrated recently, hydrate grace elapsed (`hydrate_grace_polls`)
2. Freeze order:
   - ascending `last_progress_poll` (oldest first)
   - tie-break by session id (ascending) for deterministic order
3. `max_freeze_batch_size` limits freezes per poll epoch.
4. Freeze runs in archiver poll cycle, not append path.

This keeps the first rollout simpler and avoids policy coupling to memory/pressure
tuning. Pressure-based trimming can be layered later as an optional mode.

### Optional pressure mode (future)

Future mode may add:

1. `hot_sessions_high_watermark` trigger.
2. `hot_sessions_low_watermark` target.
3. victim selection over the same inactivity-eligible set.

### Active write protection (hard constraints)

A session is never evicted if any of the following is true:

1. `last_seq > archived_seq` (pending unarchived writes).
2. `idle_polls < min_idle_polls`.
3. Guard mismatch at freeze time (`expected_last_seq`, `expected_archived_seq`,
   `expected_last_progress_poll`).
4. Hydrate grace window has not elapsed yet.

This prevents races where appends resume after candidate selection.

## Data model changes

### Raft FSM state

Add fields:

1. `poll_epoch` (non-negative integer)
2. per-session `last_progress_poll` (non-negative integer)

`last_progress_poll` update rules:

1. On `create_session`: set to current `poll_epoch`.
2. On append that changes `last_seq`: set to current `poll_epoch`.
3. On `ack_archived` that advances `archived_seq`: set to current `poll_epoch`.
4. On deduped append (no new seq): do not update.

### Archive session snapshot

Archive session rows/objects must include runtime fields required for safe
hydration:

1. identity:
   `id`, `tenant_id`, `title`, `creator_principal`, `metadata`, `created_at`
2. runtime:
   `last_seq`, `archived_seq`, `retention`, `producer_cursors`,
   `last_progress_poll`
3. optional metadata:
   `frozen_at_poll`, `snapshot_version`

`producer_cursors.*.hash` must be JSON-safe encoded (`base64`) and decoded back to
binary on hydrate.

## Raft command contracts

### 1) `{:eviction_tick, opts}`

Purpose:
advance per-group `poll_epoch` and return ordered freeze candidates based on
inactivity policy.

Inputs:

1. `min_idle_polls`
2. `max_freeze_batch_size`
3. `hydrate_grace_polls` (default `0`)
4. optional future pressure fields (ignored in v1)

Effects:

1. `poll_epoch += 1`
2. compute inactivity-eligible drained victims
3. reply with candidate tuples:
   `{session_id, expected_last_seq, expected_archived_seq, expected_last_progress_poll}`

Notes:

1. This command does not remove sessions.
2. Candidate selection is deterministic from FSM state.
3. v1 does not require high/low watermark checks.

### 2) `{:freeze_session, session_id, expected_last_seq, expected_archived_seq, expected_last_progress_poll}`

Purpose:
remove one hot session from Raft after snapshot persistence.

Guards:

1. session exists in hot map
2. `last_seq == expected_last_seq`
3. `archived_seq == expected_archived_seq`
4. `last_progress_poll == expected_last_progress_poll`
5. `last_seq == archived_seq`

On success:

1. remove `session_id` from `state.sessions`
2. return `{:ok, %{id: session_id}}`

On guard failure:

1. return `{:error, :freeze_conflict}` (or structured conflict reason)
2. do not mutate state

### 3) `{:hydrate_session, snapshot}`

Purpose:
rebuild a frozen session into the hot map.

Behavior:

1. if session already hot, return `{:ok, :already_hot}` (idempotent)
2. if missing, decode snapshot and insert `%Session{}` with runtime fields
3. return `{:ok, :hydrated}`

## Archiver workflow

Per local-group poll cycle:

1. Run normal archive flush flow.
2. Call `eviction_tick` on local group leader.
3. For each returned candidate:
   - read session snapshot from Raft
   - persist/update archive snapshot
   - call guarded `freeze_session`
4. Record success/failure metrics.

Design constraints:

1. Snapshot persistence must succeed before `freeze_session`.
2. `freeze_session` is best-effort: conflicts are normal under races.
3. Failures must not crash append path.

## Write path hydration flow

For append/append_events:

1. attempt normal Raft append
2. if `:session_not_found`:
   - load snapshot from archive
   - issue `hydrate_session`
   - retry original append exactly once
3. if hydrate fails due archive unavailability, return unavailable error.

Hot-path impact note:

1. Normal append path is unchanged for hot sessions.
2. Extra archive + hydrate work only occurs on cold session misses.

## Read/auth behavior

1. `GET /sessions` remains archive-backed.
2. Auth checks that read session metadata can keep using `ReadPath.get_session/1`
   (`SessionStore` archive read-through).
3. On hot miss during append, hydration makes Raft authoritative again before write.

## Adapter and schema changes

### Postgres

1. Add runtime columns to `sessions` table:
   `last_seq`, `archived_seq`, `retention`, `producer_cursors`,
   `last_progress_poll`, `snapshot_version`.
2. Replace create-only upsert semantics with monotonic update semantics:
   update only when incoming snapshot is newer (at minimum by `last_seq`,
   secondarily by `last_progress_poll`).

### S3

1. Extend session JSON object with runtime snapshot fields.
2. Replace write-once session put behavior with conditional update behavior
   (read/merge/write with version guard, similar to event chunk conflict handling).

## Telemetry

Required events/counters:

1. `session_eviction_tick_total`
2. `session_freeze_candidate_total`
3. `session_freeze_success_total`
4. `session_freeze_conflict_total`
5. `session_freeze_error_total`
6. `session_hydrate_attempt_total`
7. `session_hydrate_success_total`
8. `session_hydrate_error_total`
9. gauges:
   `hot_sessions`

## Rollout plan

1. Add archive snapshot fields and backward-compatible read/write support.
2. Add `hydrate_session` command and append hydrate-retry.
3. Add `eviction_tick` and `freeze_session` commands, disabled by config flag.
4. Enable metrics-only eviction tick first (no freeze calls).
5. Enable inactivity freeze in canary (`min_idle_polls` high, small batch size).
6. Ramp cluster-wide after stable hydrate latency and low conflict rate.
7. Revisit pressure mode only if hot-session growth still exceeds operational bounds.

## Configuration

1. `session_freeze_enabled` (default `false`)
2. `session_freeze_min_idle_polls` (default `3`)
3. `session_freeze_max_batch_size` (default `50`)
4. `session_freeze_hydrate_grace_polls` (default `0`)
5. optional future pressure fields:
   `session_hot_high_watermark`, `session_hot_low_watermark`

## Implemented decisions

1. `eviction_tick` returns ids + guards (+ `hot_sessions`), not full session
   snapshots.
2. `append_events` hydrates once and retries the whole batch once.
3. Snapshot monotonic conflict resolution in Postgres/S3 uses runtime tuple
   ordering: `{last_seq, last_progress_poll, archived_seq}`.
4. Session discovery stream events are emitted from Raft side-effects on create,
   freeze, and hydrate.

## Remaining question

1. Should hydrate grace stay fixed or become adaptive (for example based on
   observed archive latency percentiles)?
