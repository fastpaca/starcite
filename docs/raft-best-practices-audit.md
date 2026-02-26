# Raft (Ra) Best Practices Audit

Cross-reference of Starcite's Ra usage against RabbitMQ/Khepri lessons learned
and Ra library best practices. Audit performed 2026-02-25. Fixes verified
2026-02-25 against `fix-raft-failures` branch.

Sources: Ra internals docs, Ra state machine tutorial, Ra SNAPSHOTS.md,
RabbitMQ quorum queue docs, Khepri failure recovery docs, Ra GitHub issues
(#179, #228), rabbitmq-server#3776, RabbitMQ disk performance blog.

---

## Critical: EventStore writes inside `apply/3` break determinism

**Status: Fixed** in `fix-raft-failures`.

`RaftFSM.apply/3` called `EventStore.put_event` / `put_events` (ETS writes)
inside the `with` chain for `:append_event` and `:append_events`. The result
gated whether state was updated.

`EventStore` enforces memory backpressure via `event_store_max_bytes`. If one
replica was under more memory pressure than another, `put_event` returned
`:event_store_backpressure` on that node but succeeded on others. The `with`
chain returned `{state, {:reply, {:error, reason}}}` (unchanged state) on the
failing node but `{new_state, {:reply, {:ok, reply}}}` (updated state) on
the others.

**This violated Raft's fundamental invariant:** all replicas must converge to
the same state given the same log. Once diverged, the cluster was silently
corrupted — snapshots persisted the divergent state.

### How it was fixed

Option 2 was implemented: ETS writes are now unconditional inside `apply/3`.
`EventStore.put_event/2` and `put_events/2` always return `:ok`. Under memory
pressure they emit backpressure telemetry but never reject the write. The
`with` chain no longer gates on ETS write success — writes happen after the
session state computation as an unconditional assertion (`:ok = put_appended_event(...)`).

### Context for future reference

Ra's `apply/3` must be deterministic — no external I/O, no local-state-
dependent branching. Any side effect in `apply/3` that can produce different
results on different replicas will cause silent state divergence.

---

## High: Unbounded Raft log growth when archiver stalls

**Status: Fixed** in `fix-raft-failures`.

`release_cursor` only fired when `ack_archived` advanced `archived_seq`. If
the archiver stalled (S3 outage, Postgres down, flusher crash), no
`ack_archived` commands arrived, no `release_cursor` effects fired, and the
WAL/segment files grew without bound.

RabbitMQ hit this in production (rabbitmq-server#3776): quorum queues had
unbounded log growth when consumer acknowledgments stalled.

### How it was fixed

Periodic `{:checkpoint, raft_index, state}` effects fire every 2,048 entries
(configurable via `raft_checkpoint_interval_entries`). A `last_checkpoint_index`
field in the FSM struct tracks progress. Every `apply/3` clause routes through
`reply_with_optional_effects/4` which emits a checkpoint when the interval is
reached. This is command-driven rather than timer-driven (`tick/2`), but
achieves the same goal: log compaction proceeds regardless of archiver state.

### WAL as shared bottleneck (operational note)

The WAL is shared across all 256 Raft groups on a node. Current config:

```elixir
wal_write_strategy: :o_sync,
low_priority_commands_flush_size: 1_024,  # 40x default (25)
wal_max_batch_size: 262_144,
default_max_pipeline_count: 16_384,
```

Ra recommends separating WAL storage to a dedicated disk via `wal_data_dir`.
If the WAL shares a disk with segment files and snapshots, `fsync` latency
under write load directly throttles all 256 groups. Consider dedicated NVMe
for WAL in production.

Default `wal_max_size_bytes` is 512 MB. Allocate at least 3x WAL size limit
in node memory for WAL-related allocations (Ra accumulates entries in memory
until flush, then drops sharply).

---

## High: No rolling upgrade support for the state machine

**Status: Fixed** in `fix-raft-failures`.

`RaftFSM` implemented neither `version/0` nor `which_module/1`. Ra's state
machine versioning is the only supported path for changing FSM logic in a
running cluster. Without it, any change to `apply/3` required full cluster
downtime.

### How it was fixed

- `version/0` returns `1` (the checkpoint field bumped from v0).
- `which_module/1` maps versions 0 and 1 to `RaftFSM`.
- `apply/3` handles `{:machine_version, 0, 1}` by calling `ensure_state_schema/1`
  which backfills `last_checkpoint_index` on legacy v0 state structs.
- Tests verify version callbacks and v0->v1 migration.

New versions activate when a quorum has the new code and one is elected leader.

---

## High: No `trigger_election` after restart

**Status: Fixed** in `fix-raft-failures`.

No calls to `:ra.trigger_election/1` existed. Ra issue #179: after restarting
a node, Ra may not automatically trigger an election. Groups sat in
follower/candidate state with no leader; `ra:process_command` returned
`{:timeout, ...}` indefinitely.

### How it was fixed

Two paths now trigger elections:

1. **At startup:** Both coordinator and follower paths call
   `ensure_local_group_leaders/1` after starting/joining groups. This probes
   each group via `:ra.member_overview/2` and calls `:ra.trigger_election/2`
   for any group with no known leader.

2. **Periodic reconciliation:** A 5-second `:runtime_reconcile` timer detects
   and restarts down groups, then calls `ensure_local_group_leaders/1` on
   recovered groups. This covers groups that lose their leader at runtime.

`trigger_election` is safe to call even if an election is already in progress —
Ra's pre-vote mechanism prevents disruption of stable leaders.

---

## High: Checksums disabled

**Status: Fixed** in `fix-raft-failures`.

```elixir
# config/config.exs — before
wal_compute_checksums: false,
segment_compute_checksums: false,

# config/config.exs — after
wal_compute_checksums: true,
segment_compute_checksums: true,
```

Both WAL and segment checksums are now enabled. CRC32 cost is minimal compared
to the risk of undetectable corruption in a consensus system.

---

## Medium: PubSub effects unreliable during leadership transitions

**Status: Fixed** in `exp/raft-best-practices`.

`mod_call` effects for PubSub broadcasts are leader-only. Ra's effect
guarantees:

- Effects may execute multiple times if all servers crash simultaneously.
- Effects may never execute during leadership transitions.
- `send_msg` uses `erlang:send/3` with `no_connect` and `no_suspend` flags
  (unreliable delivery).

For tail subscribers this meant events could be missed during leader
transitions. The cursor-based reconnect in `tail` mitigated this for
reconnecting clients, but connected clients had a liveness gap.

### How it was fixed

`TailSocket` now runs a periodic catch-up timer (`@catchup_interval_ms`,
5 seconds). Every tick it calls `EventStore.max_seq(session_id)` — a cheap
ETS lookup — and if events exist beyond the current cursor, it re-enters
replay mode (`replay_done: false`) and schedules a drain. This covers the
gap where a PubSub broadcast was lost during leadership transition without
adding per-append overhead. The normal push path via PubSub remains the
primary delivery mechanism; the timer is a fallback.

---

## Medium: Pipeline command ambiguity on timeout

**Status: Mitigated.** All command types are idempotent; no code change needed.

`RaftPipelineClient` (`raft_pipeline_client.ex`): on timeout, the client
returns `{:timeout, {server_id, node}}`. The caller cannot distinguish:

- Command committed but ack lost (safe to read, dangerous to retry without
  idempotency).
- Command never reached quorum (safe to retry).

Ra issue #228: pipeline commands could be silently dropped during leadership
transfer without sending a rejection event.

### Why no code change is needed

All Starcite command types are safe to retry on timeout:

- **`append_event`/`append_events`**: Producer dedup (`ProducerIndex`) makes
  retries idempotent. Same `(producer_id, producer_seq)` returns the existing
  `session_seq`.
- **`create_session`**: Returns `{:error, :session_exists}` on duplicate —
  idempotent by design.
- **`ack_archived`**: Advancing `archived_seq` past its current value is a
  no-op — idempotent by design.

The `WritePath` already retries on pipeline timeout via
`process_command_on_node_fallback` (synchronous `ra.process_command`).

Clients without producer IDs may observe duplicate appends on timeout-then-
retry. This is documented behavior — producer dedup is opt-in.

---

## Medium: No `state_enter/2` callback

**Status: Fixed** in `exp/raft-best-practices`.

Ra calls `state_enter/2` on server state transitions (leader, follower,
candidate, etc.). This is the place to:

- Re-register process monitors (invalidated on leadership changes).
- Initialize leader-specific resources.
- Clean up when stepping down.

### How it was fixed

No-op implementation added to `RaftFSM`:

```elixir
@impl true
def state_enter(_ra_state, _state), do: []
```

Returns empty effects list. Ready for future use when leadership transition
hooks are needed (e.g., re-registering monitors, emitting telemetry).

---

## Informational: Ra failure detection latency

Ra uses Aten (probabilistic failure detector) with default 5-second poll
interval. Hard node failures (kill -9, network drops) take ~12 seconds to
detect. Graceful shutdowns trigger Erlang process monitors immediately
(<1 second).

During the detection window, clients hitting the minority partition experience
2-second timeouts (our `raft_command_timeout_ms`). The routing layer retries
on other replicas, which is correct. No action needed, but ops should be aware
of the ~12-second window.

---

## Informational: Ra pre-vote prevents election storms

Ra inserts a pre-vote state between follower and candidate. Before starting a
real election, a node checks whether it could actually win. This prevents
isolated nodes from rejoining with higher terms and disrupting stable leaders.
No action needed — this is built into Ra.

---

## Decided: Not addressing

### In-memory session growth in ETS

By design. Sessions live in the `sessions` map in `RaftFSM` state. Once the
archive flushes and `ack_archived` fires, events are evicted from the hot
cache. The `event_store_max_bytes` limit provides the memory ceiling. Session
*metadata* stays in memory permanently but is small per session.

### Static membership / no dynamic node replacement

By design for operational simplicity. Dynamic membership reconciliation (like
RabbitMQ's `continuous_membership_reconciliation`) adds significant debugging
complexity. Node replacement is a manual ops procedure: update
`STARCITE_WRITE_NODE_IDS`, rolling restart. Ra enforces one membership change
at a time per cluster, so replacing a node across 256 groups requires
sequential operations — acceptable for planned maintenance.

### Readiness probe iterates all local groups

Acceptable at 256 groups with 3-second TTL cache. `:ra.key_metrics/1` and
`:ra_leaderboard.lookup_leader/1` are ETS lookups, not network calls. Cost is
negligible under normal conditions.
