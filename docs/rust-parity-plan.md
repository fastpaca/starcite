# Rust Parity Rewrite Target

This document defines the parity bar for the Rust rewrite. Earlier versions of
`rust/starcite-rs` drifted into a different architecture, and future changes can
drift again unless the target is pinned explicitly.

This document defines the actual parity target for a Rust rewrite of the
existing Starcite app.

## Non-Negotiable Properties

- Append ack stays on the replicated hot path, not a synchronous Postgres
  transaction.
- Session ownership remains explicit and routed. One node owns the hot runtime
  for a session epoch, and replicas back it.
- Replay remains tiered: hot local state first, archived storage second.
- Archive persistence remains asynchronous and non-blocking for append ack.
- Cursor and gap semantics stay compatible with the existing read path:
  `cursor_expired`, `epoch_stale`, and `rollback`.
- Live tail remains replay-then-live over one connection.

If a Rust implementation cannot preserve those properties, it is not a rewrite.
It is a redesign.

## Postgres Is Fine, But Only As The Backend Store

The latency requirement does not rule Postgres out. It rules Postgres out of
the append ack path.

The intended Rust shape is:

- quorum-committed writes land in the replicated hot session log first
- the hot runtime and hot event store serve the recent replay path
- a background flusher persists committed state to Postgres asynchronously
- cold replay and recovery load from Postgres
- S3 can disappear entirely if Postgres replaces the archive store

That keeps the operational simplification, "one backend store instead of S3 +
Postgres," without paying synchronous database latency on every append ack.

If the cluster cannot acknowledge a write before Postgres commits it, the
rewrite has already lost the main performance property it was supposed to
preserve.

## Existing Elixir Shape To Preserve

### Session Quorum

The write boundary today is [session_quorum.ex](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/lib/starcite/data_plane/session_quorum.ex).
That module is not just a router. It owns the latency-sensitive behavior:

- route commands to the local owner runtime
- bootstrap or restart runtimes when routing epochs change
- replicate committed session state to standby logs
- wait for quorum before acknowledging writes

This boundary is the thing the Rust rewrite must mirror first.

### Local Event Store

The hot read/write store today is [event_store.ex](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/lib/starcite/data_plane/event_store.ex).
It is explicitly tiered:

- unarchived events live in local in-memory `EventQueue`
- archived reads come from `EventArchive`
- cached archived reads share the same capacity budget

That means the hot path is not "Postgres as the log." It is "local session log
plus archived backing store."

### Read Path

The compatibility contract for replay is in [read_path.ex](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/lib/starcite/read_path.ex).
Important behaviors to preserve:

- routed session reads
- cursor normalization with epoch-aware cursors
- replay from hot state, then cold archived reads when needed
- explicit gap signaling with `cursor_expired`, `epoch_stale`, and `rollback`
- gap-free merged replay ordering

Any Rust tail or replay implementation that drops those semantics is not a
parity rewrite.

### Archive

Archive is intentionally off the write ack path in [archive.ex](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/lib/starcite/archive.ex).
The current flow is:

- periodically scan local event-store session cursors
- persist batches through the archive store
- acknowledge archive progress back to the local session log

That separation is part of the latency story. Moving archive durability into the
append transaction defeats the original design.

For the Rust parity target, the archive sink can be Postgres instead of S3, but
the queueing model should stay the same: flush committed hot-path state to the
backend store after the ack, not before it.

## Remaining Drift To Watch

The current Rust branch has moved closer to parity, but there are still real
boundaries to protect:

- the hot path is still a narrow `local_async` shape with one synchronous
  standby chosen by the Postgres control plane, not the old broader routed
  quorum topology
- transport/auth/ops parity is ahead of routing/replication/archive telemetry
  parity
- external IdP interoperability is still only covered by the in-process JWKS
  harness

Those are deliberate current limits. They should stay visible instead of being
accidentally normalized into "good enough parity."

## Rust Rewrite Sequence

The next Rust work should follow this order:

1. Model the current session/runtime/log contracts in Rust.
2. Build a local in-memory hot event store with the same replay semantics.
3. Build the routed quorum boundary around per-session runtimes.
4. Add async archive flush to Postgres without putting it on append ack.
5. Recreate read-path gap semantics exactly.
6. Reuse or adapt the current Rust transport/auth/ops work only where it does
   not alter the hot-path architecture.

## Practical Rule For This Branch

For low-tail performance discussions, judge `rust/starcite-rs` by whether it is
preserving the write/read boundaries above, not by whether it happens to keep a
similar API shape while moving durability or replay back onto the wrong path.
