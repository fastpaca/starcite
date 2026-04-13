# Rust Parity Rewrite Target

The current `rust/starcite-rs` crate is a useful experiment, but it is not a
faithful rewrite of Starcite's low-latency architecture. It moved Postgres onto
the append ack path and collapsed the tiered storage model into one database.
That is a different system, and it should not be treated as the target shape
for a production rewrite.

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

## What The Current Rust Experiment Got Wrong

- It made Postgres the primary append log.
- It made replay a direct database read instead of a tiered read path.
- It replaced quorum replication with database durability.
- It replaced async archive semantics with one durable store.
- It preserved API shape in many places while changing the latency model under
  it.

That experiment can still be useful for:

- typed request/response contracts
- WebSocket framing and Phoenix transport compatibility
- auth boundary cleanup
- ops surfaces and telemetry plumbing

It should not be mistaken for the final rewrite direction.

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

For low-tail performance discussions, treat `rust/starcite-rs` as a discarded
architecture experiment until it starts preserving the existing write/read
boundaries instead of replacing them.
