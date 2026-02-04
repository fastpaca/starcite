# CLAUDE.md

Guidance for Claude Code when collaborating on FleetLM.

## Quick Start

- `mix setup` - bootstrap deps, DB, and assets
- `mix phx.server` - run the dev server
- `mix test` / `mix test path/to/file.exs` - execute test suites
- `mix precommit` - final gate before handing work back (compile + format + tests)

## Architecture Cheat Sheet

- **Storage:** 256 Raft groups (Ra library) with 3 replicas each. 2-of-3 quorum writes, automatic leader election, split-brain protection built-in.
- **No per-session processes:** Completely stateless request handling. Raft state machines are the only long-lived processes.
- **Conversation metadata in RAM:** Last sequence number cached in Raft state-no DB queries on message append.
- **Postgres as write-behind:** Background flusher streams Raft state to DB every 5s. Batched, idempotent, non-blocking. DB failures don't stop appends.
- **Cluster membership:** Erlang distribution (Node.list) provides strongly-consistent cluster view. Coordinator pattern (lowest node ID) manages topology. Rendezvous hashing assigns replicas deterministically. Continuous reconciliation handles node joins/leaves.
- **Agent dispatch:** ETS queue + polling engine pattern. Webhooks via Finch (HTTP/2 pooling), JSONL streaming back to Raft.
- **Inbox model:** One inbox per user, subscribed to metadata deltas. Many sessions per user, subscribed to raw message streams.

## Working Standards

- Pattern-match required input (controller params, channel payloads, webhook data); return descriptive errors when the shape is wrong. Do not mask missing keys with `Map.get(..., default)`.
- Make telemetry strict. Add clauses like `defp message_tags(%{role: role})` and treat everything else as the fallback path that highlights anomalies.
- When serialising structs/maps, destructure once and build the response. Avoid peppering `Map.get` across atom/string variants-normalise at the boundary.
- Use the shared agent engine for outbound HTTP. It uses Finch with HTTP/2 connection pooling-avoid reintroducing ad-hoc HTTP clients on the hot path.
- Lint, format, and test locally. Every PR should pass `mix precommit`.

## Domain Assumptions & Conventions

- Conversations are always human ↔ agent. UI and APIs assume exactly one agent per session.
- Messages are at-least-once; clients resend `last_seq` on reconnect and expect replay. Keep sequence handling intact when modifying Runtime API.
- **Raft storage**: Messages commit to Ra groups (256 groups × 3 replicas). Conversation metadata (last_seq, user_id, agent_id) cached in Raft state - no DB queries on append.
- **Background flush**: Flusher queries unflushed messages from ETS rings every 5s, batch-inserts to Postgres (5000 msgs/batch for param limits). Idempotent on `[session_id, seq]`.
- **Snapshots**: Every 100k appends, RaftFSM snapshots to `raft_snapshots` table. Ra compacts log via watermarks.
- LiveView templates begin with `<Layouts.app ...>`; forms use `<.form>`/`<.input>` combos, icons use `<.icon>`.
- Tailwind v4 is the styling backbone. Maintain the stock import stanza and craft micro-interactions via utility classes.

## Tooling Shortcuts

- `mix ecto.migrate` / `mix ecto.rollback` for schema changes
- `mix assets.build` for rebuilding Tailwind + JS bundles
- `mix phx.gen.html` / `mix phx.gen.live` are unused-prefer handcrafted components consistent with the design language

## Checklist Before You Finish

1. All new/modified modules follow the fail-loud, pattern-matching style.
2. Telemetry tags remain explicit; no new "unknown" defaults unless product requirements demand it.
3. Tests cover new code paths, especially LiveView/Channel interactions, agent webhooks, and Raft group operations.
4. Run `mix precommit` and address every warning, formatter diff, and test failure.
5. Document runtime changes (Raft membership, rebalancing, storage) in `docs/` if behaviour shifts meaningfully.

## Hot Path Design

**The goal:** Sub-150ms p99 latency for message appends, even under load.

**Critical path:**
1. Client sends message → SessionChannel receives
2. Runtime API calls Raft group (quorum write to 2-of-3 nodes)
3. Raft assigns sequence number from cached conversation state (RAM, no DB)
4. Broadcast via PubSub → client receives ack

**What's NOT on the critical path:**
- Database writes (background flusher, 5s interval)
- Agent webhooks (async queue, 50ms polling)
- Snapshots (triggered at 100k appends, async)

**Key insight:** Conversation metadata (last_seq, user_id, agent_id) lives in Raft state. This eliminates the database query that would otherwise dominate p99 latency. Raft's quorum cost (~10-20ms) is predictable and bounded, unlike database connection pools under contention.
