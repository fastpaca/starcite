# CLAUDE.md

Guidance for Claude Code when collaborating on FleetLM.

## Quick Start

- `mix setup` - bootstrap deps, DB, and assets
- `mix phx.server` - run the dev server
- `mix test` / `mix test path/to/file.exs` - execute test suites
- `mix precommit` - final gate before handing work back (compile + format + tests)

## Product Surface

FleetLM exposes three primitives under `/v1`:

- `POST /v1/sessions` (`create`)
- `POST /v1/sessions/:id/append` (`append`)
- `GET /v1/sessions/:id/tail?cursor=N` (WebSocket `tail`)

Contract reminders:

- `tail` means catch up from `cursor`, then stream live on one socket.
- `append` is shared by humans and agents.
- `actor` is required on append and is an opaque string.
- `type` + `payload` are protocol-agnostic in this iteration.
- `idempotency_key` and `expected_seq` are optional controls.
- Auth is upstream; FleetLM does not own authn/authz.
- FleetLM does not push outbound webhooks.

## Architecture Cheat Sheet

- **Storage:** 256 Raft groups (Ra library) with 3 replicas each. Quorum writes (2 of 3) and automatic leader election.
- **No per-session processes:** Request handling is stateless; Raft state machines are the long-lived state holders.
- **Session state in Raft:** Session metadata and ordered event logs are cached in Raft state.
- **Postgres as write-behind:** Optional background archiver flushes committed events to Postgres. Failures do not block append acks.
- **Cluster membership:** Erlang distribution + coordinator reconcile topology and deterministic replica assignment.

## Working Standards

- Pattern-match required input (controller params, runtime commands, websocket params); return descriptive errors for invalid shapes.
- Make telemetry strict. Add clauses like `defp message_tags(%{role: role})` and treat everything else as the fallback path that highlights anomalies.
- When serializing structs/maps, destructure once and build the response. Avoid peppering `Map.get` across atom/string variants; normalize at the boundary.
- Preserve API contract semantics: idempotency dedupe, expected-seq conflicts, and replay ordering.
- Lint, format, and test locally. Every PR should pass `mix precommit`.

## Domain Assumptions & Conventions

- Sessions are the primary resource.
- Events are append-only per session and ordered by monotonic `seq`.
- Clients recover from disconnects by reconnecting `tail` with their last processed cursor.
- `metadata` and `refs` are opaque application data; FleetLM stores and replays them.
- **Background flush**: Flusher batch-inserts committed events to Postgres when archiver is enabled. Idempotent on `[session_id, seq]`.
- **Snapshots**: Raft snapshots are used for recovery and log compaction.
- LiveView templates begin with `<Layouts.app ...>`; forms use `<.form>`/`<.input>` combos, icons use `<.icon>`.
- Tailwind v4 is the styling backbone. Maintain the stock import stanza and craft micro-interactions via utility classes.

## Tooling Shortcuts

- `mix ecto.migrate` / `mix ecto.rollback` for schema changes
- `mix assets.build` for rebuilding Tailwind + JS bundles
- `mix phx.gen.html` / `mix phx.gen.live` are unused-prefer handcrafted components consistent with the design language

## Checklist Before You Finish

1. All new/modified modules follow the fail-loud, pattern-matching style.
2. Telemetry tags remain explicit; no new "unknown" defaults unless product requirements demand it.
3. Tests cover new code paths, especially create/append/tail contracts, conflict handling, and Raft group behavior.
4. Run `mix precommit` and address every warning, formatter diff, and test failure.
5. Document runtime changes (Raft membership, rebalancing, storage) in `docs/` if behaviour shifts meaningfully.

## Hot Path Design

**The goal:** Sub-150ms p99 latency for message appends, even under load.

**Critical path:**
1. Client calls `POST /v1/sessions/:id/append`
2. Runtime routes append to the session's Raft group
3. Quorum commit assigns next `seq`
4. API responds with committed `seq`; tail subscribers receive the committed event

**What's NOT on the critical path:**
- Database writes (background flusher, 5s interval)
- Snapshots (triggered at 100k appends, async)

**Key insight:** Session metadata and ordered event state live in Raft memory/log state, avoiding synchronous database lookups on append.
