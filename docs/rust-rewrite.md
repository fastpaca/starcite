# Rust Rewrite Experiment

This branch adds a parallel Rust implementation under [rust/starcite-rs](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/rust/starcite-rs).

The goal is not feature parity in one shot. The goal is to test whether a saner core falls out when Starcite is modeled as:

- Axum handlers with typed request validation
- one Postgres-backed write path
- one Postgres-backed replay path
- no S3 archive adapter
- no in-memory Raft ownership layer

## Included in the experiment

- session create, list, show, update
- append with `expected_seq`
- producer dedupe and replay conflict handling
- archive and unarchive as a session visibility flag
- explicit auth modes with `none` and `unsafe_jwt`
- Prometheus text metrics on a separate ops listener
- `/health/live` and `/health/ready` on the same separate ops listener
- local ops-state JSON on `GET /debug/state`
- local manual drain on `POST /debug/drain`
- local manual drain reset on `DELETE /debug/drain`
- local shutdown drain with `STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS`
- Phoenix-compatible multiplexed socket transport on `GET /v1/socket/websocket`
- tenant-scoped durable lifecycle replay through `GET /v1/lifecycle/events?tenant_id=...`
- tenant-scoped replay-then-live lifecycle streaming through `GET /v1/lifecycle?tenant_id=...`
- session-scoped durable lifecycle replay through `GET /v1/sessions/:id/lifecycle/events`
- session-scoped replay-then-live lifecycle streaming through `GET /v1/sessions/:id/lifecycle`
- local in-process runtime lifecycle with `session.activated`, `session.hydrating`, `session.freezing`, and `session.frozen`
- ordered event replay through `GET /v1/sessions/:id/events`
- replay-then-live tail through `GET /v1/sessions/:id/tail?cursor=N` over WebSocket
- SQL migrations and a dedicated Docker Compose file

The raw WebSocket endpoints keep the existing public payload shape where it matters:

- `GET /v1/socket/websocket` now accepts Phoenix channel frames so one socket can join the operator `lifecycle` topic plus many `lifecycle:<session_id>` and `tail:<session_id>` topics
- lifecycle delivery arrives as `{"cursor": N, "inserted_at": "...", "event": {...}}` frames, with the inner event discriminator serialized as `kind`
- replay and live delivery arrive as `{"events":[...]}` frames
- invalid resume cursors emit a `{"type":"gap", ...}` frame with `reason = "resume_invalidated"`
- in `unsafe_jwt` mode, raw lifecycle and tail sockets emit `{"type":"token_expired","reason":"token_expired"}` and terminate once the active token passes its `exp`
- when local shutdown drain begins, raw lifecycle and tail sockets emit `{"type":"node_draining","reason":"node_draining","drain_source":"shutdown","retry_after_ms":N}` before the connection closes
- the direct raw WebSocket endpoints still use query params for `tenant_id`, `cursor`, `session_id`, and `batch_size` on the relevant routes
- `GET /metrics` plus `/health/*` are served on `STARCITE_OPS_PORT`, not the public API listener
- `GET /debug/state` is served on `STARCITE_OPS_PORT` and exposes local drain source, runtime, and fanout state for this one process
- `POST /debug/drain` is served on `STARCITE_OPS_PORT` and flips the local process into `draining` without terminating it, which is useful for local drain drills
- `DELETE /debug/drain` clears only a manual drain and refuses to undo a real shutdown drain
- `GET /health/live` returns a small JSON body and stays healthy during shutdown drain
- `GET /health/ready` now reports `mode = "ready"` or `mode = "draining"`, plus drain metadata during shutdown drain, instead of only exposing probe status code
- `GET /metrics` exports in-process Prometheus text without introducing a separate metrics service or crate dependency

The Phoenix-compatible socket is explicitly incomplete but useful. It supports `heartbeat`,
`phx_join`, and `phx_leave` with the usual Phoenix frame array shape
`[join_ref, ref, topic, event, payload]`, replies with `phx_reply`, accepts `cursor` plus
optional `session_id` on the operator `lifecycle` topic, accepts plain `cursor` on
`lifecycle:<session_id>` joins, and pushes `token_expired` when an active socket outlives an
`unsafe_jwt` token. During local shutdown drain it also pushes `node_draining` on active joined
topics before the socket closes.

Auth is now explicit instead of hand-waved:

- `STARCITE_AUTH_MODE=none` keeps the existing trust-everything local path
- `STARCITE_AUTH_MODE=unsafe_jwt` parses JWT claims without signature verification and enforces
  scope, tenant, session lock, and expiry across HTTP plus WebSocket transports
- `STARCITE_ENABLE_TELEMETRY=true` enables Prometheus exposition and in-process metric recording;
  `false` leaves only the uptime gauge
- `unsafe_jwt` expects `Authorization: Bearer <jwt>` on HTTP and `token=<jwt>` on WebSocket URLs
- tenant-scoped lifecycle subscriptions in `unsafe_jwt` require a service principal with
  `session:read` and no `session_id` lock
- session-scoped lifecycle routes and `lifecycle:<session_id>` topics use normal
  `allow_read_session` checks, so session-locked tokens can consume them directly

That gets the Rust rewrite onto the real Starcite policy surface without pretending it already has
JWKS-backed trust.

The runtime lifecycle is intentionally simple. This service keeps a local active-session map,
marks new sessions active immediately, emits `session.freezing` plus `session.frozen` after
`SESSION_RUNTIME_IDLE_TIMEOUT_MS` of inactivity, and emits `session.hydrating` plus
`session.activated` when a cold session is touched again. That gives the rewrite a typed lifecycle
story without pretending it already has cluster ownership semantics. The lifecycle log itself is
tenant-scoped, but the lifecycle surface now exposes dedicated session routes and topics in
addition to the older server-filtered tenant view. When the tenant lifecycle stream is filtered to
one session, resume-gap detection is computed against that session head instead of the full tenant
head. `GET /debug/state` exposes that local runtime map directly, including the current generation
per active session, so the rewrite now has one honest ops surface for "what this process thinks it
owns right now."

The rewrite now also has a minimal local drain story instead of pretending graceful shutdown is
free. On `SIGTERM` or `Ctrl-C`, the process flips into `draining`, `/health/ready` returns `503`
with `reason = "draining"` plus `drain_source = "shutdown"` and a live `retry_after_ms`, new
public HTTP requests and new WebSocket handshakes fail with `node_draining`, `x-starcite-drain-source`,
and shutdown `Retry-After` headers, existing raw sockets emit a terminal `node_draining` frame
with the same metadata and then close with code `1012`, existing Phoenix topic subscriptions
receive a `node_draining` push with the same metadata and then the socket closes with code `1012`,
and the listeners stay up for
`STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS` before the actual server shutdown future resolves. The same
local drain state can be triggered manually through `POST /debug/drain` on the ops listener for
local drills without terminating the process. That is still much simpler than the Phoenix routing
drain, but it gives the experiment an honest edge behavior during termination.

Because this rewrite keeps the full event log in Postgres, it does not currently emit
`cursor_expired` gaps. There is no hot-tail trimming boundary to fall behind.

The in-process fanout is now demand-driven instead of append-driven. A tail or lifecycle
subscription allocates its Tokio broadcast channel lazily on first subscribe, plain broadcasts do
not create dormant channels for untouched sessions or tenants, and a stale channel is pruned when
the last receiver disconnects instead of waiting for another broadcast to notice the zero-subscriber
sender. That keeps the local memory story closer to "active sockets only" instead of "every session
ever touched by this process."

The Postgres append path is also less naive than the first cut now. Instead of deriving producer
ordering from the `events` table on every write, the rewrite keeps a dedicated `session_producers`
table in Postgres with the last committed `producer_seq` per `(session_id, producer_id)`. That
keeps successful appends on small session and producer rows plus the one new event insert, while
still falling back to an exact historical event lookup for replay-vs-conflict decisions when a
producer sends an older sequence.

Live delivery is no longer purely process-local either. Committed event and lifecycle writes now
emit Postgres `NOTIFY` payloads, and every Rust process runs a `LISTEN` loop that reloads the
committed row from Postgres and rebroadcasts it into local fanout. That means a client connected to
one Rust node can still receive live events or lifecycle updates produced by another Rust node
sharing the same database, without adding Redis or a separate broker.

Telemetry parity is now partial instead of missing. The Rust service exports edge HTTP,
controller-entry edge-stage telemetry, auth, ingest-edge outcomes, append request timings, tail
plus lifecycle delivery timings, active raw stream subscriptions and Phoenix topic joins, active
socket connection gauges, local session lifecycle counters, and dynamic gauges for node drain
state plus runtime/fanout occupancy, including runtime sessions grouped by last touch reason, with
metric names aligned to the existing PromEx surface where that still makes sense. `/debug/state`
now exposes that same local runtime map with tenant, generation, last touch reason, and remaining
idle time per active session. It still does not cover routing, replication, archive, or
event-store invariants because those subsystems do not exist in this rewrite.

One subtle transport fix landed with those gauges: the raw tail and lifecycle sockets now keep
reading control frames so a quiet client disconnect clears the in-process connection gauge
immediately instead of leaving the task parked until the next event arrives.

## Deliberate gaps

- no signature-verified JWT or JWKS flow yet; `unsafe_jwt` is claim parsing only
- no distributed ownership, quorum replication, or topology routing behind the runtime lifecycle
- no routing/replication/archive telemetry parity with the Phoenix service

## Why this shape

The existing Elixir system splits the hot path from the durable archive path and then rebuilds ordered replay across memory, Postgres, and S3. This rewrite tests the opposite tradeoff: make Postgres the whole log, pay the write cost directly, and get a much simpler consistency story in return.

If the experiment holds up, the next decisions are straightforward:

1. Replace `unsafe_jwt` with verified JWT/JWKS handling without losing the typed policy surface.
2. Decide whether the filtered tenant `session_id` compatibility path should stay once clients move to dedicated session lifecycle routes and topics.
3. Decide whether the Rust metrics surface should stay as direct Prometheus text or eventually grow a richer event substrate before attempting any horizontal scaling story.
