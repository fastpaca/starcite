# Rust Rewrite Experiment

This branch adds a parallel Rust implementation under [rust/starcite-rs](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/rust/starcite-rs).

This branch started as a deliberate architecture experiment. It is not a
faithful rewrite of the existing Starcite hot path, and it should not be used
as the target design for low-tail parity work.

The actual parity target for a Rust rewrite is documented in
[rust-parity-plan.md](/Users/selund/git/fastpaca/starcite/.worktrees/refactor-radical-re/docs/rust-parity-plan.md).

The experiment here asked what falls out when Starcite is modeled as:

- Axum handlers with typed request validation
- one Postgres-backed write path
- one Postgres-backed replay path
- no S3 archive adapter
- no in-memory Raft ownership layer; Postgres is now carrying the lease and standby-assignment control plane in this experiment

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
- tenant-scoped lifecycle replay plus live delivery through the Phoenix `lifecycle` topic on `GET /v1/socket/websocket`
- local in-process runtime lifecycle with `session.activated`, `session.hydrating`, `session.freezing`, and `session.frozen`
- ordered event replay through `GET /v1/sessions/:id/events`
- replay-then-live tail through `GET /v1/sessions/:id/tail?cursor=N` over WebSocket
- SQL migrations and a dedicated Docker Compose file

The raw WebSocket endpoints keep the existing public payload shape where it matters:

- `GET /v1/socket/websocket` now accepts Phoenix channel frames so one socket can join the operator `lifecycle` topic plus many `tail:<session_id>` topics
- lifecycle delivery arrives as `{"cursor": N, "inserted_at": "...", "event": {...}}` frames, with the inner event discriminator serialized as `kind`
- replay and live delivery arrive as `{"events":[...]}` frames
- invalid resume cursors emit a `{"type":"gap", ...}` frame with `reason = "resume_invalidated"`
- in `unsafe_jwt` mode, raw tail sockets and Phoenix topic subscriptions emit `{"type":"token_expired","reason":"token_expired"}` and terminate once the active token passes its `exp`
- when local shutdown drain begins, raw tail sockets and Phoenix topic subscriptions emit `{"type":"node_draining","reason":"node_draining","drain_source":"shutdown","retry_after_ms":N}` before the connection closes
- the direct raw WebSocket endpoint still uses query params for `cursor` and `batch_size` on the tail route
- `GET /metrics` plus `/health/*` are served on `STARCITE_OPS_PORT`, not the public API listener
- `GET /debug/state` is served on `STARCITE_OPS_PORT` and exposes local drain source, runtime, and fanout state for this one process
- `POST /debug/drain` is served on `STARCITE_OPS_PORT` and flips the local process into `draining` without terminating it, which is useful for local drain drills
- `DELETE /debug/drain` clears only a manual drain and refuses to undo a real shutdown drain
- `GET /health/live` returns a small JSON body and stays healthy during shutdown drain
- `GET /health/ready` now reports `mode = "ready"` or `mode = "draining"`, plus drain metadata during shutdown drain, instead of only exposing probe status code
- `GET /metrics` exports in-process Prometheus text without introducing a separate metrics service or crate dependency
- the existing hot-path k6 bench can target the split public and ops listeners by keeping `CLUSTER_NODES` on the public `/v1` URLs and setting `CLUSTER_READY_NODES` to the matching ops base URLs
- the same k6 bench now also accepts `SESSION_ROUTE_MODE=designated_owner`, which hashes each session onto the same owner key Postgres uses for lease selection (`LOCAL_ASYNC_NODE_PUBLIC_URL` when present, otherwise the node id) so the load test can hit the intended owner directly instead of paying proxy hops on purpose

There is now a dedicated three-node Rust benchmark harness too:

```bash
docker compose -f docker-compose.rust.cluster.yml -p rustcluster up -d --build
docker compose -f docker-compose.rust.cluster.yml -p rustcluster --profile tools run --rm k6 run /bench/k6-hot-path-throughput.js
docker compose -f docker-compose.rust.cluster.yml -p rustcluster down -v --remove-orphans
```

That compose file advertises each node on the internal Docker network (`http://nodeN:4001` and
`http://nodeN:4002`) so owner hints and owner-proxy forwarding work correctly from the bundled k6
container. The k6 service intentionally does not depend on the Rust nodes at the Compose layer, so
`docker compose run k6 ...` does not recreate the cluster you are measuring; the script's own
ready check handles startup ordering instead. If you drive the cluster directly from the host
instead of the in-network k6 service, override the `LOCAL_ASYNC_NODE_PUBLIC_URL` values to
host-reachable addresses first.

When the point of the run is owner-path latency rather than wrong-node forwarding behavior, set
`SESSION_ROUTE_MODE=designated_owner` in the k6 container too. Leaving the default
`round_robin` mode in place is still useful, but it now measures the extra proxy hop and routing
behavior across the cluster instead of the designated owner's append hot path.

The Phoenix-compatible socket is explicitly incomplete but useful. It supports `heartbeat`,
`phx_join`, and `phx_leave` with the usual Phoenix frame array shape
`[join_ref, ref, topic, event, payload]`, replies with `phx_reply`, accepts plain `cursor` on the
operator `lifecycle` topic and on `tail:<session_id>` joins, and pushes `token_expired` when an
active socket outlives an `unsafe_jwt` token. During local shutdown drain it also pushes
`node_draining` on active joined topics before the socket closes.

Auth is now explicit instead of hand-waved:

- `STARCITE_AUTH_MODE=none` keeps the existing trust-everything local path
- `STARCITE_AUTH_MODE=unsafe_jwt` parses JWT claims without signature verification and enforces
  scope, tenant, session lock, and expiry across HTTP plus WebSocket transports
- `STARCITE_ENABLE_TELEMETRY=true` enables Prometheus exposition and in-process metric recording;
  `false` leaves only the uptime gauge
- `unsafe_jwt` expects `Authorization: Bearer <jwt>` on HTTP and `token=<jwt>` on WebSocket URLs
- tenant-scoped lifecycle subscriptions in `unsafe_jwt` require a service principal with
  `session:read` and no `session_id` lock

That gets the Rust rewrite onto the real Starcite policy surface without pretending it already has
JWKS-backed trust.

The runtime lifecycle is intentionally simple. This service keeps a local active-session map,
marks new sessions active immediately, emits `session.freezing` plus `session.frozen` after
`SESSION_RUNTIME_IDLE_TIMEOUT_MS` of inactivity, and emits `session.hydrating` plus
`session.activated` when a cold session is touched again. That gives the rewrite a typed lifecycle
story without pretending it already has cluster ownership semantics. The lifecycle log and public
delivery surface are both tenant-scoped through the Phoenix `lifecycle` topic, and
`GET /debug/state` exposes that local runtime map directly, including the current generation per
active session, so the rewrite now has one honest ops surface for "what this process thinks it
owns right now."

The rewrite now also has a minimal local drain story instead of pretending graceful shutdown is
free. On `SIGTERM` or `Ctrl-C`, the process flips into `draining`, `/health/ready` returns `503`
with `reason = "draining"` plus `drain_source = "shutdown"` and a live `retry_after_ms`, new
public HTTP requests and new WebSocket handshakes fail with `node_draining`, `x-starcite-drain-source`,
and shutdown `Retry-After` headers, existing raw sockets emit a terminal `node_draining` frame
with the same metadata and then close with code `1012`, existing Phoenix topic subscriptions
see the same terminal drain signal, and non-draining readiness now consults the local
Postgres `control_nodes` row so stale or missing control-plane registration shows up as
`lease_expired` or `routing_sync` instead of looking healthy just because the last local
heartbeat attempt succeeded.
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

One read-path parity slice has landed too: each Rust process now keeps a local in-memory hot event
store populated by local appends and relayed remote commits. HTTP event reads and tail catch-up
replay consult that hot store first and only fall back to Postgres when the requested range is not
locally contiguous. That moves replay shape closer to the Elixir `EventStore` and `ReadPath`
contracts, even though append ack is still incorrectly paying the Postgres commit path in this
experiment.

There is now a matching hot session store too. Session create, update, archive state, relayed
lifecycle catalog events, and relayed appends keep a local session-header cache current with
tenant and `last_seq` state, so hot session reads and session-scoped auth checks do not need to
start at Postgres every time. Phoenix `tail:<session_id>` replay now uses the same hot read path as
the raw tail endpoint instead of bypassing it with a direct database read.

Another parity-oriented boundary is in place now too: appends no longer execute directly on the
HTTP task. Each process keeps a local per-session append worker map, and `POST /v1/sessions/:id/append`
routes through that worker before it touches the repository layer. That gets session-local append
ordering behind one explicit in-process boundary and makes the ops surface honest about which
sessions this process is actively servicing, even though the worker still persists through
Postgres synchronously in this experiment.

There is now an experimental commit-mode split behind that worker boundary. The default
`sync_postgres` mode keeps the current durable append path. `local_async` instead acknowledges
from local hot state, puts the event into a pending-flush backlog, and lets a background flusher
persist it to Postgres and emit `NOTIFY` later. That flusher now persists each pending session
batch in one Postgres transaction instead of one transaction per event. That is the first real cut
at moving Postgres off the append ack path in this branch. It is intentionally honest about the
remaining gap: without
quorum replication yet, `local_async` is still not a production-safe multi-node commit model.
`local_async` now also keeps producer cursors in the hot session cache instead of deriving them
only from retained hot events or from the flushed `session_producers` row. Local commits, standby
replica commits, and Postgres relay fanout all advance that cache, which closes the false
`producer_seq_conflict` edge when an active session has already archived older hot events but the
background Postgres flush is still trailing the acknowledged write position.
What it does have now is explicit single-writer ownership per active session: a Rust node renews a
Postgres-backed lease while its local session worker is alive, non-owner nodes reject event-path
appends and replay with `409 session_not_owned`, and the next node can take over once the worker
idles out or the lease expires. That makes the branch honest about who may serve the hot event
path without pretending it already has Raft-style ownership transfer. Lease acquisition no longer
lets the first requester pin ownership accidentally either: Postgres deterministically designates a
live owner from the current node set, while a live standby still keeps takeover preference for an
expired lease.

There is now one more real hot-path slice on top of that lease model: `local_async` can replicate
to one synchronous standby over the ops listener, and Postgres now acts as the durable control
plane for which standby is assigned to a given owned session. Nodes that set
`LOCAL_ASYNC_NODE_OPS_URL` heartbeat themselves into `control_nodes`, session lease acquisition
selects one live non-draining peer as `standby_node_id`, spreads new assignments across the least-
renewals. When ownership does move, lease acquisition now prefers the previous live owner as the
new standby before it falls back to a cold peer, so acknowledged sessions keep a warm quorum pair
through renewals and takeover. The owner now sends one append control request to that assigned standby before it applies
the event locally and replies to the client, instead of paying separate prepare and commit round
trips on the critical path. If no assigned standby is available, append fails with
`503 quorum_unavailable` instead of silently falling back to single-node ack.
`LOCAL_ASYNC_STANDBY_URL` remains as a static fallback for local drills when Postgres-backed node
registration is not enabled. That is still only a narrow 2-of-2 experiment, not a full Raft group
or routed topology.

The control-plane heartbeat can now also carry a node's public base URL. That does not implement
full routed ownership yet, but it does make the current `409 session_not_owned` response less
opaque: when the owner has registered a public URL, non-owner nodes include it as `owner_url` in
the body and `x-starcite-owner-url` in the headers. That is enough for manual drills and for a
future edge proxy to reroute requests without guessing.

That edge proxying story has now started inside the Rust service itself for plain HTTP event-path
requests. Wrong-node `GET /events` and `POST /append` calls can forward to the current owner using
the Postgres control-plane hint instead of forcing the client to retry manually. Tail now enforces
that same ownership boundary too: a wrong-node raw tail handshake returns `307` with an owner
WebSocket target, and a wrong-node Phoenix `tail:<session_id>` join fails explicitly with
`session_not_owned` plus `owner_socket_url` so the client can reconnect to the correct node using
the same socket query params.

The local worker lifecycle now also matches the ownership story more closely. A live worker renews
its Postgres lease in the background instead of only on incoming event-path requests, so ownership
does not silently drift after a quiet period. When drain begins, that worker exits and releases the
lease promptly, which lets another node take over reads without waiting for the old TTL to burn
down.

That active worker now also keeps the session head and producer cursors in local memory while the
session stays hot. First touch still hydrates from the shared cache or Postgres, but repeated
owner-path appends no longer re-read the shared hot-session store before every quorum replicate.

Lease handoff now also respects the assigned standby on expiry. When a session lease has expired
but Postgres still shows a live non-draining `standby_node_id`, non-standby nodes stop stealing
that session and instead return the standby as the owner hint. That keeps acknowledged
`local_async` state biased toward the node that already holds the replicated hot copy, while still
falling back to any live node once the designated standby disappears.

Replica fencing now follows that handoff too. Once a standby takes over with a newer epoch, it
rejects any later append request from that stale epoch. Standby appends now apply directly into the
hot in-memory state and require the next exact session sequence, so delayed or out-of-order
internal replica traffic fails loudly instead of silently re-applying or skipping state after a
takeover.

The rewrite now also has a background archive-progress worker backed by an explicit dirty-session
queue. Owner commits enqueue the touched session, the worker advances `sessions.archived_seq`, and
hot in-memory events are pruned once they are considered archived. The
periodic tick is now only a fallback nudge if no new work arrives. In this branch that worker is
still transitional: the event rows already exist in Postgres before the flush, so it restores
hot/cold tier behavior without yet removing Postgres from the append ack path. Standby commits no
longer feed that queue. They keep only the committed hot copy in memory, and when a worker first
becomes owner it seeds the flush backlog from that retained hot state before it starts
acknowledging new appends. That keeps steady-state standby quorum commits off the Postgres flush
path while preserving a durable catch-up path after failover.

Archive progress now also relays over Postgres `NOTIFY`, so other Rust nodes can advance cached
`archived_seq` state and prune stale hot copies without waiting for a cold session refresh.

Telemetry parity is now partial instead of missing. The Rust service exports edge HTTP,
controller-entry edge-stage telemetry, auth, ingest-edge outcomes, append request timings, tail
plus lifecycle delivery timings, active raw stream subscriptions and Phoenix topic joins, active
socket connection gauges, local session lifecycle counters, and dynamic gauges for node drain
state plus runtime/fanout occupancy, including runtime sessions grouped by last touch reason, with
metric names aligned to the existing PromEx surface where that still makes sense. `/debug/state`
now exposes that same local runtime map plus hot event-store state, hot session-store state,
archive-queue backlog, local lease ownership state, standby replication config, and remaining idle
time per active session. It still does not cover routing, full replication, archive, or event-store
invariants because those subsystems do not exist in this rewrite.

One subtle transport fix landed with those gauges: the raw tail and lifecycle sockets now keep
reading control frames so a quiet client disconnect clears the in-process connection gauge
immediately instead of leaving the task parked until the next event arrives.

One runtime fix landed too: resume lifecycle persistence is no longer awaited on the hot read path.
A cold-session HTTP read or tail join updates local runtime state immediately and then persists the
runtime lifecycle in the background instead of blocking on Postgres before returning.

`GET /debug/state` now also exposes the local control-plane heartbeat config next to ownership and
replication state, so it is possible to tell whether a node is only running the hot path locally or
is actually participating in Postgres-backed standby assignment.

## Deliberate gaps

- no signature-verified JWT or JWKS flow yet; `unsafe_jwt` is claim parsing only
- no full quorum replication or topology routing behind the runtime lifecycle; `local_async` has
  only Postgres-backed single-writer session leases plus one synchronous standby chosen by the
  Postgres control plane or a static fallback URL
- no routing/replication/archive telemetry parity with the Phoenix service

## Why this shape

The existing Elixir system splits the hot path from the durable archive path and then rebuilds ordered replay across memory, Postgres, and S3. This rewrite tests the opposite tradeoff: make Postgres the whole log, pay the write cost directly, and get a much simpler consistency story in return.

If the experiment holds up, the next decisions are straightforward:

1. Replace `unsafe_jwt` with verified JWT/JWKS handling without losing the typed policy surface.
2. Decide whether the filtered tenant `session_id` compatibility path should stay once clients move to dedicated session lifecycle routes and topics.
3. Decide whether the Rust metrics surface should stay as direct Prometheus text or eventually grow a richer event substrate before attempting any horizontal scaling story.
