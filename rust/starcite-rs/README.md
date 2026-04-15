# starcite-rs

`starcite-rs` is a deliberate rewrite experiment for Starcite:

- Rust + Axum instead of Phoenix
- Postgres as the default source of truth for session headers, events, lifecycle, and producer cursors
- no S3 archive split
- no Raft process or in-memory consensus layer

This is a parallel implementation inside the existing repo, not a drop-in replacement yet.

## What it does today

- `POST /v1/sessions`
- `GET /v1/sessions`
- `GET /metrics` on the ops listener
- `GET /debug/state` on the ops listener
- `POST /debug/drain` and `DELETE /debug/drain` on the ops listener
- `GET /v1/lifecycle/events`
- `GET /v1/lifecycle` over WebSocket
- `GET /v1/socket/websocket` over WebSocket with Phoenix channel framing
- `GET /v1/sessions/:id`
- `PATCH /v1/sessions/:id`
- `POST /v1/sessions/:id/append`
- `GET /v1/sessions/:id/events`
- `GET /v1/sessions/:id/lifecycle/events`
- `GET /v1/sessions/:id/lifecycle` over WebSocket
- `GET /v1/sessions/:id/tail` over WebSocket
- `POST /v1/sessions/:id/archive`
- `POST /v1/sessions/:id/unarchive`
- `GET /health/live` on the ops listener
- `GET /health/ready` on the ops listener

The API shape stays close to the current Starcite REST surface. The main intentional differences are:

- `STARCITE_AUTH_MODE=none` keeps the local no-auth flow for fast iteration.
- `STARCITE_AUTH_MODE=jwt` verifies bearer JWTs against configured JWKS keys and enforces issuer, audience, expiry, scope, tenant, and session-lock claims across HTTP plus both WebSocket transports.
- The only supported write model acknowledges appends from local hot owner state and flushes them to Postgres in the background. That flusher persists each pending session batch in one Postgres transaction instead of one transaction per event. Each active session is guarded by a Postgres-backed lease, and Postgres also acts as the durable control plane for standby assignment when nodes heartbeat their ops URLs.
- `STARCITE_ENABLE_TELEMETRY=true` exposes Prometheus text metrics on `GET /metrics` and records a focused subset of the Phoenix telemetry contract: edge HTTP, edge-stage controller entry, auth, ingest-edge outcomes, append request timings, tail plus lifecycle delivery timings, active socket gauges, local session runtime counters, and dynamic gauges for node drain state plus runtime/fanout occupancy, including runtime sessions grouped by last touch reason.
- `STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS` puts the process into local `draining` mode on `SIGTERM` or `Ctrl-C`, flips readiness non-ready immediately, rejects new public requests plus socket handshakes with `node_draining`, includes `drain_source` and shutdown `retry_after_ms` hints in those drain responses, emits the same drain metadata to already-open raw and Phoenix topic subscriptions, waits the configured drain window, and only then shuts the listeners down.
- In `jwt` mode, HTTP endpoints expect `Authorization: Bearer <jwt>`, while the raw WebSocket endpoints plus the Phoenix-compatible socket expect `token` in the query string. `access_token` is rejected on the Phoenix-compatible socket.
- The append hot path now keeps per-session producer cursors in Postgres, so a normal successful append does not need to consult historical event rows just to discover the next `producer_seq`.
- The rewrite now keeps a local in-memory hot event store per process. Local appends and relayed remote commits both populate that hot store, and `GET /v1/sessions/:id/events` plus tail catch-up replay read hot state first and only fall back to Postgres when the requested range is not locally contiguous.
- The rewrite now also keeps a local hot session store per process. Session create, update, archive state, relayed lifecycle catalog events, and relayed appends keep that cache current, so hot session reads and session-scoped auth checks can resolve tenant and header state without treating Postgres as the first lookup every time.
- Appends now route through a local per-session worker boundary before they hit the repository layer. That gets append ordering off the HTTP task and onto one explicit session-local queue per process, which is closer to the eventual runtime/quorum shape even though the worker still calls Postgres synchronously in this branch.
- The session worker now has a real local commit path: it updates hot in-memory session and event state, broadcasts live fanout, and queues the event for a background Postgres flusher. Same-node reads can keep working while Postgres is briefly unavailable, but flushed durability and cross-node visibility trail the ack.
- `local_async` now also keeps per-session producer cursors in the hot session cache, and relayed commits update that cache too. That lets the owner and its peers continue validating `producer_seq` correctly even after old hot events have been archived away locally and before the background flush has fully caught Postgres up.
- Active session workers now also keep the current session head and per-producer cursors in local memory while the session stays hot, so repeated owner-path appends do not bounce through shared cache lookups before standby replication.
- `local_async` now also carries explicit single-writer ownership. The owner node renews a Postgres-backed session lease while its local worker is active, Postgres deterministically designates a live owner for a session instead of letting the first requester win, and a live standby still keeps takeover preference when a lease expires. The process exposes that lease state on `GET /debug/state` and rejects event-path reads or appends on non-owner nodes with `409 session_not_owned` until the worker idles out or the lease expires.
- When local drain begins, active session workers now exit and release their leases instead of holding ownership until the TTL runs out. That lets another node take over read ownership immediately while writes still require a healthy standby quorum.
- When nodes also register `LOCAL_ASYNC_NODE_PUBLIC_URL`, that `409 session_not_owned` response includes `owner_url` in the JSON body and `x-starcite-owner-url` in the headers so a caller or future edge proxy can reroute directly to the current owner.
- HTTP `GET /v1/sessions/:id/events` and `POST /v1/sessions/:id/append` now use that owner hint directly. A wrong-node request is proxied to the current owner over plain HTTP instead of forcing the client to retry manually. If the owner cannot be reached, the edge returns `503 owner_proxy_unavailable` with the same owner hint.
- Raw WebSocket tail now uses the same owner metadata instead of failing blind. A wrong-node `GET /v1/sessions/:id/tail` handshake returns `307 Temporary Redirect`, `Location`, and `x-starcite-owner-websocket-url` pointing at the owner tail URL for the same session and cursor.
- Phoenix `tail:<session_id>` joins now enforce the same ownership boundary. A wrong-node join replies with `reason = "session_not_owned"` plus `owner_socket_url`, and that socket URL preserves the original Phoenix socket query params such as `token`, `tenant_id`, and `vsn`.
- `LOCAL_ASYNC_NODE_OPS_URL=http://host:ops_port` registers the node in Postgres as a live standby candidate. When that is enabled, the session lease path assigns one live non-draining standby per owned session, spreads new standby assignments across the least-loaded live peers, keeps a healthy standby stable across routine owner renewals, prefers the previous live owner as the new standby on takeover, and treats standby replication as required for quorum.
- Assigned standby replication now uses one internal append request per event instead of separate prepare and commit hops, and the owner reuses warm standby TCP connections across appends, so the single-standby quorum path drops one round trip and one connect cost from the ack path.
- Standby append handling now applies directly into the hot local state and rejects seq gaps, so the replicated path behaves like an ordered commit log instead of buffering a second in-process prepare/commit step.
- `LOCAL_ASYNC_STANDBY_URL=http://host:ops_port` remains as a static fallback for local drills when you do not want Postgres-backed node registration.
- A local archive worker now drains an explicit dirty-session queue, advances `sessions.archived_seq`, and prunes hot in-memory events once they are considered archived. The periodic tick is now only a fallback nudge, not the primary discovery path. In this transitional branch the backend rows already exist in Postgres before that flush, so the worker is modeling the hot/cold boundary and eviction behavior rather than removing Postgres from the ack path yet.
- Only the active owner now enqueues background Postgres flush work. Standby replicas keep the committed hot copy in memory for takeover, and when a worker first becomes owner it seeds its pending flush queue from that retained hot state before acknowledging new appends. That keeps standby quorum commits cheap while still letting Postgres catch up after failover.
- Committed event and lifecycle writes now fan out across Rust processes through Postgres `LISTEN/NOTIFY`, so live sockets connected to a different Rust node can still receive updates without Redis or a separate message bus.
- `GET /v1/socket/websocket` accepts Phoenix JSON channel frames, so one client WebSocket can join the operator `lifecycle` topic plus many `lifecycle:<session_id>` and `tail:<session_id>` topics.
- In `jwt` mode, the tenant-scoped `lifecycle` topic and raw endpoint require a service principal with `session:read` and no `session_id` lock, matching the operator policy shape.
- Session-scoped lifecycle replay is available over `GET /v1/sessions/:id/lifecycle/events?cursor=N&limit=M`, and replay-then-live delivery is available over `GET /v1/sessions/:id/lifecycle?cursor=N`.
- Tenant-scoped lifecycle replay is available over `GET /v1/lifecycle/events?cursor=N&limit=M`, and replay-then-live delivery is available over `GET /v1/lifecycle?cursor=N`.
- Tenant-scoped lifecycle replay and streaming still accept optional `session_id` filters for compatibility, but the dedicated session routes and Phoenix `lifecycle:<session_id>` topic are the preferred client path.
- Live tail uses a direct WebSocket endpoint (`GET /v1/sessions/:id/tail?cursor=N`) instead of Phoenix Channels.
- Lifecycle frames follow the canonical payload shape `{"cursor": N, "inserted_at": "...", "event": {...}}`, with the inner event discriminator serialized as `kind`. The Rust rewrite emits both session catalog events (`session.created`, `session.updated`, `session.archived`, `session.unarchived`) and local runtime state transitions (`session.activated`, `session.hydrating`, `session.freezing`, `session.frozen`).
- Lifecycle storage is still tenant-scoped under the hood, but the public lifecycle surface now exposes both tenant streams and dedicated session streams. When a tenant lifecycle stream uses `session_id`, gap detection is computed against that filtered session head rather than the full tenant head.
- Raw WebSocket tail uses query params for `cursor` and `batch_size` because there is no channel join payload.
- Tail frames follow the canonical payload shape: `{"events":[...]}` for replay/live delivery and `{"type":"gap",...}` for invalid resume cursors.
- A wrong-node raw tail handshake now returns `307` with a JSON `session_not_owned` body plus `Location` and `x-starcite-owner-websocket-url` headers pointing at the owner tail URL for the same query.
- A wrong-node Phoenix `tail:<session_id>` join returns a `phx_reply` error payload with `reason = "session_not_owned"` and `owner_socket_url` for the owner node's `/v1/socket/websocket` endpoint, preserving the current socket query params.
- In `jwt` mode, raw tail and lifecycle sockets push `{"type":"token_expired","reason":"token_expired"}` and terminate once the active token crosses its `exp` boundary.
- When local shutdown drain begins, raw tail and lifecycle sockets push `{"type":"node_draining","reason":"node_draining","drain_source":"shutdown","retry_after_ms":N}` before the process closes the connection.
- Raw lifecycle sockets subscribe before replaying from Postgres, then reuse the same Postgres replay path when fanout lags.
- Tail connections subscribe before replaying, then receive in-process fanout updates; raw tail and Phoenix `tail:<session_id>` topics both replay from the hot read path first and only fall back to Postgres when the requested range is not locally contiguous. If the fanout buffer lags, the server replays again to close the gap.
- In-process fanout channels are demand-driven: broadcasts do not allocate dormant per-session or per-tenant channels, and idle channels are pruned when the last receiver disconnects instead of waiting for the next broadcast.
- `/health/live`, `/health/ready`, and `/metrics` now live on `STARCITE_OPS_PORT` instead of the public API port, matching the Phoenix deployment shape more closely.
- `GET /health/live` returns `{"status":"ok"}` and stays live during shutdown drain.
- `GET /health/ready` returns `{"status":"ok","mode":"ready"}` when the process is serving, and `503 {"status":"starting","mode":"draining","reason":"draining","drain_source":"shutdown","retry_after_ms":N}` once shutdown drain begins.
- Outside shutdown drain, `GET /health/ready` now checks the local `control_nodes` row in Postgres instead of trusting only the in-process heartbeat timer. If the row is missing or stale it returns `reason = "routing_sync"` or `reason = "lease_expired"`, and when detail is available it includes a `detail` object matching the old readiness contract shape.
- Public `503 node_draining` responses include `x-starcite-drain-source`, and shutdown drain responses also include `Retry-After` plus `x-starcite-retry-after-ms`.
- `GET /debug/state` on `STARCITE_OPS_PORT` exposes local ops mode, auth mode, write model, runtime state, hot event-store state, hot session-store state, local session-worker state, local session-lease ownership state, Postgres control-plane heartbeat config, standby replication config, pending-flush backlog, archive-queue backlog, and fanout state for this process only, including each active runtime session's tenant, generation, last touch reason, idle deadline countdown, and per-session and per-tenant subscriber counts.
- The hot session-store snapshot now also carries cached `archived_seq` per session, and the local archive worker updates that cache as it advances archive progress.
- Archive progress now also relays across Rust nodes through Postgres `NOTIFY`, so standby session caches and hot-store pruning can follow the owner’s archived frontier without waiting for a cold refresh.
- `POST /debug/drain` on `STARCITE_OPS_PORT` flips local drain without terminating the process, which is useful for verifying readiness and socket-drain behavior in local drills.
- `DELETE /debug/drain` clears only a manual drain and returns the process to `ready`; it refuses to clear a real shutdown drain.
- Runtime lifecycle is local to this process. A new session emits `session.activated` before `session.created`, an idle session emits `session.freezing` then `session.frozen`, and the next session-scoped read, append, tail join, or lifecycle read/stream on that cold session emits `session.hydrating` then `session.activated`.
- Resume lifecycle persistence is now fire-and-forget from the hot read path. A cold-session read or tail join updates local runtime state immediately and does not wait for Postgres lifecycle persistence before returning.
- `/metrics` exports Prometheus text directly from the Rust process without an external metrics service or new crate dependency.
- `/metrics` now also exports `starcite_node_draining`, `starcite_runtime_active_sessions`, `starcite_runtime_active_sessions_by_reason`, `starcite_archive_queue_pending_sessions`, `starcite_fanout_active_keys`, and `starcite_fanout_subscribers`, so the metrics surface reflects the same local ops/runtime truth as `GET /debug/state`.
- In `jwt` mode, creates always use the token principal and tenant, appends derive `actor` from token `sub` when omitted, and appended event metadata gains a `starcite_principal` object.

## Run locally

```bash
cd rust/starcite-rs
cargo run
```

Environment variables:

```bash
PORT=4001
STARCITE_OPS_PORT=4002
DATABASE_URL=postgres://postgres:postgres@localhost:5433/starcite_rust_dev
DATABASE_MAX_CONNECTIONS=20
MIGRATE_ON_BOOT=true
STARCITE_AUTH_MODE=none
STARCITE_JWT_ISSUER=https://issuer.example
STARCITE_JWT_AUDIENCE=starcite-api
STARCITE_JWKS_URL=https://issuer.example/.well-known/jwks.json
STARCITE_JWT_LEEWAY_SECONDS=1
STARCITE_JWKS_REFRESH_MS=60000
STARCITE_JWKS_HARD_EXPIRY_MS=60000
STARCITE_ENABLE_TELEMETRY=true
STARCITE_SHUTDOWN_DRAIN_TIMEOUT_MS=30000
SESSION_RUNTIME_IDLE_TIMEOUT_MS=30000
COMMIT_FLUSH_INTERVAL_MS=100
LOCAL_ASYNC_LEASE_TTL_MS=5000
LOCAL_ASYNC_NODE_PUBLIC_URL=
LOCAL_ASYNC_NODE_OPS_URL=
LOCAL_ASYNC_NODE_TTL_MS=2000
LOCAL_ASYNC_OWNER_PROXY_TIMEOUT_MS=1000
LOCAL_ASYNC_STANDBY_URL=
LOCAL_ASYNC_REPLICATION_TIMEOUT_MS=500
ARCHIVE_FLUSH_INTERVAL_MS=5000
RUST_LOG=info
```

Auth mode values:

- `none` for the current trust-everything local workflow
- `jwt` to verify JWT signatures against JWKS and enforce the claim contract

Write model:

- local-async acknowledgement with background Postgres flush, Postgres-backed single-writer session leases, and synchronous standby replication assigned either by the Postgres control plane or a static standby URL

Telemetry values:

- `true` to expose `/metrics` and record counters/histograms in-process
- `false` to keep only the uptime gauge and skip telemetry updates

## Docker Compose

From the repo root:

```bash
docker compose -f docker-compose.rust.yml up --build
```

That starts:

- Postgres on host port `5433` by default
- `starcite-rs` on host port `4001` by default
- ops endpoints on host port `4002` by default

If those collide with something local, override them when you start the stack:

```bash
STARCITE_RS_HOST_PORT=4011 STARCITE_RS_OPS_HOST_PORT=4012 STARCITE_RS_DB_HOST_PORT=5434 \
  docker compose -f docker-compose.rust.yml up --build
```

For multi-node `local_async` drills, leave the single-node compose file as-is and start each Rust node
with its own `LOCAL_ASYNC_NODE_PUBLIC_URL=http://host:public_port` plus
`LOCAL_ASYNC_NODE_OPS_URL=http://host:ops_port`. That lets Postgres assign live standbys without
hard-coding one peer per process, and it gives non-owner nodes enough metadata to return an owner
hint instead of a blind `409`. Keep `LOCAL_ASYNC_STANDBY_URL` empty unless you want the older
static 2-node fallback. On a fresh database, non-migrator nodes will now wait for the
`control_nodes` table to appear before they start heartbeating into the Postgres control plane.
When you benchmark that multi-node shape with the existing k6 hot-path script, set
`SESSION_ROUTE_MODE=designated_owner` if you want the script to hash each session onto the same
owner key Postgres uses for lease selection. Leave it at the default `round_robin` mode when you
want to measure wrong-node proxy behavior instead.

## Example

```bash
curl -X POST http://localhost:4001/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "tenant_id": "acme",
    "creator_principal": {
      "tenant_id": "acme",
      "id": "user-42",
      "type": "user"
    },
    "metadata": {"workflow": "contract"}
  }'

curl -X POST http://localhost:4001/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "actor": "user:user-42",
    "producer_id": "drafter-1",
    "producer_seq": 1,
    "expected_seq": 0
  }'

curl "http://localhost:4001/v1/sessions/ses_demo/events?cursor=0&limit=100"

curl "http://localhost:4001/v1/sessions/ses_demo/lifecycle/events?cursor=0&limit=100"

curl "http://localhost:4001/v1/lifecycle/events?tenant_id=acme&session_id=ses_demo&cursor=0&limit=100"

curl "http://localhost:4002/health/ready"

curl "http://localhost:4002/metrics"

curl "http://localhost:4002/metrics" | rg 'starcite_(node_draining|runtime_active_sessions|archive_queue_pending_sessions|fanout_active_keys|fanout_subscribers)'

curl "http://localhost:4002/debug/state"

curl -X POST "http://localhost:4002/debug/drain"

curl -X DELETE "http://localhost:4002/debug/drain"
```

To tail replay plus live events over WebSocket:

```bash
node -e '
const ws = new WebSocket("ws://localhost:4001/v1/sessions/ses_demo/tail?cursor=0&batch_size=128");
ws.onmessage = (event) => {
  const frame = JSON.parse(event.data);
  if (frame.events) console.log("events", frame.events);
  if (frame.type === "gap") console.log("gap", frame);
};
'
```

To replay then stream session-scoped lifecycle events:

```bash
node -e '
const ws = new WebSocket("ws://localhost:4001/v1/sessions/ses_demo/lifecycle?cursor=0");
ws.onmessage = (event) => console.log(JSON.parse(event.data));
'
```

To replay then stream tenant-scoped lifecycle events:

```bash
node -e '
const ws = new WebSocket("ws://localhost:4001/v1/lifecycle?tenant_id=acme&session_id=ses_demo&cursor=0");
ws.onmessage = (event) => console.log(JSON.parse(event.data));
'
```

With the default runtime settings, that socket will see `session.activated` immediately on create,
`session.created` after the row is stored, and `session.freezing` plus `session.frozen` once a
session has been idle for `SESSION_RUNTIME_IDLE_TIMEOUT_MS`. The next read, tail, or append on a
frozen session emits `session.hydrating` and then `session.activated`. When drain starts, an
already-open raw lifecycle or tail socket receives `{"type":"node_draining","reason":"node_draining","drain_source":"shutdown","retry_after_ms":N}`
and then closes with code `1012`.

In `jwt` mode, use a `token` query param instead of `tenant_id` and let the token principal
derive the tenant:

```bash
node -e '
const token = "<jwt>";
const ws = new WebSocket(`ws://localhost:4001/v1/sessions/ses_demo/lifecycle?token=${token}&cursor=0`);
ws.onmessage = (event) => console.log(JSON.parse(event.data));
'
```

To connect with Phoenix channel framing on one shared socket:

```bash
node -e '
const ws = new WebSocket("ws://localhost:4001/v1/socket/websocket?vsn=2.0.0&tenant_id=acme");
ws.onopen = () => {
  ws.send(JSON.stringify(["1", "1", "lifecycle:ses_demo", "phx_join", { cursor: 0 }]));
  ws.send(JSON.stringify(["2", "2", "tail:ses_demo", "phx_join", { cursor: 0, batch_size: 128 }]));
  ws.send(JSON.stringify([null, "3", "phoenix", "heartbeat", {}]));
};
ws.onmessage = (event) => console.log(JSON.parse(event.data));
'
```

That Phoenix-compatible socket currently supports `heartbeat`, `phx_join`, and `phx_leave`
for the operator `lifecycle` topic plus `lifecycle:<session_id>` and `tail:<session_id>` topics.
It keeps the normal Phoenix frame array shape `[join_ref, ref, topic, event, payload]`, replies
with `phx_reply`, pushes `lifecycle`, `events`, `gap`, `token_expired`, and `node_draining`, lets one socket
multiplex many session streams, and accepts either `cursor` plus optional `session_id` on the
operator `lifecycle` topic or plain `cursor` on `lifecycle:<session_id>` joins.
When drain starts, active Phoenix topic subscriptions receive a `node_draining` push with
`drain_source` and shutdown `retry_after_ms`, and then the socket closes with code `1012`.

In `jwt` mode, pass the token on the socket URL instead of `tenant_id`:

```bash
node -e '
const token = "<jwt>";
const ws = new WebSocket(`ws://localhost:4001/v1/socket/websocket?vsn=2.0.0&token=${token}`);
ws.onopen = () => {
  ws.send(JSON.stringify(["1", "1", "lifecycle:ses_demo", "phx_join", { cursor: 0 }]));
  ws.send(JSON.stringify(["2", "2", "tail:ses_demo", "phx_join", { cursor: 0 }]));
};
ws.onmessage = (event) => console.log(JSON.parse(event.data));
'
```

The Prometheus surface is intentionally narrow. Right now it exports:

- `starcite_edge_http_total` and `starcite_edge_http_duration_ms`
- `starcite_edge_stage_total` and `starcite_edge_stage_duration_ms`
- `starcite_auth_total` and `starcite_auth_duration_ms`
- `starcite_ingest_edge_total`
- `starcite_request_total` and `starcite_request_duration_ms`
- `starcite_read_total` and `starcite_read_duration_ms`
- `starcite_socket_connections` and `starcite_socket_subscriptions`
- `starcite_session_create_total`, `starcite_session_freeze_total`, and `starcite_session_hydrate_total`
- `starcite_process_uptime_seconds`

The socket gauges reflect currently open raw and Phoenix transports plus active raw stream
subscriptions and Phoenix topic joins. Raw tail and lifecycle handlers now keep reading the socket
for close and ping frames, so those gauges clear promptly when a quiet client disconnects instead
of waiting for the next broadcast.
