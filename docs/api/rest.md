# REST API

All endpoints are under `/v1`. The API is small by design — three operations on one
resource type (sessions), plus health checks.

## Authentication

Every `/v1` request requires a bearer JWT:

```
Authorization: Bearer <jwt>
```

Starcite validates JWT signatures against configured JWKS keys and enforces claims
directly. There's no session cookie, no API key — just a signed JWT with the right
claims.

Required JWT claims:

- `iss`
- `aud`
- `exp`
- `tenant_id`
- `sub`
- `scope` (space-delimited) or `scopes` (array)

Optional JWT claim:

- `session_id`
  - when omitted: token is tenant-wide
  - when present: token is locked to that one session

Scopes:

- `session:create` for `POST /v1/sessions`
- `session:read` for `GET /v1/sessions` and `GET /v1/sessions/:id/tail`
- `session:append` for `POST /v1/sessions/:id/append`

Unauthorized requests fail with `401`.

## Endpoints

### `POST /v1/sessions`

Create a session. Requires `session:create`.

- Optional fields: `id`, `title`, `metadata`
- If token has `session_id`, `id` must match it (or be omitted)
- `metadata.tenant_id` is enforced to match JWT `tenant_id` (injected when omitted)

### `GET /v1/sessions`

List sessions. Requires `session:read`.

- Always tenant-fenced by JWT `tenant_id`
- If JWT has `session_id`, result set is constrained to that session
- Supports `limit`, `cursor`, and metadata filters (exact matching)

### `POST /v1/sessions/:id/append`

Append one event to a session. This is the hot path — see
[Architecture](../architecture.md) for what happens under the hood.

Requires `session:append`. Session must match JWT `tenant_id`. If JWT has
`session_id`, `:id` must match it.

- Required: `type`, `payload`, `producer_id`, `producer_seq`
- Optional: `source`, `metadata`, `refs`, `idempotency_key`, `expected_seq`
- `actor` is derived from JWT `sub` when omitted; if provided, it must equal JWT `sub`
- Response: `{"seq", "last_seq", "deduped"}`

### `GET /v1/sessions/:id/tail?cursor=N[&batch_size=M]`

WebSocket upgrade for replay + live stream. See the
[WebSocket API](websocket.md) for frame format and reconnection semantics.

- Optional `batch_size` (`1..1000`): when `M > 1`, server emits JSON array frames
  with up to `M` events per frame
- Requires `session:read`
- Session must match JWT `tenant_id`
- If JWT has `session_id`, `:id` must match it

### `GET /health/live` and `GET /health/ready`

Health checks for load balancers and orchestrators.

- Ready: `{"status":"ok","mode":"write_node|router_node"}`
- Not ready: `{"status":"starting","mode":"...","reason":"raft_sync|router_sync|draining"}`

Use `/health/ready` for routing decisions. A node that reports `starting` is still
syncing state and shouldn't receive traffic yet.

## Behavioral rules

- Append is sequenced per-session; response includes a monotonic `seq`.
- On retry with same `(producer_id, producer_seq)`:
  - same payload content → dedupe response with `deduped: true`
  - same IDs + different payload → conflict error
- `expected_seq` enables optimistic concurrency — if the session's current `seq`
  doesn't match, the append is rejected with `409`.
- `tail` replay is ordered by `seq` and continues with live commits after replay.

## Error shape

All errors follow the same structure:

```json
{
  "error": "expected_seq_conflict",
  "message": "Expected seq 41, current seq is 42"
}
```

Status codes:

- `400` invalid payload
- `401` unauthorized
- `403` forbidden by scope/session/tenant policy
- `404` session not found
- `409` expected sequence or producer conflicts
- `503` cluster unavailable or routing failure
