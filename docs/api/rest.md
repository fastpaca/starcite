# REST API

All endpoints are under `/v1`.

## Authentication

Every `/v1` request requires a bearer JWT:

```
Authorization: Bearer <jwt>
```

Starcite validates JWT signatures against configured JWKS keys and enforces claims directly.

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

- `POST /v1/sessions`
  - create a session
  - requires `session:create`
  - optional fields: `id`, `title`, `metadata`
  - if token has `session_id`, `id` must match it (or be omitted)
  - `metadata.tenant_id` is enforced to match JWT `tenant_id` (injected when omitted)

- `GET /v1/sessions`
  - list sessions
  - requires `session:read`
  - always tenant-fenced by JWT `tenant_id`
  - if JWT has `session_id`, result set is constrained to that session id
  - supports `limit`, `cursor`, and metadata filters (exact matching)

- `POST /v1/sessions/:id/append`
  - append one event to a session
  - requires `session:append`
  - session must match JWT `tenant_id`
  - if JWT has `session_id`, `:id` must match it
  - required: `type`, `payload`, `producer_id`, `producer_seq`
  - optional: `source`, `metadata`, `refs`, `idempotency_key`, `expected_seq`
  - `actor` is derived from JWT `sub` when omitted; if provided, it must equal JWT `sub`
  - response: `{"seq", "last_seq", "deduped"}`

- `GET /v1/sessions/:id/tail?cursor=N[&batch_size=M]`
  - WebSocket channel for replay + live stream
  - optional `batch_size` (`1..1000`): when `M > 1`, server emits JSON array frames with up to `M` events per frame
  - requires `session:read`
  - session must match JWT `tenant_id`
  - if JWT has `session_id`, `:id` must match it

- `GET /health/live`
- `GET /health/ready`
  - returns `{"status":"ok","mode":"write_node|router_node"}` when ready
  - returns `{"status":"starting","mode":"...","reason":"raft_sync|router_sync|draining"}` when not ready

## Behavioral rules

- Append is sequenced per-session; response includes a monotonic `seq`.
- On retry with same `(producer_id, producer_seq)`:
  - same payload content -> dedupe response with `deduped: true`
  - same IDs + different payload -> conflict error
- `expected_seq` enables optimistic concurrency.
- `tail` replay is ordered by `seq` and continues with live commits after replay.

## Error shape

Error bodies include:

```
{
  "error": "expected_seq_conflict",
  "message": "Expected seq 41, current seq is 42"
}
```

Common status outcomes:

- `400` invalid payload
- `401` unauthorized
- `403` forbidden by scope/session/tenant policy
- `404` session not found
- `409` expected sequence or producer conflicts
- `503` unavailable (routing or quorum related)
