# REST API

All endpoints are under `/v1`.

## Authentication

In `STARCITE_AUTH_MODE=jwt`, Starcite accepts two bearer token types:

- service JWTs (customer/backend credentials)
- Starcite-issued principal tokens (short-lived, scoped)

Service JWT format:

```
Authorization: Bearer <jwt>
```

Principal token format:

```
Authorization: Bearer <starcite_principal_token>
```

Unauthorized requests fail with `401`.

## Endpoints

- `POST /v1/sessions`
  - create a session
  - service token or user principal token
  - agent principal tokens are forbidden
  - principal tokens require `session:create` scope
  - required fields for service tokens: `creator_principal: {tenant_id, id, type}`
  - principal tokens cannot override `creator_principal`; creator defaults to the authenticated principal
  - optional fields: `id`, `title`, `metadata`

- `POST /v1/auth/issue`
  - issue short-lived principal token from service auth
  - service token only
  - required: `principal: {tenant_id, id, type}`, `scopes`
  - optional: `session_ids`, `owner_principal_ids`, `ttl_seconds`

- `GET /v1/sessions`
  - list sessions
  - service token: lists all sessions
  - user principal token: requires `session:read` scope and is tenant-fenced to the authenticated principal tenant
  - user principal token: lists sessions whose `creator_principal.id` is the authenticated principal id or one of `owner_principal_ids`
  - agent principal token: forbidden
  - supports `limit`, `cursor`, and metadata filters (for exact matching)

- `POST /v1/sessions/:id/append`
  - append one event to a session
  - required: `type`, `payload`, `producer_id`, `producer_seq`
  - `actor` required for service tokens; optional/derived for principal tokens
  - optional: `source`, `metadata`, `refs`, `idempotency_key`, `expected_seq`
  - response: `{"seq", "last_seq", "deduped"}`

- `GET /v1/sessions/:id/tail?cursor=N`
  - WebSocket channel for replay + live stream

- `GET /v1/ops/status`
  - control-plane and write-node diagnostics snapshot
  - service token only in `jwt` mode

- `GET /v1/ops/ready-nodes`
  - observer-filtered write nodes currently eligible for routing
  - service token only in `jwt` mode

- `POST /v1/ops/drain`
  - marks a write node as draining (routing exclusion only)
  - body optional: `{"node":"write-2@starcite.internal"}`
  - service token only in `jwt` mode

- `POST /v1/ops/undrain`
  - marks a write node as ready again
  - body optional: `{"node":"write-2@starcite.internal"}`
  - service token only in `jwt` mode

- `GET /v1/ops/groups/:group_id/replicas`
  - returns static replica nodes for one write group
  - service token only in `jwt` mode

- `GET /health/live`
- `GET /health/ready`
  - returns `{"status":"ok","mode":"write_node|router_node"}` when ready
  - returns `{"status":"starting","mode":"...","reason":"raft_sync|router_sync"}` when not ready

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
