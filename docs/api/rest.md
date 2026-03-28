# REST API

All public API endpoints are under `/v1`. The API is small by design — three
operations on one resource type (sessions).

Operational endpoints such as `/health/*`, `/metrics`, and `pprof` live on a
separate ops port and are not part of the public API surface.

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
- `session:read` for `GET /v1/sessions` and socket subscriptions over `/v1/socket`
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
- Response includes:
  - `seq`, `last_seq`, `deduped`
  - `cursor` (`seq` of the appended event)
  - `committed_cursor` (current durable frontier as `seq`)

## Behavioral rules

- Append is sequenced per-session; response includes monotonic `seq`.
- On retry with same `(producer_id, producer_seq)`:
  - same payload content → dedupe response with `deduped: true`
  - same IDs + different payload → conflict error
- `expected_seq` enables optimistic concurrency — if the session's current `seq`
  doesn't match, the append is rejected with `409`.
- `tail` replay is ordered by `seq` and continues with live commits after replay.
- Resume discontinuities are explicit via WebSocket `gap` frames (never silent).

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
- `503` owner/routing unavailable, replication quorum unavailable, or routing failure
