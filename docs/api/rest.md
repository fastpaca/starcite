# REST API

All public API endpoints are under `/v1`. The API is small by design — four
operations on one resource type (sessions).

Operational endpoints such as `/health/*`, `/metrics`, and `pprof` live on a
separate ops port and are not part of the public API surface.

## Authentication

By default (`STARCITE_AUTH_MODE=jwt`), every `/v1` request requires a bearer JWT:

```
Authorization: Bearer <jwt>
```

Starcite validates JWT signatures against configured JWKS keys and enforces claims
directly. There's no session cookie, no API key — just a signed JWT with the right
claims.

If you explicitly run with `STARCITE_AUTH_MODE=none`, bearer auth is skipped and the
JWT-specific scoping rules below do not apply.

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
- Session tenancy comes from the authenticated principal, not from `metadata`
- `metadata` is stored as provided

### `GET /v1/sessions`

List sessions. Requires `session:read`.

- In JWT mode, results are tenant-fenced by token `tenant_id`
- If JWT has `session_id`, the result set is constrained to that session
- Supports `limit`, `cursor`, and metadata filters (exact matching)

### `PATCH /v1/sessions/:id`

Update mutable session header fields. Requires `session:create`.

Requires `session:create`. Session must match JWT `tenant_id`. If JWT has
`session_id`, `:id` must match it.

- Optional: `title`, `metadata`, `expected_version`
- At least one of `title` or `metadata` must be present
- `title: null` clears the title
- `metadata` uses shallow merge semantics:
  - omitted keys are preserved
  - provided keys overwrite existing keys
  - nested objects are replaced at the top-level key you send
- `metadata` does not support delete semantics; use overwrite-at-key instead
- `expected_version` enables optimistic concurrency:
  - when omitted: last writer wins
  - when present: the update is rejected with `409 expected_version_conflict`
    unless it matches the current session `version`
- Response returns the updated session record, including `id`, `title`,
  `creator_principal`, `metadata`, `last_seq`, `created_at`, `updated_at`, and
  `version`

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
- Session header updates do not change `last_seq`; they only update mutable
  catalog fields, advance `updated_at`, and increment `version`.
- On retry with same `(producer_id, producer_seq)`:
  - same full event content → dedupe response with `deduped: true`
  - same IDs + different event content → conflict error
- `expected_seq` enables optimistic concurrency — if the session's current `seq`
  doesn't match, the append is rejected with `409`.
- `expected_version` enables optimistic concurrency for session header
  mutations — if the session header changed since the caller last observed it,
  the update is rejected with `409` and `current_version`.
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

Session header compare-and-swap conflicts add `current_version`:

```json
{
  "error": "expected_version_conflict",
  "message": "Expected version 3, current version is 4",
  "current_version": 4
}
```

Status codes:

- `400` invalid payload
- `401` unauthorized
- `403` forbidden by scope/session/tenant policy
- `404` session not found
- `409` expected sequence, expected version, or producer conflicts
- `429` `event_store_backpressure`
- `503` owner/routing unavailable, replication quorum unavailable, or routing failure
