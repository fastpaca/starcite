# REST API

All public API endpoints are under `/v1`. The API stays focused on one resource
type (sessions), with separate endpoints for discovery, header mutation,
append, and archive state management.

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
- `session:read` for `GET /v1/sessions`, `GET /v1/sessions/:id`, and socket subscriptions over `/v1/socket`
- `session:append` for `POST /v1/sessions/:id/append`
- `session:create` also covers `POST /v1/sessions/:id/projections`, `POST /v1/sessions/:id/archive`, and `POST /v1/sessions/:id/unarchive`

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
- Archived sessions are excluded by default
- Supports `limit`, `cursor`, `archived`, and metadata filters (exact matching)
- `archived=false` or omitted returns active sessions
- `archived=true` returns archived sessions only
- `archived=all` returns both active and archived sessions

### `GET /v1/sessions/:id`

Fetch one session by id. Requires `session:read`.

- Archived sessions remain readable by id
- Response includes `archived`

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

### `POST /v1/sessions/:id/projections`

Write one or more immutable projection item versions. Requires `session:create`.

- The target session must already exist and the interval must refer to existing raw seqs
- Request body accepts either one item directly or an `items` array
- Required item fields: `item_id`, `version`, `from_seq`, `to_seq`, `payload`
- Optional item fields: `metadata`, `schema`, `content_type`
- `payload` is stored as provided and is opaque to Starcite
- Projection writes are committed as first-class session state
- The same `(item_id, version)` cannot be written twice with different content
- The same `item_id` cannot appear more than once in a single batch
- The latest version of one item cannot overlap the latest version of another item
- Future reads and replay in the default `composed` view substitute the latest version of each covered interval immediately after the session commit succeeds

### `GET /v1/sessions/:id/projections`

List the latest stored version for each projection item. Requires `session:read`.

- Response shape: `{ "items": [...] }`
- Returns only the active latest version of each `item_id`
- Items are sorted by `{from_seq, to_seq}`

### `GET /v1/sessions/:id/projections/:item_id`

Fetch the latest stored version for one projection item. Requires `session:read`.

- Returns `404 projection_item_not_found` when the item does not exist

### `GET /v1/sessions/:id/projections/:item_id/versions`

List every stored version for one projection item. Requires `session:read`.

- Response shape: `{ "items": [...] }`
- Returns the committed version history in ascending version order

### `GET /v1/sessions/:id/projections/:item_id/versions/:version`

Fetch one stored projection item version. Requires `session:read`.

- Returns `404 projection_item_version_not_found` when that version does not exist

### `DELETE /v1/sessions/:id/projections/:item_id`

Delete every stored version for one projection item. Requires `session:create`.

- Deletes the entire version history for that `item_id`
- Individual versions are immutable and cannot be deleted independently
- Once delete succeeds, the item disappears from latest and version-history reads immediately

### `POST /v1/sessions/:id/archive`

Archive one session. Requires `session:create`.

- Archive is reversible and does not delete the event timeline
- Archived sessions stay readable by id and tailable by session id
- Response includes `archived: true`

### `POST /v1/sessions/:id/unarchive`

Restore one archived session to active list results. Requires `session:create`.

- Response includes `archived: false`

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
- tail/replay defaults to a `composed` view that substitutes the latest
  projection items for covered seq ranges; `raw` is the explicit opt-out
- Resume discontinuities are explicit via WebSocket `gap` frames (never silent).
- Archiving is a visibility flag on the session catalog, not an event deletion path.
- Default list results include only active sessions; archived sessions require an explicit filter or direct lookup by id.

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
- `409` also covers projection item version conflicts and overlapping latest projection items
- `404` also covers missing projection items and missing projection item versions
- `429` `event_store_backpressure`
- `503` owner/routing unavailable, replication quorum unavailable, or routing failure
