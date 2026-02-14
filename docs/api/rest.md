# REST API

All endpoints are under `/v1`.

## Authentication

If `STARCITE_AUTH_MODE=jwt`, requests must include:

```
Authorization: Bearer <jwt>
```

Unauthorized requests fail with a 401-style error response.

## Endpoints

- `POST /v1/sessions`
  - create a session
  - optional fields: `id`, `title`, `metadata`

- `GET /v1/sessions`
  - list sessions
  - supports `limit`, `cursor`, and metadata filters (for exact matching)

- `POST /v1/sessions/:id/append`
  - append one event to a session
  - required: `type`, `payload`, `actor`, `producer_id`, `producer_seq`
  - optional: `source`, `metadata`, `refs`, `idempotency_key`, `expected_seq`
  - response: `{"seq", "last_seq", "deduped"}`

- `GET /v1/sessions/:id/tail?cursor=N`
  - WebSocket channel for replay + live stream

- `GET /health/live`
- `GET /health/ready`

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
- `401` unauthorized (JWT mode only)
- `404` session not found
- `409` expected sequence or producer conflicts
- `503` unavailable (routing or quorum related)
