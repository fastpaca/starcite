# REST API

All endpoints are under `/v1`.

## POST `/v1/sessions`

Create a session.

Request body:

```json
{
  "id": "ses_custom_123",
  "title": "Draft contract",
  "metadata": {
    "tenant_id": "acme",
    "workflow": "contract_drafting"
  }
}
```

Response `201`:

```json
{
  "id": "ses_custom_123",
  "title": "Draft contract",
  "metadata": {
    "tenant_id": "acme",
    "workflow": "contract_drafting"
  },
  "last_seq": 0,
  "created_at": "2026-02-08T15:00:00Z",
  "updated_at": "2026-02-08T15:00:00Z"
}
```

Validation:

- `id`: optional string (if omitted, Starcite generates one)
- `title`: optional string
- `metadata`: optional object

If the chosen `id` already exists, returns `409` with `error: "session_exists"`.

## GET `/v1/sessions`

List sessions through the configured archive adapter-backed catalog.

Query params:

- `limit`: optional positive integer (`<= 1000`, default `100`)
- `cursor`: optional session id cursor for pagination
- metadata filters (exact match):
  - nested form: `metadata[tenant_id]=acme`
  - dotted form: `metadata.tenant_id=acme`

Response `200`:

```json
{
  "sessions": [
    {
      "id": "ses_custom_123",
      "title": "Draft contract",
      "metadata": {
        "tenant_id": "acme",
        "workflow": "contract_drafting"
      },
      "created_at": "2026-02-08T15:00:00Z"
    }
  ],
  "next_cursor": "ses_custom_123"
}
```

`next_cursor` is `null` when no more rows are available.

## POST `/v1/sessions/:id/append`

Append one event.

Request body:

```json
{
  "type": "state",
  "payload": { "state": "running" },
  "actor": "agent:researcher",
  "producer_id": "writer_123",
  "producer_seq": 8,
  "source": "agent",
  "metadata": {
    "role": "worker",
    "identity": { "provider": "codex" }
  },
  "refs": {
    "to_seq": 41,
    "request_id": "req_123",
    "sequence_id": "seq_alpha",
    "step": 1
  },
  "idempotency_key": "run_123-step_8",
  "expected_seq": 41
}
```

Response `201`:

```json
{
  "seq": 42,
  "last_seq": 42,
  "deduped": false
}
```

Validation:

- `type`: required non-empty string
- `payload`: required JSON object
- `actor`: required non-empty string
- `producer_id`: required non-empty string
- `producer_seq`: required positive integer (`>= 1`)
- `source`: optional non-empty string
- `metadata`: optional object
- `refs`: optional object
- `refs.to_seq`: optional non-negative integer
- `refs.request_id`: optional string
- `refs.sequence_id`: optional string
- `refs.step`: optional non-negative integer
- `idempotency_key`: optional non-empty string (stored with event)
- `expected_seq`: optional non-negative integer

Producer dedupe behavior:

- same `producer_id` + same `producer_seq` + same event content: returns original `seq` and `deduped: true`
- same `producer_id` + same `producer_seq` + different content: `409 producer_replay_conflict`
- non-contiguous `producer_seq`: `409 producer_seq_conflict`

Optimistic concurrency behavior:

- `expected_seq` present and mismatched current `last_seq`: `409 expected_seq_conflict`

## Tail

`tail` is a WebSocket primitive, not an HTTP pagination endpoint.
See [WebSocket API](./websocket.md).

## Health endpoints

- `GET /health/live` returns `{"status":"ok"}`.
- `GET /health/ready` returns `{"status":"ok"}`.

## Error shape

All API errors return:

```json
{
  "error": "expected_seq_conflict",
  "message": "Expected seq 41, current seq is 42"
}
```

Status codes:

- `400` invalid payload
- `404` session not found
- `409` expected sequence or producer conflict
- `503` unavailable (quorum/routing failure)
