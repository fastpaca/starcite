---
title: REST API
sidebar_position: 1
---

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

## POST `/v1/sessions/:id/append`

Append one event.

Request body:

```json
{
  "type": "state",
  "payload": { "state": "running" },
  "actor": "agent:researcher",
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
- `source`: optional non-empty string
- `metadata`: optional object
- `refs`: optional object
- `refs.to_seq`: optional non-negative integer
- `refs.request_id`: optional string
- `refs.sequence_id`: optional string
- `refs.step`: optional non-negative integer
- `idempotency_key`: optional non-empty string
- `expected_seq`: optional non-negative integer

Idempotency behavior:

- `idempotency_key` absent: no dedupe lookup
- same key + same event: returns original `seq` and `deduped: true`
- same key + different event: `409 idempotency_conflict`

Optimistic concurrency behavior:

- `expected_seq` present and mismatched current `last_seq`: `409 expected_seq_conflict`

## Tail

`tail` is a WebSocket primitive, not an HTTP pagination endpoint.
See [WebSocket API](./websocket.md).

## Health endpoints

These are operational probes, not part of the product primitive surface.

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
- `409` expected sequence or idempotency conflict
- `503` unavailable (quorum/routing failure)
