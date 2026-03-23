# WebSocket API

Starcite exposes `tail` as a WebSocket endpoint.

## Endpoint

```
ws://HOST/v1/sessions/:id/tail?cursor=41
```

Epoch-aware resume:

```
ws://HOST/v1/sessions/:id/tail?cursor=12:41
```

Optional query params:

- `batch_size` (`1..1000`, default `1`) controls how many events are included per text frame.

Token transport during WebSocket upgrade:

- non-browser clients: `Authorization: Bearer <jwt>` header
- browser clients: include `access_token=<jwt>` query param

Example (browser):

```js
const ws = new WebSocket(
  `wss://HOST/v1/sessions/${sessionId}/tail?cursor=0&access_token=${encodeURIComponent(token)}`
)
```

Starcite redacts `access_token` from application-level logs/telemetry metadata. If you run a reverse proxy or load balancer, redact query strings there too.

JWT requirements for tail:

- valid JWT signature via JWKS
- `session:read` scope
- JWT `tenant_id` must match session tenant
- if JWT has `session_id`, it must match `:id`

Auth behavior:

- missing/invalid/expired token: HTTP `401` during upgrade
- valid token but forbidden by scope/session/tenant policy: HTTP `403` during upgrade
- after successful upgrade, socket lifetime is bounded by token `exp`
- on expiry, server closes with code `4001` and reason `token_expired`

## Semantics

On connect:

1. Replay events where cursor ordering is greater than the provided cursor, in ascending order.
2. Continue streaming newly committed events on the same socket.
3. On reconnect, use the last processed cursor.

## Server frames

When `batch_size=1` (default), Starcite emits one JSON event object per WebSocket text frame:

```json
{
  "epoch": 12,
  "seq": 42,
  "cursor": { "epoch": 12, "seq": 42 },
  "type": "state",
  "payload": { "state": "running" },
  "actor": "agent:researcher",
  "producer_id": "writer_123",
  "producer_seq": 8,
  "source": "agent",
  "metadata": { "role": "worker", "identity": { "provider": "codex" } },
  "refs": { "to_seq": 41, "request_id": "req_123", "sequence_id": "seq_alpha", "step": 1 },
  "idempotency_key": "run_123-step_8",
  "inserted_at": "2026-02-08T15:00:01Z"
}
```

When `batch_size>1`, Starcite emits a JSON array per text frame with up to `batch_size` event objects:

```json
[
  {
    "epoch": 12,
    "seq": 42,
    "cursor": { "epoch": 12, "seq": 42 },
    "type": "state",
    "payload": { "state": "running" },
    "actor": "agent:researcher",
    "producer_id": "writer_123",
    "producer_seq": 8,
    "source": "agent",
    "metadata": { "role": "worker", "identity": { "provider": "codex" } },
    "refs": { "to_seq": 41, "request_id": "req_123", "sequence_id": "seq_alpha", "step": 1 },
    "idempotency_key": "run_123-step_8",
    "inserted_at": "2026-02-08T15:00:01Z"
  }
]
```

Notes:

- server may emit explicit `gap` frames when resume cursor is unavailable or stale
- no `tombstone` event in the primary contract
- no `tail_synced` event
- tail is server-to-client only; inbound client frames are ignored

## Gap frame

When the requested cursor is outside active replay continuity, server emits:

```json
{
  "type": "gap",
  "reason": "cursor_expired",
  "from_cursor": { "epoch": 11, "seq": 120 },
  "next_cursor": { "epoch": 12, "seq": 300 },
  "committed_cursor": { "epoch": 12, "seq": 298 },
  "earliest_available_cursor": { "epoch": 12, "seq": 301 }
}
```

`reason` values:

- `cursor_expired`
- `epoch_stale`
- `rollback`
