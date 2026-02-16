# WebSocket API

Starcite exposes `tail` as a WebSocket endpoint.

## Endpoint

```
ws://HOST/v1/sessions/:id/tail?cursor=41
```

If `STARCITE_AUTH_MODE=jwt` is enabled, include an `Authorization: Bearer <jwt>` header
during the WebSocket upgrade request.

In JWT mode Starcite accepts:

- service JWT bearer token
- Starcite-issued principal bearer token

Auth behavior:

- Missing/invalid/expired token: HTTP `401` during upgrade
- Valid token but forbidden by scope/session/tenant policy: HTTP `403` during upgrade

## Semantics

On connect:

1. Replay committed events where `seq > cursor`, in ascending order.
2. Continue streaming newly committed events on the same socket.
3. On reconnect, use the last processed `seq` as the next `cursor`.

## Server frames

Starcite emits one JSON event object per WebSocket text frame:

```json
{
  "seq": 42,
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

Notes:

- No `gap` event in the primary contract.
- No `tombstone` event in the primary contract.
- No `tail_synced` event.
- Tail is server-to-client only; inbound client frames are ignored.
- In JWT mode, token validity is re-checked during event delivery. If invalid, sockets close with code `4001` (`token_expired` or `token_invalid`).
