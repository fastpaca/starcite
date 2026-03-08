# WebSocket API

Starcite exposes two tail transports:

- raw session WebSocket at `/v1/sessions/:id/tail`
- Phoenix socket + channels at `/v1/tail/socket/websocket`

Both transports replay committed events after a cursor and then continue with
live committed events.

## Auth

JWT requirements for all tail transports:

- valid JWT signature via JWKS
- `session:read` scope
- JWT `tenant_id` must match the session tenant
- if JWT has `session_id`, it must match the tailed session

Appending over an established tail transport requires `session:append` in
addition to the read access needed to join or upgrade the tail itself.

Token transport:

- non-browser clients can use `Authorization: Bearer <jwt>`
- browser clients can use `access_token=<jwt>`

Starcite redacts `access_token` from application-level logs and telemetry
metadata. If you run a reverse proxy or load balancer, redact query strings
there too.

## Raw Tail WebSocket

Endpoint:

```text
ws://HOST/v1/sessions/:id/tail?cursor=41
```

Optional query params:

- `batch_size` (`1..1000`, default `1`) controls how many events are included per text frame.

Raw tail auth behavior:

- missing, invalid, or expired token: HTTP `401` during upgrade
- valid token but forbidden by scope, session, or tenant policy: HTTP `403` during upgrade
- after successful upgrade, socket lifetime is bounded by token `exp`
- on expiry, server closes with code `4001` and reason `token_expired`

When `batch_size=1` (default), Starcite emits one JSON event object per text
frame:

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

When `batch_size>1`, Starcite emits a JSON array per text frame with up to
`batch_size` event objects.

Raw tail also accepts append frames from the client. The frame must be a JSON
text frame with this shape:

```json
{
  "type": "append",
  "ref": "append-1",
  "event": {
    "type": "content",
    "payload": { "text": "hello" },
    "producer_id": "writer_123",
    "producer_seq": 8
  }
}
```

The `event` object uses the same append payload as `POST /v1/sessions/:id/append`.

Successful append acks are emitted as JSON text frames:

```json
{
  "type": "ack",
  "ref": "append-1",
  "seq": 42,
  "last_seq": 42,
  "deduped": false
}
```

Append failures are emitted as JSON text frames:

```json
{
  "type": "error",
  "ref": "append-1",
  "error": "forbidden_scope",
  "message": "Token scope does not allow this operation"
}
```

## Phoenix Tail Channels

Endpoint:

```text
ws://HOST/v1/tail/socket/websocket?vsn=2.0.0
```

Phoenix auth model:

- socket connection is authenticated once in `TailUserSocket.connect/3`
- each topic join is authorized independently in `TailChannel.join/3`
- token expiry disconnects the Phoenix socket and all joined tail topics
- one Phoenix socket can join multiple `tail:<session_id>` topics concurrently

Topic model:

```text
tail:<session_id>
```

Join payload:

- `cursor` (`>= 0`, default `0`)
- `batch_size` (`1..1000`, default `1`)

Server pushes:

- event: `"events"`
- payload:

```json
{
  "events": [
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
  ]
}
```

Client pushes:

- event: `"append"`
- payload: the same append payload accepted by `POST /v1/sessions/:id/append`

Successful channel append replies use the normal Phoenix `"ok"` reply payload:

```json
{
  "seq": 42,
  "last_seq": 42,
  "deduped": false
}
```

Failed channel append replies use the normal Phoenix `"error"` reply payload:

```json
{
  "error": "forbidden_scope",
  "message": "Token scope does not allow this operation"
}
```
