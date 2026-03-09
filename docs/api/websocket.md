# WebSocket API

Starcite exposes session tailing through Phoenix sockets + channels at
`/v1/tail/socket/websocket`.

Each joined `tail:<session_id>` topic replays committed events after a cursor
and then continues with live committed events.

## Auth

JWT requirements for tail channels:

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

## Tail Channels

Endpoint:

```text
ws://HOST/v1/tail/socket/websocket?vsn=2.0.0
```

Auth model:

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
