# WebSocket API

Starcite exposes session tails over Phoenix Channels so one client WebSocket can
join many session streams at once. There is no standalone raw `/tail` WebSocket
endpoint.

## Socket

Connect a Phoenix client socket to the base socket endpoint:

```
ws://HOST/v1/socket
```

Pass the JWT as the socket auth param:

- `token`

`access_token` is rejected.

Example:

```js
import { Socket } from "phoenix"

const socket = new Socket("wss://HOST/v1/socket", {
  params: { token }
})

socket.connect()
```

In the default `jwt` auth mode:

- valid JWT signature via JWKS
- `session:read` scope
- JWT `tenant_id` must match the session tenant
- if JWT has `session_id`, it must match the subscribed session

If you explicitly run with `STARCITE_AUTH_MODE=none`, omit `token` and the
JWT-specific checks below do not apply.

Socket auth behavior:

- missing/invalid/expired token: socket connect is rejected
- after a successful connect, joined channels are bounded by token `exp`
- on expiry, a joined channel pushes `token_expired` and terminates

## Channel Topics

### `lifecycle`

Join the tenant-scoped lifecycle topic:

```
lifecycle
```

The server derives tenant scope from the authenticated socket. Clients do not
pass tenant identifiers in the topic or payload.

Lifecycle join behavior in JWT mode:

- requires a backend/service JWT with `session:read`
- rejects non-service JWT principals
- rejects JWTs locked to one `session_id`
- emits live-only notifications; there is no replay cursor

Example:

```js
const lifecycle = socket.channel("lifecycle", {})

lifecycle.join()
  .receive("ok", () => console.log("joined lifecycle"))
  .receive("error", (resp) => console.log("join failed", resp))
```

The channel emits `lifecycle` payloads:

```json
{
  "event": {
    "kind": "session.created",
    "session_id": "ses_123",
    "tenant_id": "acme",
    "title": "Draft",
    "metadata": { "workflow": "contract" },
    "created_at": "2026-03-24T10:00:00Z"
  }
}
```

Starcite emits lifecycle events it owns directly. Current lifecycle kinds
include:

- `session.created`
- `session.activated`
- `session.hydrating`
- `session.freezing`
- `session.frozen`

`session.created` includes immutable session header fields such as `title`,
`metadata`, and `created_at`. Runtime lifecycle events are tenant-scoped and
intentionally minimal: `kind`, `session_id`, and `tenant_id`.

Appended session events are available on `tail:<session_id>` and are not
reinterpreted as lifecycle notifications.

### `tail:<session_id>`

Join one topic per tailed session:

```
tail:<session_id>
```

Join payload:

- `cursor`
  - omit to start from the beginning of the retained stream
  - otherwise provide the last processed `seq`
- `batch_size` (`1..1000`, default `1`) is an integer controlling how many replay events are delivered per push

Example:

```js
const channel = socket.channel(`tail:${sessionId}`, {
  cursor: 41,
  batch_size: 128
})

channel.join()
  .receive("ok", () => console.log("joined"))
  .receive("error", (resp) => console.log("join failed", resp))
```

One socket can join the shared `lifecycle` topic and many `tail:*` topics
without opening more WebSocket connections.

## Semantics

On join:

1. Replay events where cursor ordering is greater than the provided cursor, in ascending order.
2. Continue streaming newly committed events on the same channel.
3. On reconnect, reuse the last processed cursor per session topic.

## Server Events

The channel emits `events` payloads:

```json
{
  "events": [
    {
      "seq": 42,
      "cursor": 42,
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

Notes:

- `events` always contains a list, including live single-event pushes
- replay is chunked according to `batch_size`
- no `tombstone` event in the primary contract
- no `tail_synced` event

## Gap Event

When the requested cursor is outside active replay continuity, the channel emits `gap`:

```json
{
  "type": "gap",
  "reason": "cursor_expired",
  "from_cursor": 120,
  "next_cursor": 300,
  "committed_cursor": 298,
  "earliest_available_cursor": 301
}
```

`reason` values:

- `cursor_expired`
- `resume_invalidated`
