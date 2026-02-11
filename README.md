# Starcite

> [!NOTE]
> Starcite is a work in progress. Expect rapid iterations and future breaking changes. Always lock your dependencies to a release version.

[![Tests](https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/test.yml)
[![Docker Build](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

**AI sessions that don't break.** Open source. Apache 2.0.

https://starcite.ai

---

Starcite sits between your AI and your users so the session stays correct — through refreshes, reconnects, and device switches. No sync logic on your side.

Three primitives:

1. **Create** a session
2. **Append** ordered events to it
3. **Tail** — catch up from a cursor, then follow live over WebSocket

## Quick Start

```bash
docker compose up -d
```

### Create a session

```bash
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"id": "ses_demo", "title": "Draft contract", "metadata": {"tenant_id": "acme"}}'
```

### Append an event

```bash
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "actor": "agent:drafter"
  }'
```

Response:

```json
{"seq": 1, "last_seq": 1, "deduped": false}
```

### Tail from cursor

```
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

1. Replays committed events where `seq > cursor`.
2. Streams new events live on the same socket.
3. On reconnect, pass your last processed `seq` as the next cursor.

## What you get

- **Ordered** — monotonic `seq` per session, no gaps
- **Durable** — ack only after quorum commit
- **Replayable** — catch up from any cursor, then follow live
- **Shared** — one append API for humans and agents
- **Idempotent** — optional `idempotency_key` for safe retries
- **Concurrent** — optional `expected_seq` for optimistic locking
- **Archived** — Postgres cold storage for full history

## What Starcite doesn't do

- Auth (your layer, upstream)
- Prompt construction or token management
- Agent orchestration
- Webhooks

## Documentation

- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Benchmarks](docs/benchmarks.md)

## Development

```bash
git clone https://github.com/fastpaca/starcite && cd starcite
mix deps.get && mix compile
mix phx.server       # http://localhost:4000
mix test
mix precommit        # format + compile (warnings-as-errors) + test
```

## Contributing

1. Fork and branch.
2. `mix precommit` before opening a PR.
3. Add tests for behavior changes.

## License

[Apache 2.0](LICENSE)
