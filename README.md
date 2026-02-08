# Starcite

[![Tests](https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/test.yml)
[![Docker Build](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

Starcite is a session semantics layer for AI products.
Main site: https://starcite.ai

- Start a session
- Append ordered events
- Tail from your cursor, then follow live over WebSocket

That is the product surface.

## API Primitives (`/v1`)

Create a session:

```bash
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"title":"Draft contract","metadata":{"tenant_id":"acme"}}'
```

Append an event:

```bash
curl -X POST http://localhost:4000/v1/sessions/ses_123/append \
  -H "Content-Type: application/json" \
  -d '{
    "type":"content",
    "payload":{"text":"Working..."},
    "actor":"agent:planner"
  }'
```

Tail from cursor over WebSocket:

```bash
ws://localhost:4000/v1/sessions/ses_123/tail?cursor=41
```

Server behavior:

1. Replay committed events where `seq > cursor` in ascending order.
2. Keep streaming new committed events on the same socket.

## Contract Notes

- Ordering is monotonic per session via `seq`.
- Appends are durable before ack.
- `append` is shared by humans and agents.
- `metadata` is application-defined and opaque to Starcite.
- `idempotency_key` is optional.
- `expected_seq` is optional optimistic concurrency.
- Auth is upstream.

## Docs

- [Quick start](docs/usage/quickstart.md)
- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Storage](docs/storage.md)
- [Deployment](docs/deployment.md)
- [Benchmarks](docs/benchmarks.md)

## Development

```bash
# Clone and set up
git clone https://github.com/fastpaca/starcite
cd starcite
mix deps.get
mix compile

# Start server on http://localhost:4000
mix phx.server

# Run tests / precommit checks
mix test
mix precommit        # format, compile (warnings-as-errors), test
```

Optional local 5-node Raft cluster:

```bash
./scripts/start-cluster.sh
./scripts/test-cluster.sh
./scripts/stop-cluster.sh
```

## Contributing

1. Run `mix precommit` before opening a PR.
2. Add tests for behavior changes.
3. Update docs when runtime behavior or contracts change.
