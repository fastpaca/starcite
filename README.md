# Starcite

[![Tests](https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/test.yml)
[![Docker Build](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

**Session state for AI applications.** Sub-150ms appends. Quorum durability. Zero sync logic on your side.

[starcite.ai](https://starcite.ai)

> [!NOTE]
> Starcite is under active development. Lock dependencies to a release version.

---

## The Problem

AI conversations break. Users refresh, switch devices, lose connection—and the session state fragments. You're left writing sync logic, managing reconnects, and debugging ordering bugs instead of building your product.

## The Solution

Starcite sits between your AI and your users. You append events. Clients tail from a cursor. Starcite handles ordering, durability, and replay. Three endpoints, nothing else.

```
POST   /v1/sessions              # create
POST   /v1/sessions/:id/append   # append (humans and agents)
GET    /v1/sessions/:id/tail     # websocket: replay + live stream
```

---

## Quick Start

```bash
docker compose up -d
```

**Create a session:**

```bash
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"id": "ses_demo", "title": "Draft contract", "metadata": {"tenant_id": "acme"}}'
```

**Append an event:**

```bash
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "actor": "agent:drafter"
  }'
# => {"seq": 1, "last_seq": 1, "deduped": false}
```

**Tail from cursor:**

```
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

Replays events where `seq > cursor`, then streams new events live. On reconnect, pass your last `seq` as the cursor.

---

## What You Get

| Feature | Behavior |
|---------|----------|
| **Ordered** | Monotonic `seq` per session, no gaps |
| **Durable** | Ack only after quorum commit (2 of 3 replicas) |
| **Replayable** | Catch up from any cursor, then follow live |
| **Shared** | One append API for humans and agents |
| **Idempotent** | Optional `idempotency_key` for safe retries |
| **Concurrent** | Optional `expected_seq` for optimistic locking |
| **Archived** | Postgres cold storage for full history |

## How It Works

- **256 Raft groups** with 3 replicas each. Sessions hash to a group.
- **Quorum writes** — appends ack after 2 of 3 replicas commit.
- **Hot path in memory** — no synchronous database lookups on append.
- **Background archival** — committed events flush to Postgres asynchronously.
- **Automatic failover** — leader election on node failure.

Target: **< 150ms p99** append latency.

## What Starcite Doesn't Do

- Auth (your layer, upstream)
- Prompt construction or token management
- Agent orchestration
- Webhooks

---

## Documentation

| Topic | Link |
|-------|------|
| REST API | [docs/api/rest.md](docs/api/rest.md) |
| WebSocket API | [docs/api/websocket.md](docs/api/websocket.md) |
| Architecture | [docs/architecture.md](docs/architecture.md) |
| Deployment | [docs/deployment.md](docs/deployment.md) |
| Benchmarks | [docs/benchmarks.md](docs/benchmarks.md) |
| Local Testing | [docs/local-testing.md](docs/local-testing.md) |

---

## Development

```bash
git clone https://github.com/fastpaca/starcite && cd starcite
mix deps.get && mix compile
mix phx.server       # http://localhost:4000
mix test
mix precommit        # format + compile (warnings-as-errors) + test
```

### Local Cluster

```bash
PROJECT=starcite-it
docker compose -f docker-compose.integration.yml -p "$PROJECT" up -d --build
docker compose -f docker-compose.integration.yml -p "$PROJECT" --profile tools run --rm k6 run /bench/1-hot-path-throughput.js
docker compose -f docker-compose.integration.yml -p "$PROJECT" down -v --remove-orphans
```

See [docs/local-testing.md](docs/local-testing.md) for failover drills and multi-cluster runs.

## Contributing

1. Fork and branch
2. `mix precommit` before opening a PR
3. Add tests for behavior changes

## License

[Apache 2.0](LICENSE)
