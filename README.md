<h1 align="center">Starcite</h1>

<p align="center">
  Durable session event storage for multi-agent LLM systems.
</p>

<p align="center">
  <a href="https://github.com/fastpaca/starcite/actions/workflows/test.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
  <a href="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg" alt="Docker Build"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://elixir-lang.org/"><img src="https://img.shields.io/badge/Elixir-1.18.4-blue.svg" alt="Elixir"></a>
</p>

AI apps get fragile when streams break. Refresh a page, lose connectivity, or redeploy a server, and session state can diverge from what the model actually produced. Starcite removes that class of failure by making the session history explicit and durable.

Every append is persisted before it is acknowledged to the client. Every consumer can resume from a cursor, and every consumer sees the same deterministic sequence.

## Why Starcite exists

If you've built an LLM-powered product, you've probably duct-taped together some
combination of Redis pub/sub, Postgres polling, SSE endpoints, and custom reconnect
logic. It works — until it doesn't. Events vanish during deploys. Streams diverge
between tabs. Agents and humans see different histories.

Starcite replaces that duct tape. It provides an ordered, durable event log per
session where every event is persisted before acknowledgment, any client can replay
from a cursor, and the sequence is the same for every consumer. You stop building
streaming infrastructure and start building your actual product.

## What you get

| Capability | Description |
| --- | --- |
| Ordered session sequence | Strictly monotonic `seq` per session |
| Durable writes | Appends are committed before client acknowledgement |
| Replay and resume | Resume from any cursor via tail endpoint |
| Concurrency safety | Optional `expected_seq` conflict checks |
| Idempotency | `deduped` responses by `(producer_id, producer_seq)` |
| Reconnect semantics | Same stream contract across clients, devices, and environments |

## Quick start

```bash
git clone https://github.com/fastpaca/starcite
cd starcite
docker compose up -d
```

```bash
# 1) Create a session
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "metadata": {"tenant_id": "acme"}
  }'

# 2) Append an event
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type": "content",
    "payload": {"text": "Reviewing clause 4.2..."},
    "actor": "agent:drafter",
    "producer_id": "agent-drafter-1",
    "producer_seq": 42,
    "expected_seq": 1
  }'

# Response
# {"seq":42,"last_seq":42,"deduped":false}

# 3) Tail from a cursor (WebSocket)
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

## Running Starcite

### Hosted (starcite.ai)

[starcite.ai](https://starcite.ai) runs Starcite for you — zero infrastructure, free
tier, same API. If you don't want to operate distributed infrastructure, start here.

### Self-hosted

Full control over your data and deployment topology. Requires operating a multi-node
cluster with persistent storage and an archive backend. See the
[self-hosting guide](docs/self-hosting.md) for configuration, bootstrap, and
operational runbooks.

## Trade-offs

Honest comparison against the alternatives engineers typically reach for.

**Starcite vs. Redis Pub/Sub + your own persistence:**
Redis pub/sub is fire-and-forget — miss a message, it's gone. Teams end up layering
Redis Streams, Lua scripts, and a Postgres write-behind on top. Starcite replaces
that stack: every event is persisted before ack, and any client can replay from a
cursor. The cost is another service to operate (or use [starcite.ai](https://starcite.ai)).

**Starcite vs. Kafka / Redpanda:**
Kafka is built for high-throughput cross-service event streaming. Starcite is built
for session-scoped ordered logs. Kafka's partition model doesn't map cleanly to
dynamic sessions (topic-per-session doesn't scale, shared partitions lose ordering).
Kafka gives you consumer groups and stream processing; Starcite gives you a simpler
model purpose-built for the session replay problem.

**Starcite vs. Postgres + SSE (the common DIY):**
Works at small scale. Breaks when you need: ordering across concurrent writers,
reconnect with cursor-based replay, horizontal scaling without sticky sessions, and
sub-150ms append latency. You end up reimplementing half of Starcite in application
code. The trade-off: Starcite is another dependency, but it's the dependency that
replaces the duct tape.

**Design trade-offs within Starcite itself:**
- *Consensus replication* adds a small latency floor (~ms) but guarantees no
  acknowledged event is ever lost, even during node failures.
- *Async archival* keeps the hot path fast (archive writes don't block appends) but
  means the archive backend lags behind the live cluster state by seconds.
- *Session-scoped ordering* keeps things simple and fast but means no cross-session
  queries — use the archive backend directly for analytics.
- *Static cluster topology* is simpler to reason about than dynamic membership but
  means adding/removing nodes requires a maintenance window.
- *Append-only log* — events cannot be updated or deleted. The log is immutable by
  design. If you need mutable state, derive it from the event stream.

## What Starcite does not do

- Prompt construction or completion orchestration
- Token budgeting / window management
- OAuth credential issuance
- Client-side sync inference
- Cross-system agent lifecycle orchestration

## Documentation

- [Architecture](docs/architecture.md)
- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)

## Development

```bash
git clone https://github.com/fastpaca/starcite && cd starcite
mix deps.get
mix compile
mix phx.server  # http://localhost:4000
mix precommit   # format + compile (warnings-as-errors) + test
```

## Contributing

1. Fork and create a branch.
2. Add tests for behavior changes.
3. Run `mix precommit` before opening a PR.

## License

Apache 2.0
