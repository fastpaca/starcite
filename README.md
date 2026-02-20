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

## Why Starcite?

Production LLM systems typically need three guarantees to feel “normal”:

1. **Reliable stream recovery** — keep every event in order, even when sockets and tabs churn.
2. **Low-latency append path** — sub-150ms p99 commit path with Raft-backed ordering.
3. **Shared event model** — humans and agents operate against the same authoritative stream.

## What You Get

| Capability | Description |
| --- | --- |
| Ordered session sequence | Strictly monotonic `seq` per session |
| Durable writes | Appends are committed before client acknowledgement |
| Replay and resume | Resume from any cursor via tail endpoint |
| Concurrency safety | Optional `expected_seq` conflict checks |
| Idempotency | `deduped` responses by `(producer_id, producer_seq)` |
| Reconnect semantics | Same stream contract across clients, devices, and environments |
| Scaled durability | Data lives in Raft-backed clusters and archive backends |

## Quick Start

```bash
git clone https://github.com/fastpaca/starcite
cd starcite
docker compose up -d
```

## Core API

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

## Delivery Guarantees

### Consistency

- Deterministic, strictly increasing `seq` per session
- Cursor-based reads return the complete, gap-free stream
- Replay works after refreshes, disconnects, and redeploys

### Concurrency and safety

- `expected_seq` enables optimistic concurrency checks on append
- `producer_id` + `producer_seq` provide deterministic de-duplication
- Archive backends include S3-compatible object storage and Postgres options

### Client simplicity

- No webhook fan-out required
- No custom cursor sync layer in the client
- No session reassembly logic outside the standard API

## What Starcite Does Not Do

- Prompt construction or completion orchestration
- Token budgeting / window management
- OAuth credential issuance
- Client-side sync inference
- Cross-system agent lifecycle orchestration

## API & Semantics

- [REST API](docs/api/rest.md)
- [WebSocket API](docs/api/websocket.md)
- [Architecture](docs/architecture.md)
- [Deployment](docs/deployment.md)
- [Benchmarks](docs/benchmarks.md)

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
