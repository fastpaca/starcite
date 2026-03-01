<h1 align="center">Starcite</h1>

<p align="center">
  Durable session event log for AI agent systems.
</p>

<p align="center">
  <a href="https://github.com/fastpaca/starcite/actions/workflows/test.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg" alt="Tests"></a>
  <a href="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml"><img src="https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg" alt="Docker Build"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://elixir-lang.org/"><img src="https://img.shields.io/badge/Elixir-1.18.4-blue.svg" alt="Elixir"></a>
</p>

You ship an agent. It streams tool calls, status updates, progress events. A user
switches tabs. The SSE connection dies. They come back — what did the agent do while
they were gone?

That's the problem. Starcite is a session event log where every event is persisted
before acknowledgment, any client can catch up from a cursor, and every consumer sees
the same ordered history. Agents, humans, and UIs all read from the same stream.

## Why this breaks at multi-agent

Single-agent chat hides it. One producer, one consumer, one connection — the
transport is the source of truth and it works fine. Then you add a second agent, or
tool calls, or a human in the loop, and things start breaking:

- **Messages vanish after reconnect.** SSE and WebSocket streams aren't durable.
  Network drops silently lose events. The user comes back to a gap in the
  conversation.
- **Duplicates on reconnect.** Server replays events the client already rendered.
  Same message appears twice. No natural dedup key for text chunks.
- **Tool results without tool calls.** Client reconnects mid-sequence — sees the
  result of a tool call but not the call itself. Orphaned UI.
- **Multi-agent messages out of order.** Two agents emit events simultaneously.
  Network jitter reorders them. Agent B references Agent A's analysis that appears
  *after* the reference. Timestamps aren't a total order.
- **Different tabs show different histories.** Each tab opens its own SSE stream from
  "now." Tab opened 30 seconds later is missing 30 seconds of context.
- **Every deploy breaks all active sessions.** All connections drop simultaneously.
  Graceful shutdown doesn't solve the durability gap.

You didn't mean to build a distributed system. But a multi-agent chat UI *is* a
distributed system — multiple producers, multiple consumers, unreliable networks,
concurrent state mutations. Every fix that treats it as something simpler addresses a
symptom, not the cause.

Starcite is the [ordered, immutable log with cursor-based
tailing](https://starcite.ai/blog/why-agent-uis-lose-messages-on-refresh) underneath
your transport layer. Three API calls:

- **`POST /v1/sessions`** — create a session
- **`POST /v1/sessions/:id/append`** — append an event (persisted before ack)
- **`GET /v1/sessions/:id/tail?cursor=N`** — catch up from cursor, then stream live

## What this gives you

**Streams survive disconnects.** User switches tabs, phone goes to sleep, deploy
happens mid-stream — they reconnect with their last `cursor` and pick up exactly
where they left off. No gaps, no duplicates.

**Multiple writers, one timeline.** Agents, tools, and humans all append to the same
session. Events are sequenced into a single ordered log. No interleaving, no
attribution confusion.

**What the user saw is what was saved.** Events are persisted before the client gets
an ack. If the user saw it, it's in the log. Cancel a run — the partial progress is
still there.

**Reconnect without re-executing.** Clients resume from a cursor, not by re-running
the agent. The log is the source of truth, not ephemeral stream state.

## Quick start

```bash
git clone https://github.com/fastpaca/starcite
cd starcite
docker compose up -d
```

```bash
# Create a session
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "metadata": {"tenant_id": "acme"}
  }'

# Append an event
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

# Response: {"seq":42,"last_seq":42,"deduped":false}

# Tail from a cursor (WebSocket)
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

## Running Starcite

### Hosted (starcite.ai)

[starcite.ai](https://starcite.ai) runs Starcite for you — same API, no
infrastructure to manage.

### Self-hosted

Full control over your data and deployment. Requires operating a multi-node cluster.
See the [self-hosting guide](docs/self-hosting.md) for configuration, bootstrap, and
operational runbooks.

## Trade-offs

**When Starcite fits:** Session-scoped event streams that need durable ordered
delivery, cursor-based replay, and consistent sequencing across multiple producers and
consumers. Tool calls, status updates, agent output — anything where "what happened in
this session" needs to be reliable and replayable.

**When something else is better:** Cross-service event streaming with consumer groups
→ Kafka. Small-scale, low-throughput sessions → Postgres + SSE is simpler. Ephemeral
fan-out without durability → Redis pub/sub.

**What you're taking on:** Starcite is a distributed system. Self-hosting means a
multi-node cluster with persistent storage.
[starcite.ai](https://starcite.ai) exists for teams that don't want that operational
burden.

**Design trade-offs:**
- *Consensus replication* adds a latency floor (~ms per append) in exchange for
  durability across node failures.
- *Async archival* means the archive backend lags the live cluster by seconds.
- *Session-scoped ordering* means no cross-session queries at the API level — use the
  archive backend for analytics.
- *Static cluster topology* means adding or removing nodes requires a maintenance
  window.
- *Append-only log* means events can't be updated or deleted. Derive mutable state
  from the stream.

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
