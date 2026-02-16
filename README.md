# [starcite](https://starcite.ai)

[![Tests](https://github.com/fastpaca/starcite/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/test.yml) [![Docker Build](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/starcite/actions/workflows/docker-build.yml) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE) [![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

**The session log for multi-agent AI**

Your AI product streams messages to users in real time. When they switch tabs, refresh, or drop WiFi, the stream dies — and the session can vanish.

starcite is the session log underneath: every message, tool call, and status update is persisted before the write returns. Your UI reads from a cursor — catch-up and live streaming are the same operation.

It makes the hard operational work explicit so your app can focus on product behavior, not stream recovery.

**Most AI systems face the same reliability gaps**:

- Response lost on page refresh
- Tab switch kills the stream
- Partial message discarded on error
- Stuck response after disconnect
- Context lost on device switch
- Token-by-token write storms
- Message IDs shift mid-stream
- Multi-step agent state corrupted
- Agent streams tangled in one socket
- Stream can’t resume mid-response
- History gone after redeploy
- Session state diverges across devices
- Ordering breaks on reconnect
- Mobile backgrounding breaks the stream
- SSE/WebSocket reconnection logic complexity
- Expired or moved connections dropping long sessions

Starcite’s contract is explicit and minimal:

1. `POST /v1/sessions` — create
2. `POST /v1/sessions/:id/append` — append one event
3. `GET /v1/sessions/:id/tail?cursor=N` — tail from cursor

That is the full behavioral chain.

## Reliability

**Refresh. It’s all there.**

Connections drop. Pages refresh. Servers redeploy. Messages remain in order with no gaps because every change is durably committed before it is acknowledged.

- Monotonic `seq` per session
- Durable commit on each append
- Replay from any cursor and continue live from that point
- Reconnect by sending your last processed `seq`

## Visibility

**Every agent, every step.**

Tool calls, intermediate results, and handoffs all flow through one session stream. You can inspect all actors together or stream focused agent views as needed.

- Shared event model for humans and agents
- Per-event metadata for provenance and traceability
- Deterministic ordering of interleaved workflows

## Consistency

**Open it anywhere.**

Session readbacks are canonical and replayable. The same session can be opened on desktop, phone, or shared links with matching state.

- Session state is independent of client-side history
- No client-specific sync layer required
- Same `seq` contract applies across consumers

## Quick Start

```bash
docker compose up -d
```

## Example API Flow

1) **Create a session**

```bash
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{
    "id": "ses_demo",
    "title": "Draft contract",
    "metadata": {"tenant_id": "acme"}
  }'
```

1) **Append an event**

```bash
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
```

```json
{"seq": 42, "last_seq": 42, "deduped": false}
```

1) **Tail from a cursor**

```text
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

On reconnect, pass the last processed `seq` to resume.

## What Starcite guarantees

- Ordered event stream: strictly increasing `seq` per session
- Replay safety: no gaps between committed events
- Concurrency control: optional `expected_seq` conflict detection
- Retry safety: dedupe by `(producer_id, producer_seq)`
- Consistent cursor semantics for human and agent flows
- No outbound webhook side effects required

## What Starcite does not do

- Prompt construction
- Token budgeting or window management
- Agent orchestration
- OAuth or credential issuance
- Webhook fan-out
- Custom sync logic on the client

## API & Semantics

See the API reference for full request/response contracts:

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
mix precommit        # format + compile (warnings-as-errors) + test
```

## Contributing

1. Fork and create a branch.
2. Add tests for behavior changes.
3. Run `mix precommit` before opening a PR.

## License

[Apache 2.0](LICENSE)
