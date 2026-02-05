# FleetLM

[![Tests](https://github.com/fastpaca/fleetlm/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/fleetlm/actions/workflows/test.yml)
[![Docker Build](https://github.com/fastpaca/fleetlm/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/fleetlm/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

> **Conversation infrastructure for agents.** An append-only, replayable, ordered message log with streaming updates and optional archival.

If you’re building an agent (or many agents) and want a full conversation history that **just works** as you scale out workers, FleetLM gives you the primitives to do it safely: ordered appends, optimistic concurrency, and a built-in recovery story when consumers miss events.

FleetLM stores and streams messages — it does **not** build prompts, manage token budgets, or decide how much history to include. Your app owns prompt assembly and context policy.

```
                      ╔═ FleetLM ════════════════════════╗
╔══════════╗          ║                                   ║░    ╔═optional═╗
║          ║░         ║  ┏━━━━━━━━━━━┓     ┏━━━━━━━━━━━┓  ║░    ║          ║░
║  client  ║░───API──▶║  ┃  Message  ┃────▶┃   Raft    ┃  ║░ ──▶║ postgres ║░
║          ║░         ║  ┃   Log     ┃     ┃  Storage  ┃  ║░    ║ (archive)║░
╚══════════╝░         ║  ┗━━━━━━━━━━━┛     ┗━━━━━━━━━━━┛  ║░    ╚══════════╝░
 ░░░░░░░░░░░░         ║                                   ║░     ░░░░░░░░░░░░
                      ╚═══════════════════════════════════╝░
                       ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

Docs: [Quick start](https://fleetlm.com/docs/usage/quickstart) · [Examples](https://fleetlm.com/docs/usage/examples) · [REST API](https://fleetlm.com/docs/api/rest) · [Websocket API](https://fleetlm.com/docs/api/websocket) · [Deployment](https://fleetlm.com/docs/deployment)

## Highlights

- **Durable conversation history:** append-only messages per conversation
- **Ordered by default:** strictly increasing `seq` per conversation
- **Real-time delivery:** websocket stream for updates (`message`, `gap`, `tombstone`)
- **Replay & recovery:** read by sequence to rebuild state or recover missed events
- **Multi-writer safe:** optimistic concurrency with `version` + `if_version` (`409 Conflict` on mismatch)
- **Optional long-term retention:** Postgres archive for audit/analytics

Works with any agent backend (n8n, LangChain, Python workers, your own API) as long as it can make HTTP calls.

## Quick start (Docker + curl)

Start a single-node FleetLM (data persists under `/data`):

```bash
docker run -d \
  -p 4000:4000 \
  -v fleetlm_data:/data \
  ghcr.io/fastpaca/fleetlm:latest
```

FleetLM listens on `http://localhost:4000/v1`.

Create a conversation:

```bash
curl -X PUT http://localhost:4000/v1/conversations/demo-chat \
  -H "Content-Type: application/json" \
  -d '{"metadata":{"channel":"web"}}'
```

Append a message:

```bash
curl -X POST http://localhost:4000/v1/conversations/demo-chat/messages \
  -H "Content-Type: application/json" \
  -d '{"message":{"role":"user","parts":[{"type":"text","text":"Hello!"}]}}'
```

Read recent messages:

```bash
curl "http://localhost:4000/v1/conversations/demo-chat/tail?limit=50"
```

## Common patterns

### Multi-writer safety (version guard)

When multiple workers can append to the same conversation, use a version guard:

- Read the current `version`
- Append with `if_version`
- On `409 Conflict`, re-read and retry with the new version

See the REST docs for details: [Handling version conflicts (409)](https://fleetlm.com/docs/api/rest#handling-version-conflicts-409).

### Reconnect & gap recovery (websocket + replay)

If a consumer misses events (disconnect/reconnect), FleetLM can emit a `gap` event with the expected sequence. Replay from there:

```ts
channel.on('gap', async ({ expected }) => {
  const { messages } = await convo.replay({ from: expected, limit: 200 });
  // apply missing messages to your local state
});
```

## TypeScript SDK

```ts
import { createClient } from '@fleetlm/client';

const fleetlm = createClient({ baseUrl: 'http://localhost:4000/v1' });

// Create or get conversation (idempotent)
const convo = await fleetlm.conversation('chat-123', {
  metadata: { user_id: 'u_123', channel: 'web' }
});

// Append messages
await convo.append({ role: 'user', parts: [{ type: 'text', text: 'Hello!' }] });
await convo.append({ role: 'assistant', parts: [{ type: 'text', text: 'Hi there!' }] });

// Read messages for your prompt builder / UI
const { messages } = await convo.tail({ limit: 100 });

// Replay from a specific sequence (for gap recovery)
const { messages: replay } = await convo.replay({ from: 50, limit: 50 });

// Guard writes when multiple workers can append
const info = await fleetlm.getConversation('chat-123');
await convo.append(
  { role: 'assistant', parts: [{ type: 'text', text: 'Safe multi-writer append.' }] },
  { ifVersion: info.version }
);
```

## Websocket streaming

FleetLM uses Phoenix Channels for streaming updates:

```ts
import { Socket } from "phoenix";

const socket = new Socket("ws://localhost:4000/socket/websocket");
socket.connect();

const channel = socket.channel("conversation:demo-chat");
await channel.join();

channel.on("message", (payload) => console.log(payload));
channel.on("gap", (payload) => console.log(payload));
channel.on("tombstone", (payload) => console.log(payload));
```

## Concepts (2-minute mental model)

- **Conversation:** a named thread of messages you choose the id for.
- **Message:** one entry with a `role` and typed `parts`.
- **Part:** a typed payload inside a message (e.g. `{type:"text"}`, `{type:"tool_call"}`).
- **`seq`:** strictly increasing sequence number per conversation (ordering).
- **`version`:** optimistic concurrency counter per conversation (bumps on each append).
- **Tail:** pagination from newest messages backward.
- **Replay:** fetch messages by sequence range.

Non-goals (by design): prompt construction/context windows, token budgets/summarization policy, and tool execution/model/provider calls.

## Learn more

- [Getting started](https://fleetlm.com/docs/usage/getting-started)
- [Examples](https://fleetlm.com/docs/usage/examples)
- [Websocket gap handling](https://fleetlm.com/docs/api/websocket)
- [Prompt assembly patterns](https://fleetlm.com/docs/usage/context-management)
- [Architecture](https://fleetlm.com/docs/architecture) (implementation details live here)

---

## Development

```bash
# Clone and set up
git clone https://github.com/fastpaca/fleetlm
cd fleetlm
mix setup            # install deps, create DB, run migrations (requires Postgres)

# Start server on http://localhost:4000
mix phx.server

# Run tests / precommit checks
mix test
mix precommit        # format, compile (warnings-as-errors), test
```

---

## Contributing

We welcome pull requests. Before opening one:

1. Run `mix precommit` (format, compile, test)
2. Add tests for new behaviour
3. Update docs if you change runtime behaviour or message flow

If you use a coding agent, make sure it follows `AGENTS.md`/`CLAUDE.md` and review all output carefully.
