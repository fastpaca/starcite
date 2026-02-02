# Fastpaca Message Store

[![Tests](https://github.com/fastpaca/context-store/actions/workflows/test.yml/badge.svg)](https://github.com/fastpaca/context-store/actions/workflows/test.yml)
[![Docker Build](https://github.com/fastpaca/context-store/actions/workflows/docker-build.yml/badge.svg)](https://github.com/fastpaca/context-store/actions/workflows/docker-build.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Elixir](https://img.shields.io/badge/Elixir-1.18.4-purple.svg)](https://elixir-lang.org/)

> **Message backend for AI agents.** An append-only, replayable, ordered message log with streaming updates and optional archival.

Fastpaca is a **message substrate** — it stores and streams messages, but does NOT manage prompt windows, token budgets, or compaction. Cria (or other prompt systems) owns prompt assembly.

```
                      ╔═ fastpaca ════════════════════════╗
╔══════════╗          ║                                   ║░    ╔═optional═╗
║          ║░         ║  ┏━━━━━━━━━━━┓     ┏━━━━━━━━━━━┓  ║░    ║          ║░
║  client  ║░───API──▶║  ┃  Message  ┃────▶┃   Raft    ┃  ║░ ──▶║ postgres ║░
║          ║░         ║  ┃   Log     ┃     ┃  Storage  ┃  ║░    ║ (archive)║░
╚══════════╝░         ║  ┗━━━━━━━━━━━┛     ┗━━━━━━━━━━━┛  ║░    ╚══════════╝░
 ░░░░░░░░░░░░         ║                                   ║░     ░░░░░░░░░░░░
                      ╚═══════════════════════════════════╝░
                       ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
```

- [Documentation](https://docs.fastpaca.com/)
- [Quick start](https://docs.fastpaca.com/usage/quickstart)
- [API Reference](https://docs.fastpaca.com/api/rest)

## What Fastpaca Does

- **Store messages** in append-only conversation logs
- **Stream updates** via WebSocket in real-time
- **Replay by sequence** for gap recovery
- **Archive to Postgres** for full history retention
- **Sub-150ms p99** append latency via Raft consensus

## What Fastpaca Does NOT Do

- Build LLM context windows
- Manage token budgets
- Run compaction policies
- Execute tools or call providers

**Prompt assembly is handled by Cria** (or your prompt system of choice). Fastpaca just stores and streams messages.

## Quick Start

Start container (postgres is optional — data persists in Raft):

```bash
docker run -d \
  -p 4000:4000 \
  -v fastpaca_data:/data \
  ghcr.io/fastpaca/context-store:latest
```

Use the TypeScript SDK:

```ts
import { createClient } from '@fastpaca/fastpaca';

const fastpaca = createClient({ baseUrl: 'http://localhost:4000/v1' });

// Create or get conversation (idempotent)
const conv = await fastpaca.conversation('chat-123', {
  metadata: { user_id: 'u_123', channel: 'web' }
});

// Append messages
await conv.append({ role: 'user', parts: [{ type: 'text', text: 'Hello!' }] });
await conv.append({ role: 'assistant', parts: [{ type: 'text', text: 'Hi there!' }] });

// Get messages for your prompt system
const { messages } = await conv.tail({ limit: 100 });

// Replay from a specific sequence (for gap recovery)
const { messages: replay } = await conv.replay({ from: 50, limit: 50 });
```

## Cria Integration

Fastpaca stores messages; Cria builds the prompt:

```ts
// 1. Fetch messages from Fastpaca
const { messages } = await conv.tail({ limit: 100 });

// 2. Pass to Cria for prompt assembly with your token budget
const prompt = cria.render(messages, { budget: 100_000 });
```

## When to Use Fastpaca

**Good fit:**
- Multi-turn agent conversations with full history retention
- Apps that need real-time message streaming
- Scenarios requiring replay/gap recovery
- Separation of message storage from prompt assembly

**Not a fit:**
- Single-turn Q&A (no conversation state to manage)
- Apps that want server-side prompt management or compaction

---

## Development

```bash
# Clone and set up
git clone https://github.com/fastpaca/context-store
cd context-store
mix setup            # install deps, create DB, run migrations

# Start server on http://localhost:4000
mix phx.server

# Run tests / precommit checks
mix test
mix precommit        # format, compile (warnings-as-errors), test
```

### Storage Tiers

- **Hot (Raft):** Message tail + metadata. 256 Raft groups × 3 replicas for high availability.
- **Cold (optional):** Archiver persists full history to Postgres and acknowledges a high-water mark so Raft can trim older tail segments.

---

## Contributing

We welcome pull requests. Before opening one:

1. Run `mix precommit` (format, compile, test)
2. Add tests for new behaviour
3. Update docs if you change runtime behaviour or message flow

If you use a coding agent, make sure it follows `AGENTS.md`/`CLAUDE.md` and review all output carefully.
