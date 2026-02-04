---
title: FleetLM
slug: /
sidebar_position: 0
---

# FleetLM

FleetLM is a message backend for AI agents: an append-only, replayable conversation log with streaming updates and optional archival. It stores messages and sequence numbers. It does **not** build LLM prompts or manage token budgets.

- Append messages with deterministic `seq` and `version`
- Tail or replay message history
- Stream updates over websocket
- Optional Postgres archive for long-term retention
- Prompt assembly stays in your app (or Cria)

```
                      +-------------------------------+
+---------+           |           FleetLM             |          +-----------+
| client  | -- API -->|  Message Log + Stream (Raft)  | -- opt ->| Postgres  |
+---------+           +-------------------------------+          +-----------+
```

- [Quick start](./usage/quickstart.md)
- [Getting started](./usage/getting-started.md)
- [How it works](./architecture.md)
- [Prompt assembly](./usage/context-management.md)
- [Self-hosting](./deployment.md)
- [Storage and audit](./storage.md)
- [REST API](./api/rest.md)
- [Websocket API](./api/websocket.md)

## Quick taste

```ts
import { createClient } from '@fleetlm/client';

const fleetlm = createClient({ baseUrl: 'http://localhost:4000/v1' });
const convo = await fleetlm.conversation('demo-chat', {
  metadata: { channel: 'web' }
});

await convo.append({
  role: 'user',
  parts: [{ type: 'text', text: 'Hi there' }]
});

// Build your prompt from the log (tail, replay, or your own policy)
const { messages } = await convo.tail({ limit: 50 });
```

## Why this exists

Most LLM apps need a durable, ordered message log and real-time updates. They also need flexible prompt assembly that changes as models, products, and policies evolve. FleetLM handles the log; you control the prompt.
