---
title: FleetLM
slug: /
sidebar_position: 0
---

# FleetLM

FleetLM is conversation infrastructure for AI agents: an append-only, replayable conversation log with streaming updates and optional archival. It stores messages and sequence numbers. It does **not** build prompts or manage token budgets.

```
agent backend / workers  ──HTTP──►  FleetLM  ──websocket──►  your UI / services
        (n8n, LangChain,            ordered log + stream         (any stack)
         custom API, etc)
```

## Start here

- [Quick start](./usage/quickstart.md)
- [Getting started](./usage/getting-started.md)
- [Examples](./usage/examples.md)
- [REST API](./api/rest.md)
- [Websocket API](./api/websocket.md)
- [Deployment](./deployment.md)
- [Storage and audit](./storage.md)

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

## Prompt assembly

FleetLM does not build LLM context windows. You choose which messages to send to your model.

- [Prompt assembly patterns](./usage/context-management.md)

## Why this exists

Most LLM apps need a durable, ordered message log and real-time updates. They also need flexible prompt assembly that changes as models, products, and policies evolve. FleetLM handles the log; you control the prompt.
