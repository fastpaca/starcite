---
title: Quick Start
sidebar_position: 1
---

# Quick Start

## 1. Run FleetLM

```bash
docker run -d \
  -p 4000:4000 \
  -v fleetlm_data:/data \
  ghcr.io/fastpaca/fleet-lm:latest
```

FleetLM listens on `http://localhost:4000/v1`. The container persists data under `fleetlm_data/`.

---

## 2. Create a conversation

Create a conversation with the id `demo-chat`.

```bash
curl -X PUT http://localhost:4000/v1/conversations/demo-chat \
  -H "Content-Type: application/json" \
  -d '{
    "metadata": { "channel": "web" }
  }'
```

---

## 3. Append a message

Add a user message with `How do I deploy this?` as text.

```bash
curl -X POST http://localhost:4000/v1/conversations/demo-chat/messages \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "role": "user",
      "parts": [{ "type": "text", "text": "How do I deploy this?" }]
    }
  }'
```

FleetLM replies with the assigned sequence number and version:

```json
{ "seq": 1, "version": 1, "token_count": 24 }
```

Use `if_version` to prevent race conditions when multiple clients append simultaneously. See the [REST API docs](../api/rest.md#messages) for retry patterns.

---

## 4. Read recent messages

Fetch the most recent messages with tail pagination:

```bash
curl "http://localhost:4000/v1/conversations/demo-chat/tail?limit=50"
```

Response (trimmed):

```json
{
  "messages": [
    {
      "seq": 1,
      "role": "user",
      "parts": [{ "type": "text", "text": "How do I deploy this?" }]
    }
  ]
}
```

---

## 5. Build a prompt (SDK example)

```typescript title="app/api/chat/route.ts"
import { createClient } from '@fleetlm/client';
import { streamText } from 'ai';
import { openai } from '@ai-sdk/openai';

export async function POST(req: Request) {
  const { conversationId, message } = await req.json();

  const fleetlm = createClient({ baseUrl: process.env.FLEETLM_URL || 'http://localhost:4000/v1' });
  const convo = await fleetlm.conversation(conversationId, { metadata: { channel: 'web' } });

  // Append user message
  await convo.append({
    role: 'user',
    parts: [{ type: 'text', text: message }]
  });

  // Build your prompt from the log (here: last 50 messages)
  const { messages } = await convo.tail({ limit: 50 });

  // Stream response and append assistant output
  return streamText({
    model: openai('gpt-4o-mini'),
    messages,
  }).toUIMessageStreamResponse({
    onFinish: async ({ responseMessage }) => {
      await convo.append(responseMessage);
    },
  });
}
```

---

Ready to go deeper? Continue with [Getting Started](./getting-started.md) or jump straight to the [API reference](../api/rest.md).
