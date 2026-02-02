---
title: Examples
sidebar_position: 5
---

# Examples

End-to-end snippets that show how Fastpaca fits into common workflows.

---

## Streaming chat route (Next.js + ai-sdk)

```typescript title="app/api/chat/route.ts"
import { createClient } from '@fastpaca/fastpaca';
import { streamText } from 'ai';
import { openai } from '@ai-sdk/openai';

export async function POST(req: Request) {
  const { conversationId, message } = await req.json();

  const fastpaca = createClient({ baseUrl: process.env.FASTPACA_URL || 'http://localhost:4000/v1' });
  const convo = await fastpaca.conversation(conversationId, { metadata: { channel: 'web' } });

  await convo.append({
    role: 'user',
    parts: [{ type: 'text', text: message }]
  });

  const { messages } = await convo.tail({ limit: 50 });
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

## Non-streaming response (Anthropic)

```typescript
import { createClient } from '@fastpaca/fastpaca';
import { generateText } from 'ai';
import { anthropic } from '@ai-sdk/anthropic';

const fastpaca = createClient({ baseUrl: process.env.FASTPACA_URL || 'http://localhost:4000/v1' });
const convo = await fastpaca.conversation('chat_non_stream', { metadata: { channel: 'web' } });

await convo.append({
  role: 'user',
  parts: [{ type: 'text', text: 'Summarise the release notes.' }]
});

const { messages } = await convo.tail({ limit: 50 });

const { text } = await generateText({
  model: anthropic('claude-3-opus'),
  messages
});

await convo.append({
  role: 'assistant',
  parts: [{ type: 'text', text }]
});
```

---

## Handling websocket gaps

```typescript
channel.on('gap', async ({ expected }) => {
  const { messages } = await convo.replay({ from: expected, limit: 200 });
  // Apply missing messages to your local state
});
```

---

## Switching providers mid-conversation

```typescript
const fastpaca = createClient({ baseUrl: process.env.FASTPACA_URL || 'http://localhost:4000/v1' });
const convo = await fastpaca.conversation('mixed-sources', { metadata: { channel: 'web' } });

await convo.append({
  role: 'user',
  parts: [{ type: 'text', text: 'Explain vector clocks.' }]
});

await convo.append({
  role: 'assistant',
  parts: [{
    type: 'text',
    text: await streamText({
      model: openai('gpt-4o'),
      messages: await convo.tail({ limit: 50 }).then(r => r.messages)
    }).text()
  }]
});

await convo.append({
  role: 'user',
  parts: [{ type: 'text', text: 'Now explain like I\'m five.' }]
});

await convo.append({
  role: 'assistant',
  parts: [{
    type: 'text',
    text: await streamText({
      model: anthropic('claude-3-haiku'),
      messages: await convo.tail({ limit: 50 }).then(r => r.messages)
    }).text()
  }]
});
```

---

## Raw REST calls with curl

```bash
# Append tool call output
curl -X POST http://localhost:4000/v1/conversations/support/messages \
  -H "Content-Type: application/json" \
  -d '{
    "message": {
      "role": "assistant",
      "parts": [
        { "type": "text", "text": "Fetching the latest logs..." },
        { "type": "tool_call", "name": "fetch-logs", "payload": {"tail": 200} }
      ]
    }
  }'
```

```bash
# Get the last 50 messages
curl "http://localhost:4000/v1/conversations/support/tail?limit=50"
```

---

These examples mirror the SDK helpers, the REST API, and the websocket stream described elsewhere in the docs. Mix and match based on how your application is structured.
