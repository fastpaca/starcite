---
title: TypeScript SDK
sidebar_position: 4
---

# TypeScript SDK

These helpers mirror the REST API. They accept plain messages with `role` and `parts` (each part has a `type` string). The shape is compatible with ai-sdk v5 `UIMessage`, but the SDK does not depend on ai-sdk.

```bash
npm install @fastpaca/fastpaca
```

## 1. Create (or load) a conversation

```typescript
import { createClient } from '@fastpaca/fastpaca';

const fastpaca = createClient({
  baseUrl: process.env.FASTPACA_URL ?? 'http://localhost:4000/v1'
});

// Idempotent create/update when options are provided
const convo = await fastpaca.conversation('123456', {
  metadata: { channel: 'web', user_id: 'u_123' }
});
```

`conversation(id)` does not create IDs for you. Call it with options to ensure the conversation exists.

## 2. Append messages

```typescript
await convo.append({
  role: 'assistant',
  parts: [
    { type: 'text', text: 'I can help with that.' },
    { type: 'tool_call', name: 'lookup_manual', payload: { article: 'installing' } }
  ],
  metadata: { reasoning: 'User asked for deployment steps.' }
});

// Optionally pass known token count for accuracy
await convo.append({
  role: 'assistant',
  parts: [{ type: 'text', text: 'OK!' }]
}, { tokenCount: 12 });
```

Messages are stored exactly as you send them and receive a deterministic `seq` for ordering.

## 3. Read messages

```typescript
// Tail pagination (newest to oldest)
const latest = await convo.tail({ offset: 0, limit: 50 });
const previous = await convo.tail({ offset: 50, limit: 50 });

// Replay by sequence
const { messages } = await convo.replay({ from: 0, limit: 100 });
```

## 4. Conversation info and tombstones

```typescript
const info = await fastpaca.getConversation('123456');

await fastpaca.tombstoneConversation('123456');
```

## 5. Prompt assembly

Fastpaca does not build LLM prompts. Use tail or replay to select messages, then pass them to your model.

```typescript
const { messages } = await convo.tail({ limit: 50 });
const { text } = await generateText({ model: openai('gpt-4o-mini'), messages });
await convo.append({ role: 'assistant', parts: [{ type: 'text', text }] });
```

## Error handling

- Append conflicts return `409 Conflict` when you pass `ifVersion` and the conversation version changed (optimistic concurrency control).
- Tombstoned conversations return `410 Gone` for new appends.
- Missing conversations return `404 Not Found`.

Retry pattern for optimistic concurrency:
1. Read current version (`getConversation` or append response)
2. Append with `ifVersion`
3. On `409 Conflict`, read the conversation again and retry

Notes:
- The server estimates token counts by default; pass `tokenCount` when you have an accurate value from your provider.

See the [REST API reference](../api/rest.md) for exact payloads.
