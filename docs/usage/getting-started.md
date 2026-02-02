---
title: Getting Started
sidebar_position: 2
---

# Getting Started

Fastpaca is a message backend for AI agents. It stores append-only conversations and streams updates. It does **not** build LLM prompts or manage token budgets. You decide which messages to send to your model.

## Key terms

- Conversation: a named thread of messages.
- Message: one entry with a `role` and `parts`.
- Part: a typed payload inside a message (must include `type`).
- Seq: strictly increasing sequence number per conversation.
- Version: optimistic concurrency counter per conversation (bumps on each append).
- Tail: pagination from the newest messages backward.
- Replay: fetch messages by sequence range (from a known point).
- Tombstone: conversation is read-only; new appends are rejected.
- Archive: optional cold storage for full history beyond the in-memory tail.
- token_count: optional per-message token count you provide on append.

## Create a conversation

Conversations are created with a unique id you choose. Create them explicitly before appending.

```typescript
import { createClient } from '@fastpaca/fastpaca';

const fastpaca = createClient({ baseUrl: process.env.FASTPACA_URL || 'http://localhost:4000/v1' });

const convo = await fastpaca.conversation('support-123', {
  metadata: { user_id: 'u_123', channel: 'web' }
});
```

## Append messages

Messages are plain objects with a `role` and an array of `parts`. Each part must include a `type` string. This shape is compatible with ai-sdk v5 `UIMessage`, but Fastpaca does not depend on ai-sdk.

```typescript
await convo.append({
  role: 'assistant',
  parts: [
    { type: 'text', text: 'I can help with that.' },
    { type: 'tool_call', name: 'lookup_manual', payload: { article: 'installing' } }
  ],
  metadata: { reasoning: 'User asked for deployment steps.' }
});
```

Each append assigns a deterministic `seq` and increments the conversation `version`.

## Build your prompt

Fastpaca does not build LLM context windows. You choose what to send to your model. A common pattern is to fetch the most recent messages and build a prompt from them.

```typescript
const { messages } = await convo.tail({ limit: 50 });

const { text } = await generateText({
  model: openai('gpt-4o-mini'),
  messages
});

await convo.append({
  role: 'assistant',
  parts: [{ type: 'text', text }]
});
```

## Read messages

### Tail pagination (newest to oldest)

```typescript
// Fetch the last ~50 messages
const latest = await convo.tail({ offset: 0, limit: 50 });

// If the user scrolls, fetch the next page
const older = await convo.tail({ offset: 50, limit: 50 });
```

### Replay by sequence

Use replay when you know the last seq you processed (for example after a websocket gap event).

```typescript
const { messages } = await convo.replay({ from: 120, limit: 100 });
```

## Tombstone a conversation

Tombstoned conversations reject new writes but remain readable.

```typescript
await fastpaca.tombstoneConversation('support-123');
```

## Archive and full history

If you enable the Postgres archive, older messages can be trimmed from the in-memory tail after they are persisted. The REST API only serves messages currently in the tail; for full-history exports, query the archive directly. See [Storage and audit](../storage.md).
