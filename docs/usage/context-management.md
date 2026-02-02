---
title: Prompt Assembly
sidebar_position: 3
---

# Prompt Assembly

Fastpaca stores an append-only message log. It does **not** build LLM context windows, enforce token budgets, or perform compaction. Prompt assembly is entirely your responsibility.

This is intentional: different products need different prompt policies, and those policies change as models evolve.

---

## Common strategies

### 1) Tail window

Fetch the most recent messages and send those to your model.

```typescript
const { messages } = await convo.tail({ limit: 50 });
```

If you already have `token_count` per message (from your provider), you can trim the tail client-side by token budget.

### 2) Replay from a known seq

When you need to rebuild a prompt deterministically (or recover after a websocket gap), replay from a sequence boundary.

```typescript
const { messages } = await convo.replay({ from: 0, limit: 500 });
```

### 3) Client-side summaries

Maintain a summary outside Fastpaca (database, cache, vector store), and combine it with a recent tail.

```typescript
const summary = await loadSummary(convoId); // your storage
const { messages } = await convo.tail({ limit: 40 });

const prompt = [
  { role: 'system', parts: [{ type: 'text', text: summary }] },
  ...messages
];
```

You can also append summaries as regular messages if that fits your product, but remember the log is append-only.

### 4) External memory (RAG)

Use Fastpaca as the source of truth for raw messages, and build prompt inputs from your own retrieval system. The message log provides deterministic ordering and auditability.

---

## Token counts

Fastpaca accepts an optional `token_count` per message when you append. If you pass accurate values from your model provider, you can enforce budgets or trimming precisely in your own prompt builder. If you omit it, Fastpaca estimates a count from text parts for observability only.

---

## Recap

- Fastpaca never rewrites messages.
- It does not compact or summarize.
- You control selection, summarization, and ordering for model input.

See [Getting Started](./getting-started.md) for basics and [Examples](./examples.md) for end-to-end flows.
