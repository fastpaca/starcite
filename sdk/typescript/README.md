# FleetLM Message Store

**Message backend for AI agents.** FleetLM is an append-only, replayable message log with streaming updates and optional archival.

This is a **message substrate** — it stores and streams messages, but does NOT manage prompt windows, token budgets, or compaction. Cria (or other prompt systems) handles prompt assembly.

## Installation

```bash
npm install @fleetlm/client
```

## Usage

```typescript
import { createClient } from '@fleetlm/client';

// Create client
const fleetlm = createClient({ baseUrl: 'http://localhost:4000/v1' });

// Create or get conversation (idempotent)
const conv = await fleetlm.conversation('chat-123', {
  metadata: { user_id: 'u_123', channel: 'web' }
});

// Append a message
await conv.append({
  role: 'user',
  parts: [{ type: 'text', text: 'Hello!' }]
});

// Append with optional token count (client-provided metadata)
await conv.append({
  role: 'assistant',
  parts: [{ type: 'text', text: 'Hi there!' }]
}, { tokenCount: 12 });

// Get last 50 messages (tail pagination)
const { messages } = await conv.tail({ limit: 50 });

// Replay from sequence number (for gap recovery)
const { messages: replay } = await conv.replay({ from: 100, limit: 50 });

// Tombstone conversation (soft delete, prevents writes)
await fleetlm.tombstoneConversation('chat-123');
```

## Cria Integration

FleetLM stores messages; Cria builds the prompt. Example flow:

```typescript
// 1. Fetch messages from FleetLM
const { messages } = await conv.tail({ limit: 100 });

// 2. Convert to Cria prompt input and render
// (Cria handles token budgets, compaction, provider formatting)
```

## API

### Client

- `createClient(config)` - Create a FleetLM client
- `client.conversation(id, opts?)` - Get or create a conversation
- `client.getConversation(id)` - Get conversation metadata
- `client.tombstoneConversation(id)` - Tombstone a conversation

### Conversation

- `conv.append(message, opts?)` - Append a message
- `conv.tail({ offset?, limit? })` - Get messages from tail (newest)
- `conv.replay({ from?, limit? })` - Replay messages by sequence

See [the docs](https://fleetlm.com/docs/) for full API reference.
