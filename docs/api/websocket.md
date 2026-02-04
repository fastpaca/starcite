---
title: Websocket
sidebar_position: 2
---

# Websocket API

FleetLM exposes a backend-only websocket for streaming conversation updates in near real time. It is implemented as a Phoenix Channel.

Endpoint:

```
ws://HOST/socket/websocket
```

Topic:

```
conversation:CONVERSATION_ID
```

## Example (Phoenix JS client)

```typescript
import { Socket } from "phoenix";

const socket = new Socket("ws://localhost:4000/socket", {
  params: {}
});

socket.connect();

const channel = socket.channel("conversation:support-123");
channel.join();

channel.on("message", payload => {
  // payload.type === "message"
  console.log(payload);
});

channel.on("tombstone", payload => {
  // payload.type === "tombstone"
  console.log(payload);
});

channel.on("gap", payload => {
  // payload.type === "gap"
  console.log(payload);
});
```

## Event payloads

### Message event

```json
{
  "type": "message",
  "seq": 101,
  "version": 101,
  "message": {
    "role": "assistant",
    "parts": [{ "type": "text", "text": "Got it - checking now." }],
    "metadata": { "source": "agent" },
    "token_count": 27
  }
}
```

Sent whenever a new message is appended to the conversation.

### Tombstone event

```json
{ "type": "tombstone" }
```

Sent when the conversation is tombstoned. New appends will be rejected after this event.

### Gap event

```json
{ "type": "gap", "expected": 120, "actual": 124 }
```

Indicates the client missed messages. Fetch the missing range via replay:

```
GET /v1/conversations/:id/messages?from=120&limit=100
```

## Notes

- This websocket is intended for backend-to-backend use. If you need browser updates, fan out through your own gateway (SSE, WebSocket, Pub/Sub).
- There are no compaction or prompt-window events. This service only streams message-log updates.
