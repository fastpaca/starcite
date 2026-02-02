---
title: REST API
sidebar_position: 1
---

# REST API

All endpoints live under `/v1`. Requests must include `Content-Type: application/json` where applicable. Responses are JSON unless stated otherwise.

## Conversations

### PUT `/v1/conversations/:id`

Create or update a conversation. Idempotent (`PUT`).

```json title="Request body"
{
  "metadata": {
    "project": "support"
  }
}
```

```json title="Response"
{
  "id": "support-123",
  "version": 0,
  "tombstoned": false,
  "last_seq": 0,
  "archived_seq": 0,
  "metadata": { "project": "support" },
  "created_at": "2026-02-02T12:00:00Z",
  "updated_at": "2026-02-02T12:00:00Z"
}
```

`metadata` is optional; when provided it must be a JSON object.

### GET `/v1/conversations/:id`

Returns the conversation record (id, version, tombstone state, metadata, and timestamps).

### DELETE `/v1/conversations/:id`

Tombstones the conversation. Existing messages remain readable, but new messages are rejected. Returns `204 No Content` on success.

## Messages

### POST `/v1/conversations/:id/messages`

Append a message.

```json title="Request body"
{
  "message": {
    "role": "assistant",
    "parts": [
      { "type": "text", "text": "Checking now..." },
      { "type": "tool_call", "name": "lookup", "payload": { "sku": "A-19" } }
    ],
    "token_count": 128,
    "metadata": { "reasoning": "User asked for availability." }
  },
  "if_version": 41
}
```

```json title="Response"
{ "seq": 42, "version": 42, "token_count": 128 }
```

- `token_count` (optional): when provided, the server stores it verbatim. If omitted, the server estimates from text parts.
- `if_version` (optional): optimistic concurrency control. The server returns `409 Conflict` if the current version does not match. Use this to prevent race conditions when multiple clients append simultaneously.

### GET `/v1/conversations/:id/tail`

Retrieve messages with tail-based pagination (newest to oldest). Designed for backward iteration from recent messages, ideal for infinite scroll and message history UIs.

Query parameters:
- `limit` (integer, default: 100): maximum messages to return
- `offset` (integer, default: 0): number of messages to skip from tail (0 = most recent)

```bash
# Get last 50 messages
GET /v1/conversations/demo/tail?limit=50

# Get next page (messages 51-100 from tail)
GET /v1/conversations/demo/tail?offset=50&limit=50

# Get third page (messages 101-150 from tail)
GET /v1/conversations/demo/tail?offset=100&limit=50
```

Response:

```json
{
  "messages": [
    {
      "seq": 951,
      "role": "user",
      "parts": [{ "type": "text", "text": "..." }],
      "token_count": 42,
      "metadata": {},
      "inserted_at": "2026-02-02T12:00:00Z"
    },
    {
      "seq": 952,
      "role": "assistant",
      "parts": [{ "type": "text", "text": "..." }],
      "token_count": 128,
      "metadata": {},
      "inserted_at": "2026-02-02T12:00:15Z"
    }
  ]
}
```

Messages are returned in **chronological order** (oldest to newest in the result). An empty array indicates you have reached the beginning of history.

**Pagination pattern:**
```typescript
let offset = 0;
const limit = 100;

while (true) {
  const { messages } = await fetch(
    `/v1/conversations/${id}/tail?offset=${offset}&limit=${limit}`
  ).then(r => r.json());

  if (messages.length === 0) break; // Reached beginning

  displayMessages(messages);
  offset += messages.length;
}
```

### GET `/v1/conversations/:id/messages`

Replay messages by sequence number range.

Query parameters:
- `from` (integer, default: 0): starting sequence number (inclusive)
- `limit` (integer, default: 100): maximum messages to return

```bash
GET /v1/conversations/demo/messages?from=0&limit=100
```

Response is the same shape as `tail`, ordered by `seq` ascending.

## Health endpoints

- `GET /health/live` - returns `{"status":"ok"}` when the node is accepting traffic.
- `GET /health/ready` - returns `{"status":"ok"}` when the node has joined the cluster and can serve requests.

---

## Error codes

| Status | Meaning | Notes |
| --- | --- | --- |
| `400` | Invalid payload | Schema or validation failure |
| `404` | Not found | Conversation does not exist |
| `409` | Conflict | Version guard failed (`if_version` mismatch) |
| `410` | Gone | Conversation is tombstoned (writes rejected) |
| `429` | Rate limited | Per-node rate limiting (configurable) |
| `500` | Internal error | Unexpected server failure |
| `503` | Unavailable | No Raft quorum available (retry with backoff) |

Errors follow a consistent shape:

```json
{
  "error": "conflict",
  "message": "Version mismatch (current: 84)"
}
```

### Handling version conflicts (409)

When `if_version` is supplied, the server checks the current version before appending. A `409 Conflict` response indicates the version has changed since your last read.

**Retry pattern for network failures:**
1. Read current version (from `GET /conversations/:id` or the append response)
2. Append with `if_version` matching the current version
3. On timeout or 5xx errors, retry the same request (version unchanged)
4. On `409 Conflict`, read the conversation again to get the updated version, then retry with the new version

This provides optimistic concurrency control without requiring per-message idempotency keys.
