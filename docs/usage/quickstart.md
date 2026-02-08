---
title: Quick Start
sidebar_position: 1
---

# Quick Start

This quick start uses the three session primitives.

## 1. Start FleetLM

```bash
docker run -d \
  -p 4000:4000 \
  -v fleetlm_data:/data \
  ghcr.io/fastpaca/fleetlm:latest
```

## 2. Create a session

```bash
curl -X POST http://localhost:4000/v1/sessions \
  -H "Content-Type: application/json" \
  -d '{"id":"ses_demo","title":"Draft contract","metadata":{"tenant_id":"acme"}}'
```

## 3. Append an event

```bash
curl -X POST http://localhost:4000/v1/sessions/ses_demo/append \
  -H "Content-Type: application/json" \
  -d '{
    "type":"content",
    "payload":{"text":"Working..."},
    "actor":"agent:planner",
    "source":"agent"
  }'
```

## 4. Tail from a cursor over WebSocket

Connect to:

```text
ws://localhost:4000/v1/sessions/ses_demo/tail?cursor=0
```

Behavior:

1. FleetLM replays events with `seq > cursor`.
2. FleetLM keeps streaming new events as they commit.
3. Reconnect with your last processed `seq` as the next cursor.

## 5. Optional write controls

- `idempotency_key`: optional dedupe key for retries.
- `expected_seq`: optional optimistic concurrency guard.

Append response shape:

```json
{
  "seq": 1,
  "last_seq": 1,
  "deduped": false
}
```
