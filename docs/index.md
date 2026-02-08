---
title: Starcite
slug: /
sidebar_position: 0
---

# Starcite

Starcite is the session semantics layer for AI experiences.

Core primitives:

- `create`: start a session
- `append`: write one ordered event
- `tail`: catch up from `cursor`, then stream live over WebSocket

## Contract guarantees

- Monotonic per-session ordering via `seq`.
- Durable append acknowledgements.
- Replay + live follow semantics for `tail`.
- One append contract for humans and agents.
- Opaque metadata and payloads (application-defined).

## Out of scope

- Authn/authz enforcement (upstream concern).
- Prompt construction and token-window policy.
- Agent scheduling/orchestration policy.
- Outbound webhook delivery.

## Start here

- [Quick start](./usage/quickstart.md)
- [REST API](./api/rest.md)
- [WebSocket API](./api/websocket.md)
- [Architecture](./architecture.md)
- [Storage](./storage.md)
- [Deployment](./deployment.md)
- [Benchmarks](./benchmarks.md)
