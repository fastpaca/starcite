---
title: Architecture
sidebar_position: 4
---

# Architecture

Starcite runs a Raft-backed state machine that stores session event logs and serves tail streams.

![Starcite Architecture](./img/architecture.png)

## Public contract vs internals

The public API surface is intentionally small:

- `POST /v1/sessions`
- `POST /v1/sessions/:id/append`
- `GET /v1/sessions/:id/tail?cursor=N` (WebSocket upgrade)

Everything else in this document exists to preserve one behavior chain: `create -> append -> tail`.

## Runtime components

| Component | Responsibility |
| --- | --- |
| REST / WebSocket gateway | Validates API input and forwards to runtime |
| Runtime | Applies create/append commands and tail replay queries |
| Raft groups | 256 logical shards, each replicated across three nodes |
| Snapshot manager | Raft snapshots for state recovery |
| Archiver (optional) | Persists committed events to Postgres |
| Archive recovery sweep | Rebuilds pending archive work from unarchived Raft state |

## Request flow

1. **Create**: create session metadata in the shard owning that session id.
2. **Append**: append one event; after quorum commit, ack with `seq`.
3. **Tail**: client connects with `cursor`; runtime replays `seq > cursor`, then streams live commits.
4. **Archive (optional)**: committed events are queued for Postgres; archive ack advances `archived_seq` and allows bounded in-memory trimming.

## Ordering and durability model

- Ordering is monotonic per session (`seq`).
- Appends are acknowledged only after quorum commit.
- `tail` replay is always ordered ascending by `seq`.
- Client recovery is reconnect + `cursor` (last processed `seq`).

## Storage model

- Hot (Raft): session metadata + event log
- Cold (optional Postgres): full event history

## Intentional boundaries

- Starcite does not define domain event vocabularies.
- Starcite does not run agent business logic.
- Starcite does not push outbound webhooks.
