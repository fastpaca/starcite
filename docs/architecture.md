---
title: Architecture
sidebar_position: 4
---

# Architecture

FleetLM runs a Raft-backed state machine that stores session event logs and serves tail streams.

![FleetLM Architecture](./img/architecture.png)

## Components

| Component | Responsibility |
| --- | --- |
| REST / WebSocket gateway | Validates API input and forwards to runtime |
| Runtime | Applies session create/append commands and cursor queries |
| Raft groups | 256 logical shards, each replicated across three nodes |
| Snapshot manager | Raft snapshots for recovery/log compaction |
| Archiver (optional) | Persists committed events to Postgres |

## Data flow

1. **Create**: create session metadata in the shard owning that session id.
2. **Append**: append one event; after quorum commit, ack with `seq`.
3. **Tail**: client connects with `cursor`; runtime replays `seq > cursor`, then streams live commits.
4. **Archive**: committed events are queued for Postgres; archive ack advances `archived_seq` and allows bounded in-memory trimming.

## Storage model

- Hot (Raft): session metadata + bounded event tail
- Cold (optional Postgres): full event history

## Notes

- Ordering is monotonic per session (`seq`).
- Writes are durable before ack.
- Integrations pull via `tail`; FleetLM does not deliver outbound webhooks.
