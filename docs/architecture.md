---
title: Architecture
sidebar_position: 4
---

# Architecture

FleetLM runs a Raft-backed state machine that stores conversation logs and streams updates. Every node exposes the same API; requests can land anywhere and are routed to the appropriate shard.

![FleetLM Architecture](./img/architecture.png)

## Components

| Component | Responsibility |
| --- | --- |
| **REST / Websocket gateway** | Validates requests and forwards them to the runtime. |
| **Runtime** | Applies appends, manages conversation metadata, enforces version guards, emits events. |
| **Raft groups** | 256 logical shards, each replicated across three nodes. Provide ordered, durable writes. |
| **Snapshot manager** | Raft snapshots for log compaction and fast recovery (not LLM windows). |
| **Archiver (optional)** | Writes committed messages to Postgres. Leader-only, fed by Raft effects. |

## Data flow

1. **Append** - the node forwards the message to the shard leader; once a quorum commits, the conversation updates and the client receives `{seq, version}`.
2. **Tail/Replay** - reads are served from the in-memory message log, ordered by `seq`.
3. **Archive (optional)** - the leader persists messages to Postgres and acknowledges a high-water mark (`ack_archived(seq)`), allowing the runtime to trim older tail segments while retaining a small bounded tail.
4. **Stream** - PubSub fans out `message`, `tombstone`, and `gap` events to the websocket channel.

## Storage tiers

- **Hot (Raft):**
  - Conversation metadata (version, tombstone state, watermarks)
  - Message tail (newest entries, newest-first internally)
  - Watermarks: `last_seq` (writer), `archived_seq` (trim cursor)
- **Cold (Archive):**
  - Full message history in Postgres (optional component)

## Operational notes

- Leader election is automatic. Losing one node leaves the cluster writable (2/3 quorum).
- Append latency is dominated by network RTT between replicas; keep nodes close.
- Snapshots are small: they include conversation metadata and the message tail. ETS archive buffers are not part of snapshots.
- **Topology coordinator**: The lowest node ID (by sort order) manages Raft group membership. Coordinator bootstraps new groups and rebalances replicas when nodes join/leave. Non-coordinators join existing groups and keep local replicas running. If coordinator fails, the next-lowest node automatically takes over.
