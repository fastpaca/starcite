# Architecture

Starcite runs a Raft-backed state machine that stores session event logs and serves tail streams.

![Starcite Architecture](./img/architecture.png)

## Public contract vs internals

The public API surface is intentionally small:

- `POST /v1/sessions`
- `GET /v1/sessions`
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
| Archiver | Persists committed events to Postgres |
| Session catalog (adapter-backed) | Lists/filter sessions for API reads |

## Request flow

1. **Create**: create session metadata in the shard owning that session id.
2. **Append**: append one event; after quorum commit, ack with `seq`.
3. **Tail**: client connects with `cursor`; runtime replays `seq > cursor`, then streams live commits.
4. **Archive**: committed events are queued for Postgres; archive ack advances `archived_seq` and allows bounded in-memory trimming.

## Ordering and durability

- Ordering is monotonic per session (`seq`).
- Appends are acknowledged only after quorum commit.
- `tail` replay is always ordered ascending by `seq`.
- Client recovery is reconnect + `cursor` (last processed `seq`).
- Append-only event history — no deletes, no updates.
- Producer dedupe uses `(producer_id, producer_seq)` cursors scoped to a session.
- Producer cursor state is bounded by LRU eviction policy per session.
- Idempotent archival writes (`ON CONFLICT DO NOTHING`).
- At-least-once delivery from runtime to archive.

## Storage model

| Tier | Contents |
| --- | --- |
| Hot (Raft) | Session metadata (`last_seq`, `archived_seq`, retention state) + bounded producer cursors |
| Hot mirror (ETS) | In-flight committed events |
| Cold (adapter-backed) | Full event history + session catalog projection |

## Replay behavior

- `tail` replays committed events where `seq > cursor`.
- Runtime trims older hot entries after archive acknowledgement.

## Intentional boundaries

- Starcite does not issue OAuth/JWT credentials or manage auth client lifecycle.
- Starcite does not define domain event vocabularies.
- Starcite does not run agent business logic.
- Starcite does not push outbound webhooks.
