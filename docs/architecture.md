# Architecture

Starcite runs a thin-FSM Raft runtime where payload events are mirrored into
node-local ETS and replayed from ETS/cold archive tiers.

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
| Event store (ETS) | Node-local payload mirror keyed by `{session_id, seq}` |
| Session catalog (archive adapter) | Lists/filter sessions for API reads |

## Request flow

1. **Create**: create session metadata in the shard owning that session id and project one catalog row.
2. **Append**: append one event; after quorum commit, ack with `seq`.
3. **Tail**: client connects with `cursor`; runtime replays `seq > cursor`, then streams live commits.
4. **Archive**: committed events are persisted asynchronously; archive ack advances `archived_seq` and allows deterministic ETS release.

## Ordering and durability

- Ordering is monotonic per session (`seq`).
- Appends are acknowledged only after quorum commit.
- `tail` replay is always ordered ascending by `seq`.
- Client recovery is reconnect + `cursor` (last processed `seq`).
- Append-only event history — no deletes, no updates.
- Idempotent archival writes (`ON CONFLICT DO NOTHING`).
- At-least-once delivery from runtime to archive.

## Storage model

| Tier | Contents |
| --- | --- |
| Hot metadata (Raft) | Session metadata (`last_seq`, `archived_seq`, retention/idempotency state) |
| Hot payloads (ETS) | Node-local event payloads keyed by `{session_id, seq}` |
| Cold (archive adapter) | Full event history + session catalog projection |

## Replay behavior

- `tail` replays committed events where `seq > cursor`.
- Hot/live replay reads from local ETS.
- Cold replay reads from archive adapter through Cachex.
- Archive acknowledgement advances `archived_seq` and releases ETS entries below that cursor.

## Capacity and degradation

- Event payload mirroring is bounded by ETS memory backpressure limits.
- When backpressure triggers, appends are rejected with explicit error (`event_store_backpressure`) rather than silently dropping or blocking forever.
- Readiness remains red until local assigned Raft groups are running, reducing rollout-time partial availability.

## Intentional boundaries

- Starcite does not define domain event vocabularies.
- Starcite does not run agent business logic.
- Starcite does not push outbound webhooks.
- Connection draining for rolling updates is infrastructure-level (LB + readiness), not a custom runtime drain mode.
