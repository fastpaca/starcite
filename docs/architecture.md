# Architecture

Starcite is a clustered session-stream service that guarantees ordered delivery and deterministic replay for each session.

![Starcite Architecture](./img/architecture.png)

## Runtime contract

Starcite intentionally supports one behavioral chain:

1. Create a session.
2. Append one event at a time to that session.
3. Tail from a cursor for replay and live stream.
4. Persist committed history to the configured archive adapter for durable replay (`s3` by default, with optional Postgres mode).

## Core behavior

- `append` writes are sequenced into a monotonic per-session `seq`.
- Appends are acknowledged only after commit quorum.
- `tail` always replays committed events where `seq > cursor`, then streams new commits in order.
- On reconnect, clients resume by sending the last processed `seq` as cursor.
- Producer dedupe is enforced via `(producer_id, producer_seq)` to keep retries safe.
- `archived_seq` advances only after durable archival confirmation.

## How components interact

- API receives and validates traffic, then forwards commands to runtime.
- Runtime owns sequence assignment, conflict detection, and session state transitions.
- Sharded state machine provides session locality for ordered progression and replication.
- Archiver flushes committed records to S3-compatible object storage (or Postgres when configured) and signals safe hot-state reclamation.
- Session listing and historical reads can come from the same durable adapter-backed catalog/history.

## Intentional boundaries

- Starcite defines stream behavior, not event meaning.
- Starcite does not issue OAuth/JWT credentials or manage client lifecycle.
- Starcite does not run prompt construction, orchestration, or downstream workflow logic.
- Starcite does not push outbound webhooks.
