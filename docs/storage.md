---
title: Storage & Audit
sidebar_position: 2
---

# Storage & Audit

Fastpaca separates hot runtime storage (Raft) from optional cold storage used for long-term history and audit. This page covers what is stored where, how retention works, and how to enable an audit-grade archive.

---

## Storage tiers

- Hot (Raft)
  - Message tail (newest entries in memory)
  - Conversation metadata and watermarks: `last_seq` (writer position), `archived_seq` (trim cursor)
- Cold (Archive)
  - Full message history per conversation for analytics and compliance
  - Built-in adapter: Postgres

Raft snapshots include conversation metadata and the in-memory tail. Cold storage holds the full immutable history. The runtime acknowledges cold-store progress via `ack_archived(seq)` which allows trimming older portions of the tail while keeping a safety buffer (`tail_keep`).

---

## Audit goals and guarantees

- Append-only history keyed by `(conversation_id, seq)` with a total order per conversation.
- Idempotent archival: duplicates are ignored (`ON CONFLICT DO NOTHING`).
- Messages are immutable.
- At-least-once delivery from runtime to archive with batch retries and back-pressure signaling via telemetry.

For legal/compliance export, query the archive by `conversation_id` ordered by `seq`, or page the live tail for recent history if you have not enabled an archive.

---

## Enabling the Postgres archive

The Postgres adapter is built in and disabled by default. Enable it via env and provide a database URL. Migrations are run by your release process (see `Fastpaca.ReleaseTasks.migrate/0`).

```bash
-e FASTPACA_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host:5432/db \
-e FASTPACA_ARCHIVE_FLUSH_INTERVAL_MS=5000
```

Details
- Migrations create the `messages` table with a composite primary key `(conversation_id, seq)` and an index on `(conversation_id, inserted_at)`.
- Inserts are chunked and idempotent. The adapter uses `ON CONFLICT DO NOTHING` so replays are safe.
- The runtime computes a contiguous `upto_seq` watermark for each flush; once acknowledged, older tail segments may be trimmed while retaining `tail_keep` messages.

You can adjust batch size and tail retention via application config:

```elixir
config :fastpaca,
  archive_batch_size: 5_000,
  tail_keep: 1_000
```

---

## Retention and trimming

- The runtime never trims messages newer than `archived_seq`.
- If archiving is disabled, all messages remain in the Raft tail. Use the tail API to export periodically for compliance.

---

## Telemetry and monitoring

Key Prometheus series exposed via `/metrics` (subset):

- `fastpaca_archive_pending_rows` / `fastpaca_archive_pending_conversations`
- `fastpaca_archive_attempted_total` / `fastpaca_archive_inserted_total`
- `fastpaca_archive_bytes_attempted_total` / `fastpaca_archive_bytes_inserted_total`
- `fastpaca_archive_flush_duration_ms`
- `fastpaca_archive_lag` (per conversation)
- `fastpaca_archive_tail_size` and `fastpaca_archive_trimmed_total`

Use logs as a secondary audit trail. Structured logs include fields like `type`, `conversation_id`, and `seq` suitable for ingestion by your logging stack.

---

## Exporting history

- With Postgres: `SELECT * FROM messages WHERE conversation_id = $1 ORDER BY seq;`
- Without archive: iterate the live tail via `GET /v1/conversations/:id/tail?offset=...&limit=...` until empty and persist externally.

For large conversations, prefer server-side exports from your data store and page by `seq`.
