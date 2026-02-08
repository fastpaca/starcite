---
title: Storage & Audit
sidebar_position: 2
---

# Storage & Audit

Starcite separates hot runtime storage (Raft) from optional cold storage (Postgres archive).

## Storage tiers

- Hot (Raft)
  - Session metadata (`last_seq`, `archived_seq`, retention state)
  - Ordered event log used for append and tail replay
- Cold (Archive)
  - Full event history in table `events`
  - Composite key `(session_id, seq)`

## Guarantees

- Append-only event history per session.
- Monotonic total order per session via `seq`.
- Idempotent archival writes with `ON CONFLICT DO NOTHING`.
- At-least-once delivery from runtime to archive.

## Replay behavior

- `tail` replays committed events where `seq > cursor`.
- If archive is enabled, runtime can trim older hot entries after archive acknowledgement.
- If archive is disabled, replay is constrained to the retained hot tail window.

## Enabling Postgres archive

```bash
-e STARCITE_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host:5432/db \
-e STARCITE_ARCHIVE_FLUSH_INTERVAL_MS=5000
```

Tail retention and batch size are configurable:

```elixir
config :starcite,
  archive_batch_size: 5_000,
  tail_keep: 1_000
```

## Telemetry (archive subset)

- `starcite_archive_pending_rows`
- `starcite_archive_pending_sessions`
- `starcite_archive_attempted_total`
- `starcite_archive_inserted_total`
- `starcite_archive_bytes_attempted_total`
- `starcite_archive_bytes_inserted_total`
- `starcite_archive_flush_duration_ms`
- `starcite_archive_lag`
- `starcite_archive_tail_size`
- `starcite_archive_trimmed_total`

## Exporting history

With Postgres archive enabled:

```sql
SELECT * FROM events WHERE session_id = $1 ORDER BY seq;
```
