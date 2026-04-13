# Changelog

All notable changes to Starcite will be documented in this file.

The project keeps one `Unreleased` section for pending work and moves entries
into versioned sections when cutting `vX.Y.Z` tags.

## [Unreleased]

### Added

- Postgres-backed archived event persistence, archive batching limits, and
  one-off cutover tasks for importing legacy S3 exports, reconciling archive
  progress, and verifying cutover readiness.

### Changed

- Shifted the archive system to Postgres as the primary durable store and added
  a temporary legacy-S3 cutover path with double-write, read-through backfill,
  and optional boot backfill for rolling production migration.
- Reworked runtime, Docker, and self-hosting configuration to remove the old
  S3-first archive model and document the cutover release plus final
  Postgres-only cleanup path.

## [0.0.3] - 2026-04-13

### Fixed

- Fresh session claims now recover on the surviving ready nodes after a
  single-node failure once the failed node's lease expires.
- Session reads, replay, and tail cursor lookups can continue from follower
  replicas when the owner is unavailable.

### Performance

- Tail replay and catch-up reads now prefer follower replicas before the owner,
  reducing leader load on busy sessions.

## [0.0.2] - 2026-04-12

### Added

- Phoenix Channel tails on `/v1/socket` with `tail:<session_id>` replay and live
  streaming on the same connection.
- Batched catch-up WebSocket frames and cursor updates that include event
  payloads for easier client reconciliation.
- A service-authenticated `lifecycle` channel for session create, archive, and
  unarchive events.
- Session header updates via `PATCH /v1/sessions/:id`.
- Session archive and unarchive endpoints plus archived-session filtering in the
  durable session catalog.
- Tenant-scoped service JWT auth for API and tail access.
- A dedicated ops listener with `pprof`, recon tooling, and expanded Raft
  leadership and contention telemetry.
- Write and read SLO metrics plus tenant-aware telemetry labeling and richer
  ingest-edge error reporting.
- Canonical hot-path benchmark metrics and external tail catch-up benchmark
  gates.

### Changed

- Pivoted to the static write-node architecture and replaced the earlier write
  path with direct streaming and Khepri-backed routing.
- Decoupled session reads from Raft through a Cachex-backed session store and
  simplified session eviction and hydration flow.
- Simplified auth down to the tenant-scoped service JWT model and removed the
  earlier principal-token path.
- Tightened public replay to use sequence-only cursors and hardened the Phoenix
  tail protocol contract.
- Standardized the Phoenix endpoint on Cowboy and isolated ops traffic from the
  public API listener.
- Hardened the archive model around tenant-segregated S3 layout, manual schema
  migration, and archive bootstrap recovery.
- Sped up Docker publishing with a native multi-arch matrix and follow-up fixes
  to the release image build path.
- Reworked README, trade-off, and self-hosting docs to match the current
  runtime model and deployment surface.

### Fixed

- Raft readiness, drain, undrain, and failover handling across restarts and
  ownership transfer.
- Startup and reconcile availability during rollout plus coordinator nodedown
  bootstrap races.
- Deterministic FSM apply behavior, leadership skew, and command outcome
  visibility in the Raft runtime.
- Append hot-path latency, tail catch-up batching, TailSocket gap recovery, and
  cursor update payload delivery.
- S3 startup bootstrap, archive write recovery, and cold replay behavior across
  archive and hot storage boundaries.
- Permissive default HTTP CORS handling and configurable permissive WebSocket
  origins.
- Runtime config documentation drift and operator visibility into effective
  storage and routing state.

### Performance

- Pipelined hot-path appends under the default raft performance profile.
- Improved Raft tail latency and reduced avoidable work on the public API path.
- Served more session reads from cached state instead of synchronously
  involving Raft on the common path.

## [0.0.1] - 2026-02-18

### Added

- Initial tagged Starcite release.
