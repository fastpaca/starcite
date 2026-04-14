# starcite-rs parity spec

This file pins the rewrite target so the Rust code stops drifting into a new product.

## objective

Implement the existing Starcite system semantics in Rust with cleaner boundaries and stronger types.

The rewrite target is:

- Rust data plane and edge implementation
- Postgres as the durable control plane and archive store
- no S3
- no alternate synchronous-Postgres write mode
- no redesign of the client-visible append contract

## required parity decisions

### write acknowledgement model

The Rust rewrite must preserve the old acknowledgement shape.

- Append acknowledgement is not tied to a Postgres insert transaction.
- Append acknowledgement is tied to the hot write path after ownership validation and standby replication succeed.
- Postgres remains the durable control plane for ownership and the durable archive store for replay and recovery.
- Background flush persists acknowledged events to Postgres after the hot-path ack.
- `cursor` and `committed_cursor` are separate concepts. They must not be collapsed into one field.

This means the only supported write model is the current local-async shape, tightened until it matches the old protocol expectations.

### auth

The rewrite must support verified JWT auth.

- Bearer tokens must be signature-verified.
- Signing keys must come from JWKS.
- Issuer and audience must be enforced.
- Expiry and other required time-based claims must be enforced.
- HTTP and WebSocket auth must share the same verification and claim-shaping logic.
- No production auth path may rely on unverified claims.

### cursor and resume contract

The rewrite must restore epoch-aware cursors.

- Session event cursors and committed cursors must carry `{epoch, seq}`.
- Resume validation must distinguish at least:
  - `cursor_expired`
  - `epoch_stale`
  - `rollback`
- Public wire payloads may collapse reasons where the existing contract does, but internal logic must preserve the richer signal.
- Tail and lifecycle replay must use the same epoch-aware cursor model.

### readiness

Readiness must represent actual ability to serve the protocol.

- Draining nodes are not ready.
- Nodes without a valid control-plane heartbeat state are not ready.
- Nodes whose lease/control-plane state is stale are not ready.
- Readiness must reflect the Postgres-backed control plane, not just `SELECT 1`.

## architecture boundaries

### edge

- HTTP handlers
- raw WebSocket handlers
- Phoenix-compatible socket framing
- auth extraction and request validation

### runtime

- session workers
- lifecycle emission
- fanout
- hot session and hot event caches

### cluster

- Postgres-backed ownership
- standby assignment
- standby replication
- owner forwarding and wrong-node responses
- readiness inputs derived from control-plane state

### data plane

- archive flush queue
- archive worker
- read-path fallback into Postgres
- repository persistence and replay queries

## immediate implementation sequence

1. Remove `SyncPostgres` and all commit-mode branching. Keep one write model only.
2. Replace unsafe JWT parsing with verified JWT plus JWKS support.
3. Introduce epoch-aware cursor types and restore richer gap semantics.
4. Tighten readiness to reflect drain state plus control-plane validity.

## non-goals for this phase

- Reintroducing Raft
- Reintroducing S3
- Adding batch append
- Reworking the public API surface beyond parity fixes
