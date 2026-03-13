# Session Contract

Starcite provides durable sessions for AI agents and apps.

A Starcite session is a durable communication context with:

- a stable `session_id`
- tenant fencing and auth boundaries
- minimal session metadata
- an ordered, append-only event stream
- durable append, replay, and live resume semantics

Starcite is authoritative for that session stream and its minimal envelope. It
is not intended to replace your broader application database.

## What Starcite Owns

- session identity and tenant fencing
- ordered events and monotonic sequence numbers
- deduplication and cursor-based replay/tail semantics
- minimal session metadata needed to create, authorize, route, and resume
- a small session catalog exposed by the public API

## What Your App Owns

- business objects that reference a `session_id` such as threads, runs, or workflows
- richer relational metadata and ownership models
- search, analytics, dashboards, inboxes, and arbitrary query patterns

Think of it like this:

- Starcite owns the durable session.
- Your app owns the business object around that session.

## API Scope

The core product is:

- create a session
- append to a session
- tail from a cursor

`GET /v1/sessions` is a basic tenant-scoped catalog endpoint, not a
general-purpose query engine.

## Storage Implications

- hot-path state exists to serve append, replay, and resume
- archive backends exist for durability, recovery, and replay
- archive storage is not the source of truth for arbitrary query semantics

Object storage backends such as S3 should be treated as archive storage, not as
an implicit query layer built from ad hoc key layouts and derived indexes. If
Starcite needs richer catalog behavior, that catalog should be designed
explicitly as its own subsystem.
