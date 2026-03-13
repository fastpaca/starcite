# Session Contract

Starcite provides durable sessions for AI agents and apps.

A Starcite session is a managed durable communication context built on an
ordered, append-only event stream with cursor-based resume. It gives clients a
stable session identity, durable writes before ack, ordered replay, and live
continuation on the same stream.

Starcite is authoritative for the session stream and its minimal envelope
metadata. It is not intended to replace the customer's broader application
database.

## Core Model

A Starcite session includes:

- a stable `session_id`
- tenant scoping and auth boundaries
- minimal session envelope metadata such as title and small metadata fields
- an ordered, append-only event log
- per-session sequencing, deduplication, and cursor-based replay semantics

This is the product primitive. The stream is the storage primitive underneath
it.

## What Starcite Owns

Starcite is the system of record for:

- session identity and tenant fencing
- ordered session events and their monotonic sequence numbers
- durable append semantics and deduplication
- replay and live tail semantics
- minimal session envelope data required to create, authorize, route, and resume
- basic session catalog operations needed by the public API

That basic catalog surface includes creating a session, reading it by ID, and a
small tenant-scoped list view suitable for recent-session UX and operational
inspection.

## What Customers Own

Customers remain authoritative for their broader business objects and query
workloads.

That usually includes:

- conversations, threads, runs, workflows, tickets, documents, or other app-level objects
- user/workspace/project relationships
- rich relational metadata and ownership models
- search, analytics, dashboards, inboxes, and arbitrary filtering/sorting
- any browse/query pattern that depends on the customer's product semantics

A customer application can map one of its own objects to a Starcite `session_id`
and use Starcite as the durable communication layer for that object.

## API Scope

Starcite should optimize for the session operations that define the product:

- create a session
- append to a session
- tail a session from a cursor
- perform a basic tenant-scoped session list

`append` and `tail` are the center of gravity. They define the latency,
correctness, and durability requirements of the system.

`GET /v1/sessions` is a basic catalog endpoint, not a general-purpose query
engine. Exact-match metadata filters and cursor pagination are acceptable as
convenience features. Arbitrary query workloads are out of scope unless Starcite
grows an explicit catalog subsystem for them.

## Storage Contract

Storage backends must reflect the product boundary.

- Hot-path session state exists to serve append, replay, and resume semantics.
- Archive backends exist to durably store session/event data and support replay,
  recovery, and retention.
- Archive storage is not the source of truth for arbitrary query semantics.

In particular, object storage backends such as S3 should be treated as archive
storage for immutable or append-oriented blobs, snapshots, and recovery
artifacts. They should not become an implicit query engine through ad hoc key
layouts and derived index objects for every new browse pattern.

If Starcite needs richer session catalog behavior, that catalog must be designed
explicitly as a first-class subsystem with a clear schema and consistency model.

## Design Rules

- Keep the session envelope minimal and operational.
- Do not let archive layout define the public query model by accident.
- Treat every new session query shape as a product decision, not an adapter trick.
- New catalog semantics require an explicit schema and ownership model.
- Prefer customer-owned application catalogs for customer-specific browse and search experiences.

## Practical Guidance

If you are building on Starcite, think of it like this:

- Starcite owns the durable session.
- Your app owns the business object around that session.

Examples:

- a chat thread may reference a Starcite session
- an agent run may reference a Starcite session
- a workflow execution may reference a Starcite session

In each case, Starcite stores the durable interaction history. The customer app
stores the richer object model and the broader query surface around it.
