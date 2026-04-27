# Native Stream Projection

This document describes the intended design for native stream projections as
first-class session state.

The goal is to keep Starcite's source of truth as the raw ordered event stream
while allowing external reducers to publish reduced native stream items into
the session itself. Once committed, those projection items replace their
covered seq range on normal reads and replay.

Starcite should own projection activation and replay semantics. It should not
execute reducer logic.

## Core Boundary

Starcite owns:

- raw session events
- raw archive storage and `archived_seq`
- committed projection state inside each session
- default composed read and replay behavior that substitutes projection items
  for covered raw seq ranges
- explicit raw-view reads and replay that ignore projection items

External projectors own:

- protocol parsing
- chunk reduction
- message semantics
- rebuild logic
- choosing which seq intervals are replaced by projection items

This means Starcite does not understand `openai.responses.v1`,
`ai_sdk.ui_message_stream.v1`, or any other reducer semantics. It only stores
opaque versioned items over seq intervals and commits them as session state.

## Session Contract

Projection is attachable to any session at any time.

Rules for v1:

- a session has zero or one projection layer
- there is no public projection name
- if a session has no projection items, both `composed` and `raw` views return
  raw events
- default view is `composed`
- callers may explicitly request `raw` view when they want every event
- once a projection item exists, `composed` reads and replay substitute it for
  its covered range while `raw` continues to expose the underlying raw events

This is intentionally opinionated for AI use cases. The public API should not
introduce projection families or named views in v1.

## Projection Item Model

A projection item is an immutable opaque replacement over a seq interval.

Example:

```json
{
  "item_id": "msg_123",
  "version": 3,
  "from_seq": 45,
  "to_seq": 241,
  "content_type": "application/json",
  "payload": {
    "id": "msg_123",
    "role": "assistant",
    "parts": [{ "type": "text", "text": "hello" }]
  }
}
```

This means seq `45..241` is served as one native stream item instead of the raw
events in that interval.

Projection items are not required to be linear or prefix-based. A session may
contain arbitrary non-overlapping projection intervals.

## Versions

Versions live on individual projection items, not on the whole session.

That is important because we do not want to rewrite an entire session-level
projection just to correct one reduced message.

Example:

- item `msg_123` version `1` covers seq `45..241`
- later the projector wants to fix only that message
- it writes item `msg_123` version `2`
- default reads and replay should now use version `2`
- every other item remains unchanged

So "latest version wins" means:

- for one `(session_id, item_id)`, Starcite serves the highest version
- older versions remain immutable history, but they are not used by default

## Session State Model

Each session can carry:

- raw events
- zero or more committed projection items
- version history per `item_id`

Projection visibility is part of the replicated session state, not an external
manifest or storage-side compare-and-swap boundary.

That means:

- projection writes are validated against the current committed session view
- latest-version selection is owned by the session
- default composed reads use the session's active latest projection items
- version-history reads return the versions committed for that `item_id`

## Validity Rules

For one session:

- items must be immutable
- `from_seq` and `to_seq` must be valid session seq bounds
- `from_seq <= to_seq`
- latest versions of active items used for substitution must not overlap
  ambiguously

For v1, the simplest rule is:

- collapse to the latest version of each `item_id`
- reject ambiguous overlaps across those latest-version items

If an external projector needs to change interval boundaries across multiple
items, it should write a coherent batch of new item versions so the latest-item
view remains non-overlapping.

## Read And Replay Semantics

Raw events remain the source of truth internally, but Starcite should support
two read and replay views:

### `composed` View

This is the default.

For a `composed` read or replay:

1. load projection items in the requested seq range
2. collapse them to the latest version of each `item_id`
3. sort them by `from_seq`
4. for covered intervals, return the projection items
5. for uncovered intervals, return the raw events
6. return one ordered composed stream

Example:

- raw events for `1..44`
- one projection item for `45..241`
- raw events for `242..300`

That means a fresh `composed` reader or replay sees one native stream, not a
separate projection API.

### `raw` View

This is the explicit opt-out.

For a `raw` read or replay:

1. ignore projection items entirely
2. return every raw event in seq order

This mode exists for projectors, debugging, audits, and any client that
needs the literal underlying event stream.

### Live Subscribers

V1 should stay simple:

- new `composed` reads and replay use projection items immediately after they
  are written
- existing live subscribers are not retroactively rewritten in place
- `raw` subscribers continue to see only raw events
- a reconnect or new `composed` replay sees the substituted stream

This avoids introducing range invalidation or replacement frames in the first
version.

## Cursor Semantics

Public cursors remain raw `seq` values.

For a projection item:

- its cursor position is `to_seq`
- resuming from a later cursor skips the raw events in its covered range as
  expected because the projection item already replaced them

This keeps Starcite's cursor contract stable.

## Write Semantics

The expected external flow is:

1. create a session or pick an existing one
2. external projector reads raw events from Starcite using seq cursors
3. projector reduces raw events into projection items
4. projector writes item versions to Starcite through the session write path
5. Starcite immediately uses those committed items on future reads and replay

Starcite should not run reducer logic as part of its archiver, but it should
treat projection state as part of the session aggregate.

## Rebuild Semantics

Rebuilds should be item-based.

Example:

1. the projector decides that one reduced message is wrong
2. it writes a higher version for that one `item_id`
3. future reads and replay prefer the latest version of that item
4. unaffected items do not change

If a projector wants to rebuild the whole session-level projection, it
can do so by writing newer versions for all relevant items over time. Starcite
should not require whole-stream rewrites for correctness.

## Why This Fits Starcite Better

This model preserves Starcite's core contract:

- ordered raw events are the source of truth
- archive semantics stay about raw events, while projection state remains part
  of the session itself
- Starcite does not gain hidden protocol semantics
- the public stream becomes more useful for AI clients without introducing
  named projection APIs

It also makes message reduction feel natural:

- a reduced message is just one projection item over `from_seq..to_seq`
- if no such item exists yet, raw events are returned
- if an item is corrected later, only that item's version changes

## Suggested API Direction

The API should stay opinionated and small.

Likely surfaces:

- write projection item version
- use the default session read and replay APIs for the `composed` stream
- allow `view=raw` on read and tail/replay APIs as an explicit opt-out

There should not be a public `/projection/:name` API in v1.

## Explicit Non-Goals

- per-session archiver workers
- protocol auto-detection
- built-in reducer execution inside Starcite
- whole-stream rewrites as the default update mechanism
- using Postgres as the projection item store

## Open Questions

- Whether v1 projection item writes should be single-item only or also support
  a batch write endpoint for coherent multi-item interval updates
- Whether projection item payloads should always be stored by reference or
  allow small inline payloads
- Whether Starcite should expose an internal-only raw read path distinct from
  the default composed read path
- Whether projection payloads should remain inline in session state for v1, or
  move to blob refs once large payloads become common
