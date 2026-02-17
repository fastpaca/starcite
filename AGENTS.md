# Starcite - Agent Guide

Starcite is a clustered Phoenix application that provides durable, low-latency session event storage for LLM applications. It maintains ordered session histories with sub-150ms p99 appends via Raft consensus, leaving prompt construction and token management to the client.

## Ground Rules

- Run `mix precommit` before you hand work back. It compiles with warnings-as-errors, formats, and runs tests.
- Use the built-in [`Req`](https://hexdocs.pm/req/Req.html) client for HTTP. Do not add `:httpoison`, `:tesla`, or `:httpc`.
- Never introduce new dependencies or services without explicit approval.
- Keep the default Tailwind v4 imports in `assets/css/app.css`; extend styling with Tailwind utility classes, not `@apply`.
- Do not add decorative separator/banner comments (for example `# -----`); keep comments meaningful and specific.

## Golden Patterns

- Fail loudly on bad input. Prefer function-head pattern matching or `with` pipelines that enforce required keys. Only provide defaults when the product intentionally supports omissions.
- Validate telemetry metadata in explicit clauses. Treat unexpected or missing labels as an error path so instrumentation stays trustworthy.
- Destructure known structs/maps and coerce once. Avoid chaining `Map.get/3` with defaults-trust shape where it’s guaranteed, and guard upstream.
- Honour immutability: produce new assigns/state rather than mutating in place, and prefer small pure helpers for transformations.
- Design for back-pressure. The runtime assumes at-least-once delivery and sequence numbers; handle drains and retries with clear status signaling.

## Simplify First

- Start with the simplest readable implementation that works, then add complexity only when a measured requirement demands it.
- Do not duplicate parsing/validation across layers. Parse and coerce at system boundaries (for example `runtime.exs`), then trust internal types.
- Prefer direct data flow over defensive transformation pipelines. Avoid re-shaping maps multiple times when one representation already fits.
- For storage formats, prefer off-the-shelf encoding/decoding (`Jason` for JSON/JSONL) over custom codecs unless there is a concrete performance or compatibility need.
- Keep adapters thin: isolate transport/client calls from layout/serialization concerns, but avoid fragmenting logic into many tiny helper modules without clear payoff.
- Remove generic “just in case” guards in internal paths. Crash loudly on impossible states rather than silently normalizing them.
- Minimize bespoke parsing (regexes, hand-rolled XML/date parsing, etc.) unless required by an unavoidable external protocol.
- Optimize for fewer lines and clearer control flow. Deleting code is preferred to adding abstraction when behavior stays correct.
- For prototype work, bias toward readability and explicitness over hardening.

## Domain Assumptions

- Messages are append-only and replayable with deterministic sequence numbers.
- Sessions are stored durably in 256 Raft groups (3 replicas each, quorum writes).
- No compaction, token budgets, or prompt-window logic—clients own that responsibility.
- Background flusher streams Raft state to Postgres (non-blocking, idempotent).

## Phoenix & LiveView Summary

- LiveView templates must start with `<Layouts.app flash={@flash} current_scope={@current_scope}>`.
- Use `<.form>` with `assigns.form = to_form(...)` and drive fields via `<.input field={@form[:field]}>`.
- Stick to `<.icon>` from `core_components` for hero icons; do not import other icon packs.
- Keep the UI polished: balanced spacing, subtle hover/transition states, and consistent typography.

## Frontend Notes

- Tailwind v4 import block must stay:
  ```
  @import "tailwindcss" source(none);
  @source "../css";
  @source "../js";
  @source "../../lib/starcite_web";
  ```
- No inline `<script>` tags. Extend behaviour via `assets/js/app.js` and Phoenix hooks with `phx-update="ignore"` when hooks own the DOM.
- Build micro-interactions via Tailwind utility classes and CSS transitions; avoid component libraries like daisyUI.

## Observability & Testing

- Emit telemetry via the centralized telemetry helper module; add new events there so tags stay normalised.
- When you touch collections rendered in LiveView, prefer streams (`stream/3`) and track counts/empty states separately.
- Use `mix test`, `mix test --failed`, or file-scoped runs to iterate quickly. End every work session with `mix precommit`.
- Local cluster testing/benchmarking uses manual Compose lifecycle:
  - `docker compose -f docker-compose.integration.yml -p <project> up -d --build`
  - `docker compose -f docker-compose.integration.yml -p <project> --profile tools run --rm k6 run /bench/k6-hot-path-throughput.js`
  - `docker compose -f docker-compose.integration.yml -p <project> down -v --remove-orphans`
- Avoid adding start/stop wrapper scripts for Docker Compose workflows; keep local failover drills explicit (`docker compose kill/pause/up`) and document them in `docs/local-testing.md`.
- Treat cluster runs as optional vibe checks for local iteration; default to faster `mix test` loops when cluster behavior is not under test.

## Delivery Checklist

- Pattern-match inputs, return errors for invalid shapes.
- Update/invalidate caches when side effects change underlying data.
- Document new runtime behaviours in `docs/` if you alter process lifecycles or message flow.
