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

## Avoid Defensive Overcoding

- Do not add wrapper helpers that only re-check types/keys already guaranteed by caller context and callee guards.
- Do not add `normalize_*` helpers unless they perform a real representation change. Renaming, pass-through, or forcing defaults is not a valid normalization.
- Do not add silent fallback defaults (`:internal`, `:unknown`, `%{}`, `[]`, `nil`) on internal paths unless product behavior explicitly requires that default.
- In trusted internal flows, pattern match directly in function heads or `with` clauses and let mismatches fail loudly.
- When adding telemetry labels, avoid broad catch-all coercion. Preserve domain-level reason atoms from the source error unless an explicit mapping is required by a metric contract.
- Prefer deleting defensive branches over keeping "just in case" logic that hides impossible states.

### Pre-Handoff Self-Check (Required)

- Did I introduce any new `maybe_*`, `normalize_*`, or pass-through helper that only forwards to another function?
- Did I duplicate guards/validation that already exist at boundaries or in the called function head?
- Did I add any default/fallback value on an internal path that could mask a real bug?
- Can this be simplified by inlining logic into the existing `with`/pattern match flow?
- If a fallback remains, is it explicitly required by product semantics and documented in code/comments?

## Domain Assumptions

- Messages are append-only and replayable with deterministic sequence numbers.
- Sessions are stored durably in 256 Raft groups (3 replicas each, quorum writes).
- No compaction, token budgets, or prompt-window logic—clients own that responsibility.
- Background flusher streams Raft state to Postgres (non-blocking, idempotent).

## Performance Learnings

### Generic Hot-Path Playbook (Agent Reusable)

- Step 1: define the target before edits (`qps`, `p95`, `p99`, timeout/error rate, CPU, allocations).
- Step 2: run a baseline and record both offered load and effective throughput.
- Step 3: optimize one bottleneck class at a time, then rerun the same workload.
- Step 4: keep changes only when they improve the declared target; revert complexity otherwise.

- Signal: high allocation rate on a dominant single-item path.
- Action: add a dedicated single-item code path that bypasses batch list building, reverse, and map churn.
- Guardrail: preserve ordering and idempotency semantics exactly.
- Exit criterion: lower alloc pressure and higher sustained throughput at equal or better latency.

- Signal: endpoint/controller CPU dominates even before consensus/storage.
- Action: parse and validate once at the boundary using strict fast clauses; keep a separate slow path for optional inputs.
- Guardrail: invalid input still fails loudly with explicit errors.
- Exit criterion: reduced boundary CPU without shape/validation regressions.

- Signal: hot path performs non-essential synchronous reads.
- Action: skip read-path work when authorization or flow type makes the read unnecessary.
- Guardrail: keep strict checks for the privileged/principal path.
- Exit criterion: fewer sync operations in traces and improved throughput.

- Signal: per-call overhead from generic option containers (`Keyword`, map defaults) for scalar fields.
- Action: move frequently accessed fields into direct scalars in command payloads and function heads.
- Guardrail: keep compatibility adapters only at boundaries, not inside hot loops.
- Exit criterion: lower reductions/instruction count and allocation per request.

- Signal: global contention primitives appear in every request path.
- Action: replace global counters/IDs with process-local counters or amortized periodic checks.
- Guardrail: ensure no safety regressions in capacity and back-pressure behavior.
- Exit criterion: contention disappears from profiles and tail latency improves under load.

- Signal: wrapper helpers add extra ETS/list work in critical queues.
- Action: use direct operations on named tables/structures and add singleton fast paths where traffic is mostly single events.
- Guardrail: keep queue semantics and visibility unchanged.
- Exit criterion: reduced CPU + allocs in queue hot functions.

- Signal: hashing/fingerprinting is a top CPU consumer.
- Action: use cheaper fingerprints when cryptographic guarantees are unnecessary.
- Guardrail: document collision risk and restrict weaker hash use to safe domains.
- Exit criterion: measurable CPU reduction with acceptable collision profile.

- Signal: application hot path is optimized but throughput ceiling remains.
- Action: validate ingress/routing topology as a first-order bottleneck before deeper consensus changes.
- Guardrail: do not conflate control-plane churn with data-plane saturation.
- Exit criterion: topology experiments explain the ceiling difference clearly.

- Rule: treat every optimization as provisional until measured.
- Rule: prefer deletion over abstraction when an optimization does not move the ceiling.

### Starcite Instantiation (Current Cycle)

- Single append no longer pays batch costs in Raft FSM (`append_one_to_session`, `put_appended_event`, `build_effect_for_event`).
- Append validation has a strict fast clause and a separate fallback clause.
- Non-principal append auth skips `ReadPath.get_session/1`; principal flow keeps session checks.
- Raft command payload uses scalar `expected_seq`; hot append path avoids option-list lookups.
- Capacity polling uses process-local counters instead of per-write global `:erlang.unique_integer`.
- Event queue hot methods use direct named ETS operations and singleton `put_events` fast path.
- Experimental dedupe path used cheaper composite `phash2` fingerprint with explicit collision tradeoff.
- After CPU/alloc fixes, ingress shape dominated measured ceilings (single-ingress outperformed round-robin).
- Preferred-write-node routing experiment was rolled back when complexity increased without ceiling gains.

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
- For k6 throughput tests, distinguish offered rate from effective throughput (`events_sent`/`http_reqs`) and always inspect `dropped_iterations` and failure rate before concluding the service ceiling.
- For append benchmarks, avoid artificial contention by using enough sessions and stable producer identity/sequence generation; otherwise results overstate contention bottlenecks.
- Ensure cluster readiness before high-load tests; if skipping readiness checks for warm reruns, verify node/bootstrap health first.

## Delivery Checklist

- Pattern-match inputs, return errors for invalid shapes.
- Update/invalidate caches when side effects change underlying data.
- Document new runtime behaviours in `docs/` if you alter process lifecycles or message flow.
