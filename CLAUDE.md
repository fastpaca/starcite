# CLAUDE.md

Starcite: durable, low-latency session event storage for LLM applications.

## Commands

```bash
mix deps.get          # Install dependencies
mix compile           # Compile the application
mix phx.server        # Run dev server at localhost:4000
mix test              # Run tests (creates/migrates DB automatically)
mix test path/to/file # Run specific test file
mix precommit         # Gate before finishing: typecheck + dialyzer + format + test
```

## API Surface

Three endpoints under `/v1`:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v1/sessions` | POST | Create session |
| `/v1/sessions/:id/append` | POST | Append event |
| `/v1/sessions/:id/tail?cursor=N` | GET (WebSocket) | Replay from cursor, then stream live |

Required fields on append: `type`, `payload`, `actor`. Optional: `source`, `metadata`, `refs`, `idempotency_key`, `expected_seq`.

Session IDs: auto-generated as `ses_<base64>` or client-provided.

## Architecture

```
Client → Phoenix Gateway → Runtime → Raft Groups (256 × 3 replicas)
                                          ↓
                              Archiver → Postgres (cold storage)
```

**Key insight:** Session metadata and event logs live in Raft memory. No synchronous DB lookups on append—Postgres is write-behind only.

| Component | Module | Role |
|-----------|--------|------|
| Runtime | `Starcite.Runtime` | Routes commands to correct Raft group |
| Raft FSM | `Starcite.Runtime.RaftFSM` | State machine for sessions/events |
| Raft Manager | `Starcite.Runtime.RaftManager` | Group lifecycle, shard assignment |
| Event Store | `Starcite.Runtime.EventStore` | In-memory event cache (ETS) |
| Archiver | `Starcite.Archive` | Background flush to Postgres (5s interval) |
| Tail Socket | `StarciteWeb.TailSocket` | WebSocket handler for replay + live streaming |

**Storage tiers:**
- Hot (Raft): Session metadata + recent events
- Cold (Postgres): Full event history, keyed by `(session_id, seq)`

## Coding Standards

**Pattern matching:** Fail loudly on bad input. Use function-head guards or `with` pipelines. Only provide defaults when product intentionally supports omissions.

**Telemetry:** All events emit through `Starcite.Observability.Telemetry`. Add explicit clauses for new metadata—never swallow unexpected shapes into a fallback.

**Boundary normalization:** Destructure params once at the controller/socket boundary. Avoid `Map.get` chains deeper in.

**Contract semantics:** Preserve idempotency dedupe, expected-seq conflicts, and replay ordering.

## Key Files

| Task | Files |
|------|-------|
| API request handling | `lib/starcite_web/controllers/session_controller.ex` |
| WebSocket tail | `lib/starcite_web/tail_socket.ex` |
| Runtime logic | `lib/starcite/runtime.ex` |
| Raft state machine | `lib/starcite/runtime/raft_fsm.ex` |
| Background archival | `lib/starcite/archive.ex` |
| Telemetry events | `lib/starcite/observability/telemetry.ex` |
| Tests | `test/starcite/runtime_test.exs`, `test/starcite_web/` |

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` / `STARCITE_POSTGRES_URL` | Postgres connection |
| `STARCITE_RAFT_DATA_DIR` | Raft log/snapshot directory |
| `CLUSTER_NODES` | Comma-separated node list for local clusters |
| `DNS_CLUSTER_QUERY` | K8s headless service DNS for prod clustering |
| `SECRET_KEY_BASE` | Required in prod |
| `PHX_SERVER=true` | Enable HTTP server in releases |

## Local Cluster Testing

Use manual Compose lifecycle for integration tests:

```bash
PROJECT=starcite-it-a
docker compose -f docker-compose.integration.yml -p "$PROJECT" up -d --build
docker compose -f docker-compose.integration.yml -p "$PROJECT" \
  --profile tools run --rm k6 run /bench/1-hot-path-throughput.js
docker compose -f docker-compose.integration.yml -p "$PROJECT" down -v --remove-orphans
```

For failover drills: `docker compose ... kill`, `pause`, `unpause`, `up -d`.

Keep cluster runs optional during iteration—default to `mix test` unless testing cluster behavior specifically.

## Before Finishing

1. Run `mix precommit` and fix all warnings/failures
2. New modules follow pattern-matching, fail-loud style
3. Telemetry tags are explicit—no silent "unknown" defaults
4. Tests cover new code paths, especially API contracts and conflict handling
5. Document runtime behavior changes in `docs/` if significant
