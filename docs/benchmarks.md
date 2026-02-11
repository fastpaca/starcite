# Benchmarks

Starcite keeps benchmarks intentionally small and explicit:

- One end-to-end k6 benchmark for the API hot path.
- Three Elixir Benchee benchmarks for internal attribution.

## Benchmark Files

- `bench/k6-hot-path-throughput.js`: external API hot-path throughput/latency benchmark.
- `bench/k6-lib.js`: shared k6 helpers used by the hot-path benchmark.
- `bench/elixir-hot-path-throughput.exs`: closed-loop `Runtime.append_event/3` benchmark.
- `bench/elixir-routing-attribution.exs`: append-path attribution (`append_event`, local call, RPC self-hop, direct `:ra.process_command`).
- `bench/elixir-internal-attribution.exs`: internal attribution across `Session`, `EventStore`, `RaftFSM.apply`, and full runtime append.

## Local Quick Run (k6 Hot Path)

Use the manual Compose workflow from `docs/local-testing.md`.

1. Start cluster:
   ```bash
   PROJECT_NAME=starcite-it-a
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
   ```
2. Run k6 hot path:
   ```bash
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm \
     k6 run /bench/k6-hot-path-throughput.js
   ```
3. Tear down:
   ```bash
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" down -v --remove-orphans
   ```

## Elixir Benchmarks

Run from repo root:

```bash
mix run --no-start bench/elixir-hot-path-throughput.exs
mix run --no-start bench/elixir-routing-attribution.exs
mix run --no-start bench/elixir-internal-attribution.exs
```

Useful env knobs (all scripts):

- `BENCH_LOG_LEVEL` (default: `error`)
- `BENCH_RAFT_DATA_DIR`
- `BENCH_CLEAN_RAFT_DATA_DIR` (`true`/`false`)
- `BENCH_SESSION_COUNT`
- `BENCH_PAYLOAD_BYTES`
- `BENCH_PARALLEL`
- `BENCH_WARMUP_SECONDS`
- `BENCH_TIME_SECONDS`
- `BENCH_ARCHIVE_FLUSH_INTERVAL_MS`

Additional knobs:

- `bench/elixir-hot-path-throughput.exs`:
  - `BENCH_EVENT_STORE_MAX_SIZE`
  - `BENCH_EVENT_STORE_CAPACITY_CHECK`
  - `BENCH_APPEND_PUBSUB_EFFECTS`
  - `BENCH_APPEND_TELEMETRY`
- `bench/elixir-routing-attribution.exs`:
  - `BENCH_APPEND_PUBSUB_EFFECTS`
  - `BENCH_APPEND_TELEMETRY`
- `bench/elixir-internal-attribution.exs`:
  - `BENCH_EVENT_STORE_MAX_SIZE`
  - `BENCH_APPEND_PUBSUB_EFFECTS`
  - `BENCH_APPEND_TELEMETRY`

## Interpreting Results

- Use k6 results as end-to-end truth (network + HTTP + runtime path).
- Use Elixir benchmarks to attribute internal cost and compare implementation changes quickly.
- Publish benchmark numbers with commit SHA and benchmark command/env.
