# Benchmarks

Starcite ships with a reproducible benchmark harness so you can validate performance on your own hardware.

## What's included

- `bench/k6/1-hot-path-throughput.js`: high-throughput append latency and failure-rate guardrails
- `bench/k6/2-rest-read-write-mix.js`: append mix with `expected_seq` guard behavior
- `bench/k6/3-cold-start-replay.js`: idempotency retry/dedupe behavior
- `bench/k6/4-durability-cadence.js`: sustained ordered append workload with ordering checks
- `bench/k6/5-backpressure-capacity.js`: stepped-QPS load to measure `qps_until_backpressure` plus lag/memory signals
- `bench/elixir/1-hot-path-throughput.exs`: closed-loop internal hot-path benchmark via `Benchee` (no HTTP/k6)
- `bench/elixir/2-routing-attribution.exs`: internal attribution benchmark comparing `append_event`, `append_event_local`, RPC self-hop, and direct `:ra.process_command`
- `bench/elixir/3-internal-attribution.exs`: internal attribution benchmark splitting `Session`, `EventStore`, `RaftFSM.apply`, and full `Runtime.append_event`
- `bench/aws/*`: Terraform + scripts to run k6 in the same VPC as the cluster

## Local quick run

Use the manual Compose workflow from `docs/local-testing.md`.

1. Start a local integration cluster:
   ```bash
   PROJECT_NAME=starcite-it-a
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
   ```
2. Run scenarios sequentially from the Compose `k6` service:
   ```bash
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm k6 run /bench/1-hot-path-throughput.js
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm k6 run /bench/2-rest-read-write-mix.js
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm k6 run /bench/3-cold-start-replay.js
   docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm k6 run /bench/4-durability-cadence.js
   ```
3. Tear down:
   ```bash
   ./scripts/test-cutover.sh
   ```
4. Run scenarios sequentially:
   ```bash
   k6 run bench/k6/1-hot-path-throughput.js
   k6 run bench/k6/2-rest-read-write-mix.js
   k6 run bench/k6/3-cold-start-replay.js
   k6 run bench/k6/4-durability-cadence.js
   k6 run bench/k6/5-backpressure-capacity.js
   ```
5. Stop the cluster:
   ```bash
   ./scripts/stop-cluster.sh
   ```

Run k6 scenarios sequentially, not in parallel. Each scenario includes threshold gates and may abort on failure.
For manual failover drills (`kill`, `pause`, `restart`), see `docs/local-testing.md`.

### Internal Elixir hot-path benchmark (Benchee)

This benchmark exercises `Runtime.append_event/3` directly in-process (closed loop), so it isolates runtime/raft/event-store overhead without HTTP client/server effects.

```bash
mix run --no-start bench/elixir/1-hot-path-throughput.exs
```

Useful knobs:

```bash
BENCH_SESSION_COUNT=256 \
BENCH_PARALLEL=16 \
BENCH_PAYLOAD_BYTES=256 \
BENCH_WARMUP_SECONDS=5 \
BENCH_TIME_SECONDS=30 \
BENCH_ARCHIVE_FLUSH_INTERVAL_MS=5000 \
BENCH_LOG_LEVEL=error \
BENCH_RAFT_DATA_DIR=tmp/bench_raft \
BENCH_CLEAN_RAFT_DATA_DIR=true \
mix run --no-start bench/elixir/1-hot-path-throughput.exs
```

Optional:
- `BENCH_EVENT_STORE_MAX_SIZE` (for example `2GB`, `512MB`)

### Internal routing attribution benchmark (Benchee)

This benchmark attributes append-path overhead by comparing:
- `append_public` (`Runtime.append_event/3`)
- `append_local` (`Runtime.append_event_local/3`)
- `append_rpc_self_local` (`:rpc.call(node, Runtime.append_event_local/3)`)
- `append_ra_direct` (`:ra.process_command/3` directly)

```bash
BENCH_SESSION_COUNT=64 \
BENCH_PARALLEL=4 \
BENCH_PAYLOAD_BYTES=256 \
BENCH_WARMUP_SECONDS=1 \
BENCH_TIME_SECONDS=5 \
BENCH_ARCHIVE_FLUSH_INTERVAL_MS=60000 \
BENCH_LOG_LEVEL=error \
mix run --no-start bench/elixir/2-routing-attribution.exs
```

Default settings in this script are intentionally conservative for local stability.
For stress runs, increase `BENCH_SESSION_COUNT` and `BENCH_PARALLEL` explicitly.

### Internal FSM/ETS attribution benchmark (Benchee)

This benchmark isolates internal append components:
- `session.append_event`
- `event_store.put_event`
- `event_store.raw_ets_insert`
- `raft_fsm.apply_append`
- `runtime.append_event`

```bash
BENCH_SESSION_COUNT=256 \
BENCH_PARALLEL=8 \
BENCH_PAYLOAD_BYTES=256 \
BENCH_WARMUP_SECONDS=3 \
BENCH_TIME_SECONDS=10 \
BENCH_ARCHIVE_FLUSH_INTERVAL_MS=60000 \
BENCH_EVENT_STORE_MAX_SIZE=64GB \
BENCH_LOG_LEVEL=error \
mix run --no-start bench/elixir/3-internal-attribution.exs
```

### Backpressure capacity run (example: 2GB event store)

Start cluster with the same runtime behavior you use in prod. For this run, only pin the event-store cap:

```bash
STARCITE_EVENT_STORE_MAX_SIZE=2GB ./scripts/start-cluster.sh
```

Then run:

```bash
EVENT_STORE_MAX_SIZE_LABEL=2GB \
START_QPS=200 STEP_QPS=200 MAX_QPS=4000 STAGE_DURATION=20s \
k6 run bench/k6/5-backpressure-capacity.js --summary-export logs/k6-backpressure-capacity-raw.json
```

The script also writes `backpressure-capacity-summary.json` with:
- `qps_until_backpressure`
- `archive_lag_max`
- `archive_pending_rows_max`
- `event_store_memory_bytes_max`

Keep archive flush cadence at its normal setting for this test; slowing it artificially turns the result into a pure fill-rate test.

To run an open-ended search (no fixed QPS ceiling), use:

```bash
STARCITE_EVENT_STORE_MAX_SIZE=2GB ./scripts/start-cluster.sh

EVENT_STORE_MAX_SIZE_LABEL=2GB \
START_QPS=500 STEP_QPS=500 STAGE_DURATION=10s \
./scripts/find-backpressure-qps.sh
```

The search stops when either:
- backpressure is observed, or
- the load generator can no longer sustain the requested QPS (`TARGET_RATIO_MIN`).

## AWS reproducible setup

```bash
cd bench/aws/terraform
terraform init
terraform apply -var="ssh_public_key=$(cat ~/.ssh/id_rsa.pub)"

cd ../scripts
./deploy.sh
./run-k6-remote.sh 1-hot-path-throughput
```

Tear down:

```bash
cd bench/aws/terraform && terraform destroy
```

## Interpreting results

- Append latency depends on network RTT between replicas. Keep cluster nodes close.
- Throughput scales with independent sessions and cluster size.
- `503` responses mean quorum is not formed. Verify health and retry with backoff.

If publishing numbers, capture: instance types, region/AZ layout, cluster size, k6 scenario config, and commit SHA.
