# Benchmarks

Starcite ships with a reproducible benchmark harness so you can validate performance on your own hardware.

## What's included

- `bench/k6/1-hot-path-throughput.js`: high-throughput append latency and failure-rate guardrails
- `bench/k6/2-rest-read-write-mix.js`: append mix with `expected_seq` guard behavior
- `bench/k6/3-cold-start-replay.js`: idempotency retry/dedupe behavior
- `bench/k6/4-durability-cadence.js`: sustained ordered append workload with ordering checks
- `bench/k6/5-backpressure-capacity.js`: stepped-QPS load to measure `qps_until_backpressure` plus lag/memory signals
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
