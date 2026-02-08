---
title: Benchmarks
sidebar_position: 90
---

# Benchmarks

Starcite ships with a reproducible benchmark harness so you can validate performance and failure modes on your own hardware/network.

The benchmark suite focuses on the session primitives: append latency, optimistic concurrency behavior, idempotency behavior, and ordering durability under load.

## What’s included

- `bench/k6/1-hot-path-throughput.js`: high-throughput append latency and failure-rate guardrails
- `bench/k6/2-rest-read-write-mix.js`: append mix with `expected_seq` guard behavior
- `bench/k6/3-cold-start-replay.js`: idempotency retry/dedupe behavior
- `bench/k6/4-durability-cadence.js`: sustained ordered append workload with ordering checks
- `bench/aws/*`: Terraform + scripts to provision an AWS environment and run the k6 suite close to the cluster (removes WAN latency from the measurement)

## Local quick run

1. Start the local cluster:
   ```bash
   ./scripts/start-cluster.sh
   ```
2. Verify primitive health:
   ```bash
   ./scripts/test-cluster.sh
   ```
3. Run scenarios sequentially (one process at a time):
   ```bash
   k6 run bench/k6/1-hot-path-throughput.js
   k6 run bench/k6/2-rest-read-write-mix.js
   k6 run bench/k6/3-cold-start-replay.js
   k6 run bench/k6/4-durability-cadence.js
   ```
4. Stop the cluster when done:
   ```bash
   ./scripts/stop-cluster.sh
   ```

Notes:

- Run k6 scenarios sequentially, not in parallel.
- Each scenario includes threshold gates and may abort on failure.

## Single scenario quick run

1. Start Starcite.
2. Install k6.
3. Run a scenario:

```bash
k6 run bench/k6/1-hot-path-throughput.js
```

Each scenario defines **thresholds** as guardrails (for example, `append_latency p99 < 120ms` in the hot-path test). Treat these as pass/fail signals for the environment you’re running in.

## AWS reproducible setup

The AWS harness provisions a cluster and a benchmark client in the same VPC so you can measure Starcite without public-internet jitter.

```bash
cd bench/aws/terraform
terraform init
terraform apply -var="ssh_public_key=$(cat ~/.ssh/id_rsa.pub)"

cd ../scripts
./deploy.sh
./run-k6-remote.sh 1-hot-path-throughput
```

Tear down when you’re done:

```bash
cd bench/aws/terraform
terraform destroy
```

## Interpreting results

- Append latency is heavily influenced by network RTT between replicas; keep cluster nodes close for production.
- Throughput scales with the number of independent sessions and the cluster size.
- If you see `503` responses, your cluster may not be fully formed or you may have lost quorum; verify health and retry with backoff.

If you run these benchmarks in a representative environment and want to publish numbers, capture:
- instance types + region/AZ layout
- cluster size
- k6 scenario config (`VUS`, `SESSION_COUNT`, `PIPELINE_DEPTH`, durations)
- commit SHA / version
