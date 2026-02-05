---
title: Benchmarks
sidebar_position: 90
---

# Benchmarks

FleetLM ships with a reproducible benchmark harness so you can validate performance and failure modes on your own hardware/network.

The benchmark suite focuses on the *conversation layer* primitives: append latency, tail/replay reads, and durability under load. Your end-to-end latency will still be dominated by the model provider.

## What’s included

- `bench/k6/*`: k6 scenarios for REST write/read mixes, throughput, and durability cadence
- `bench/aws/*`: Terraform + scripts to provision an AWS environment and run the k6 suite close to the cluster (removes WAN latency from the measurement)

## Local quick run

1. Start FleetLM locally (single node).
2. Install k6.
3. Run a scenario:

```bash
k6 run bench/k6/1-hot-path-throughput.js
```

Each scenario defines **thresholds** as guardrails (for example, `append_latency p99 < 120ms` in the hot-path test). Treat these as pass/fail signals for the environment you’re running in.

## AWS reproducible setup

The AWS harness provisions a cluster and a benchmark client in the same VPC so you can measure FleetLM without public-internet jitter.

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
- Throughput scales with the number of independent conversations and the cluster size.
- If you see `503` responses, you likely don’t have a Raft quorum (or the cluster isn’t formed yet); retry with backoff.

If you run these benchmarks in a representative environment and want to publish numbers, capture:
- instance types + region/AZ layout
- cluster size
- k6 scenario config (`MAX_VUS`, `PIPELINE_DEPTH`, durations)
- commit SHA / version
