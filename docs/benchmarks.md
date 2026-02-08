# Benchmarks

Starcite ships with a reproducible benchmark harness so you can validate performance on your own hardware.

## What's included

- `bench/k6/1-hot-path-throughput.js`: high-throughput append latency and failure-rate guardrails
- `bench/k6/2-rest-read-write-mix.js`: append mix with `expected_seq` guard behavior
- `bench/k6/3-cold-start-replay.js`: idempotency retry/dedupe behavior
- `bench/k6/4-durability-cadence.js`: sustained ordered append workload with ordering checks
- `bench/aws/*`: Terraform + scripts to run k6 in the same VPC as the cluster

## Local quick run

1. Start the local cluster:
   ```bash
   ./scripts/start-cluster.sh
   ```
2. Verify primitive health:
   ```bash
   ./scripts/test-cluster.sh
   ```
3. Run scenarios sequentially:
   ```bash
   k6 run bench/k6/1-hot-path-throughput.js
   k6 run bench/k6/2-rest-read-write-mix.js
   k6 run bench/k6/3-cold-start-replay.js
   k6 run bench/k6/4-durability-cadence.js
   ```
4. Stop the cluster:
   ```bash
   ./scripts/stop-cluster.sh
   ```

Run k6 scenarios sequentially, not in parallel. Each scenario includes threshold gates and may abort on failure.

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
