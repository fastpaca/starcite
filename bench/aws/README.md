# Fastpaca AWS Benchmark Infrastructure

Reproducible Terraform configuration for benchmarking Fastpaca on AWS.

## ⚠️ WARNING

This will deploy real AWS infrastructure and **incur costs**. All resources are **intentionally insecure** (no authentication, wide-open security groups). Use only for ephemeral benchmarks and destroy immediately after.

## Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform >= 1.0
- `jq` installed
- SSH key pair

## Running Benchmarks

**1. Provision infrastructure:**

```bash
cd bench/aws/terraform
terraform init
terraform apply -var="ssh_public_key=$(cat ~/.ssh/id_rsa.pub)"
```

**2. Deploy Fastpaca to the nodes:**

```bash
cd ../scripts
./deploy.sh
./smoke-test.sh   # Optional sanity check (creates a conversation, appends, reads)
```

**3. Run a benchmark (locally or from the AWS client):**

```bash
# From your workstation
./run-k6-benchmark.sh 1-hot-path-throughput

# From the benchmark client in AWS (eliminates WAN latency)
./run-k6-remote.sh 1-hot-path-throughput
```

**4. Teardown (important - stops billing):**

```bash
cd ../terraform
terraform destroy
```

## Configuration

Scale to 3 nodes:
```bash
terraform apply -var="instance_count=3"
```

Change instance type:
```bash
terraform apply -var="instance_type=c5d.xlarge"
```

Fastpaca runs single-container nodes with durable on-disk storage and no external database. Each instance comes up ready to serve requests as soon as Docker starts the release.

## Results & Methodology

See [docs/benchmarks.md](../../docs/benchmarks.md) for published results and methodology.
