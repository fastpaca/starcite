# Local Testing

This is the manual local cluster workflow for Starcite.

- No wrapper scripts.
- One Compose file (`docker-compose.integration.yml`).
- Multiple concurrent local clusters via unique Compose project names.
- Cluster runs are optional for local feedback loops; use this when you specifically want a cluster vibe check.

## Prerequisites

- Docker Engine + Docker Compose v2
- Optional: host `k6` install (not required if you use the Compose `k6` service)

## Start a cluster

Pick a project name and bring up the stack:

```bash
PROJECT_NAME=starcite-it-a
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
```

This starts:

- `node1`, `node2`, `node3` (3-node cluster)
- `db` (Postgres)

`node1` publishes a random host port by default (`NODE1_PORT=0`). To inspect it:

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" port node1 4000
```

## Run benchmarks

Run the k6 hot-path benchmark in the same Compose network (service-name DNS, no host-port wiring needed):

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm \
  k6 run /bench/k6-hot-path-throughput.js
```

Tune scenario params with `-e` flags:

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm \
  k6 run -e MAX_VUS=10 -e STEADY_DURATION=30s /bench/k6-hot-path-throughput.js
```

Run Elixir attribution benchmarks from the host:

```bash
mix bench
mix bench routing
mix bench internal
```

## Manual failover drills

Run a workload in one terminal, then inject faults from another.

Kill and restart a node:

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" kill -s SIGKILL node2
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d node2
```

Pause/unpause a node process:

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" pause node3
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" unpause node3
```

Follow logs:

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" logs -f node1 node2 node3
```

## Run multiple clusters concurrently

Use different project names:

```bash
docker compose -f docker-compose.integration.yml -p starcite-it-a up -d --build
docker compose -f docker-compose.integration.yml -p starcite-it-b up -d --build
```

Benchmark each cluster independently by targeting the matching `-p` value.

## Tear down

```bash
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" down -v --remove-orphans
```
