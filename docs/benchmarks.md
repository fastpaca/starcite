# Benchmarks

Use end-to-end k6 first, then mix benchmarks for component-level signals.

## End-to-end

```bash
PROJECT_NAME=starcite-it-a
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" --profile tools run --rm \
  k6 run /bench/k6-hot-path-throughput.js
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" down -v --remove-orphans
```

## Internal

```bash
mix bench            # default
mix bench routing
mix bench internal
```

Keep a short note with commit SHA and the command/env used.
