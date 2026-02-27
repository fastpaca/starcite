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

`mix bench` (hot-path) runs a single `mpmc_mixed` Benchee scenario where
`write`, `read`, and `rtt` operations execute concurrently under the same load
window.

Default profile is now `production_like`:

- write-heavy operation mix (`write/read/rtt = 96:2:2`)
- point reads for `read`/`rtt` operation latency (`read_mode=point`)
- defaults sized for contention (`parallel=64`, `sessions=2048`, `producer_pool_size=128`)
- larger default payload (`payload_bytes=1024`)
- background tail consumers (`tail_consumer_count=64`, `tail_frame_batch_size=128`)
  connect to `GET /v1/sessions/:id/tail` so catch-up lag uses the external API path

Use `BENCH_WORKLOAD_PROFILE=balanced` to return to the old `1:1:1` mix with
point reads.

For each operation, the benchmark prints:

- `ips`
- latency stats in `ms`: `avg`, `min`, `p50`, `p95`, `p99`, `p999`, `max`

Hot-path also prints read catch-up lag stats (`write-ack -> observed`) in `ms`
to make benchmark changes correlate better with production catch-up behavior.

Hot-path SLO checks run after Benchee completes:

- `read` `p99` and `p999`
- `write` `p99` and `p999`

Default SLO targets:

- `read p99 < 50ms`
- `write p99 < 150ms`
- `read p999 < 100ms`
- `write p999 < 500ms`

The benchmark raises on SLO failure so regressions are explicit.

To exercise the external tail catch-up path locally, run with a reachable archive
backend (for example MinIO on `127.0.0.1:9000`):

```bash
STARCITE_AUTH_MODE=none \
STARCITE_ARCHIVE_ADAPTER=s3 \
STARCITE_S3_ENDPOINT=http://127.0.0.1:9000 \
STARCITE_S3_ACCESS_KEY_ID=minioadmin \
STARCITE_S3_SECRET_ACCESS_KEY=minioadmin \
STARCITE_S3_BUCKET=starcite-archive \
STARCITE_S3_REGION=us-east-1 \
STARCITE_S3_PATH_STYLE=true \
mix bench hot-path
```

Keep a short note with commit SHA and the command/env used.
