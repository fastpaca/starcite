# Deployment

Starcite needs durable Raft state plus a stable Postgres target to keep ordering guarantees.

## Runtime patterns

Single node:

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/starcite:/data \
  -e DATABASE_URL=postgres://user:password@host/db \
  -e STARCITE_EVENT_STORE_MAX_SIZE=2GB \
  ghcr.io/fastpaca/starcite:latest
```

Clustered nodes:

```bash
NODE_NAME=starcite-1
CLUSTER_NODES=starcite-1@starcite-1.local,starcite-2@starcite-2.local,starcite-3@starcite-3.local
docker run -d \
  -p 4000:4000 \
  -e DATABASE_URL=postgres://user:password@host/db \
  -e NODE_NAME=$NODE_NAME \
  -e CLUSTER_NODES=$CLUSTER_NODES \
  ghcr.io/fastpaca/starcite:latest
```

Use the same pattern for `starcite-2` and `starcite-3`, and place a load balancer in front of them.

For rolling updates, prefer infrastructure draining and have clients resume tails with their last committed `seq`.

## Integration testing stack

```bash
PROJECT_NAME=starcite-it-a
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" down -v --remove-orphans
```

## Authentication behavior

Starcite can run with no API auth or with JWT validation at the boundary:

- `STARCITE_AUTH_MODE=none` (default): API boundary is unauthenticated.
- `STARCITE_AUTH_MODE=jwt`: `Authorization: Bearer <jwt>` required on `/v1/*` HTTP and WebSocket upgrades.
- `Authorization` is optional for `/health/live` and `/health/ready`.

When JWT mode is enabled, Starcite validates:

- signature (JWKS from `STARCITE_AUTH_JWKS_URL`)
- `iss` (`STARCITE_AUTH_JWT_ISSUER`)
- `aud` (`STARCITE_AUTH_JWT_AUDIENCE`)
- `exp` with optional leeway (`STARCITE_AUTH_JWT_LEEWAY_SECONDS`)

Missing or invalid token -> `401` (HTTP) or WebSocket close.

## Key config

| Variable | Default | Purpose |
| --- | --- | --- |
| `NODE_NAME` | random | Node identifier |
| `CLUSTER_NODES` | none | Comma-separated cluster peers |
| `DNS_CLUSTER_QUERY` | none | DNS discovery target for cluster |
| `DNS_CLUSTER_NODE_BASENAME` | `starcite` | DNS node basename |
| `DNS_POLL_INTERVAL_MS` | `5000` | DNS cluster poll interval |
| `STARCITE_RAFT_DATA_DIR` | `priv/raft` | Raft state directory |
| `STARCITE_EVENT_STORE_MAX_SIZE` | `2GB` | Hot memory cap for queued session events |
| `DATABASE_URL` | none | Postgres URL |
| `STARCITE_POSTGRES_URL` | none | Alternate Postgres URL |
| `STARCITE_ARCHIVE_FLUSH_INTERVAL_MS` | `5000` | Archive flush interval |
| `STARCITE_ARCHIVE_READ_CACHE_TTL_MS` | `600000` | Archive read cache TTL |
| `STARCITE_ARCHIVE_READ_CACHE_CLEANUP_INTERVAL_MS` | `60000` | Cache cleanup interval |
| `STARCITE_ARCHIVE_READ_CACHE_COMPRESSED` | `true` | Compress archive read cache values |
| `STARCITE_ARCHIVE_READ_CACHE_MAX_SIZE` | `512MB` | Archive read cache budget |
| `STARCITE_ARCHIVE_READ_CACHE_RECLAIM_FRACTION` | `0.25` | Cache reclaim fraction |
| `STARCITE_AUTH_MODE` | `none` | API auth mode (`none` or `jwt`) |
| `STARCITE_AUTH_JWKS_URL` | none | JWKS endpoint (`jwt` mode) |
| `STARCITE_AUTH_JWT_ISSUER` | none | Required JWT issuer |
| `STARCITE_AUTH_JWT_AUDIENCE` | none | Required JWT audience |
| `STARCITE_AUTH_JWT_LEEWAY_SECONDS` | `30` | Token clock-skew tolerance |
| `STARCITE_AUTH_JWKS_REFRESH_MS` | `60000` | JWKS cache refresh interval |
| `DB_POOL_SIZE` | `10` | Postgres pool size |
| `PORT` | `4000` | HTTP port |
| `PHX_SERVER` | unset | Enable Phoenix endpoint in releases |
| `SECRET_KEY_BASE` | none | Phoenix secret for production |
| `PHX_HOST` | `example.com` | Endpoint host |

## Metrics

Starcite exposes Prometheus metrics on `/metrics`.

| Metric | Why it matters |
| --- | --- |
| `starcite_events_append_total` | Append throughput |
| `starcite_archive_pending_rows` | Backlog waiting for archival |
| `starcite_archive_lag` | Archive delay |
| `starcite_event_store_backpressure_total` | Append pressure at hot store |
