# Deployment

Starcite needs durable Raft state plus a durable archive backend to keep ordering guarantees.

Default archive backend is `s3` (S3-compatible object storage). Postgres remains available via `STARCITE_ARCHIVE_ADAPTER=postgres`.

## Runtime patterns

Single node:

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/starcite:/data \
  -e STARCITE_ARCHIVE_ADAPTER=s3 \
  -e STARCITE_S3_BUCKET=starcite-archive \
  -e STARCITE_S3_REGION=auto \
  -e STARCITE_S3_ENDPOINT=https://fly.storage.tigris.dev \
  -e STARCITE_S3_ACCESS_KEY_ID=... \
  -e STARCITE_S3_SECRET_ACCESS_KEY=... \
  -e STARCITE_EVENT_STORE_MAX_SIZE=2GB \
  ghcr.io/fastpaca/starcite:latest
```

Clustered nodes:

```bash
NODE_NAME=starcite-1
CLUSTER_NODES=starcite-1@starcite-1.local,starcite-2@starcite-2.local,starcite-3@starcite-3.local
docker run -d \
  -p 4000:4000 \
  -e STARCITE_ARCHIVE_ADAPTER=s3 \
  -e STARCITE_S3_BUCKET=starcite-archive \
  -e STARCITE_S3_REGION=auto \
  -e STARCITE_S3_ENDPOINT=https://fly.storage.tigris.dev \
  -e STARCITE_S3_ACCESS_KEY_ID=... \
  -e STARCITE_S3_SECRET_ACCESS_KEY=... \
  -e NODE_NAME=$NODE_NAME \
  -e CLUSTER_NODES=$CLUSTER_NODES \
  ghcr.io/fastpaca/starcite:latest
```

Use the same pattern for `starcite-2` and `starcite-3`, and place a load balancer in front of them.

For rolling updates, prefer infrastructure draining and have clients resume tails with their last committed `seq`.

To run Postgres archive mode instead:

```bash
docker run -d \
  -p 4000:4000 \
  -e STARCITE_ARCHIVE_ADAPTER=postgres \
  -e DATABASE_URL=postgres://user:password@host/db \
  ghcr.io/fastpaca/starcite:latest
```

## Integration testing stack

```bash
PROJECT_NAME=starcite-it-a
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" up -d --build
docker compose -f docker-compose.integration.yml -p "$PROJECT_NAME" down -v --remove-orphans
```

## Authentication behavior

Starcite can run with no API auth or with JWT validation at the boundary:

- `STARCITE_AUTH_MODE=none` (default): API boundary is unauthenticated.
- `STARCITE_AUTH_MODE=jwt`: layered auth on `/v1/*` HTTP and WebSocket upgrades.
- `Authorization` is optional for `/health/live` and `/health/ready`.

When JWT mode is enabled:

- Service JWTs are validated using JWKS (`iss`/`aud`/`exp`).
- Service JWTs can call service-only endpoints (`/v1/auth/issue`, session create/list).
- `/v1/auth/issue` mints short-lived scoped principal tokens signed by Starcite.
- Principal tokens can access append/tail within scope/session/tenant limits.

Service JWT validation:

- signature (JWKS from `STARCITE_AUTH_JWKS_URL`)
- `iss` (`STARCITE_AUTH_JWT_ISSUER`)
- `aud` (`STARCITE_AUTH_JWT_AUDIENCE`)
- `exp` with optional leeway (`STARCITE_AUTH_JWT_LEEWAY_SECONDS`)

Missing/invalid token -> `401` (HTTP) or WebSocket close.
Policy denial (scope/session/tenant) -> `403` for HTTP/WebSocket upgrade.

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
| `STARCITE_ARCHIVE_ADAPTER` | `s3` | Archive backend (`s3` or `postgres`) |
| `STARCITE_S3_BUCKET` | none | S3 bucket name (required in `s3` mode) |
| `STARCITE_S3_REGION` | `AWS_REGION` or `us-east-1` | S3 region for signing |
| `STARCITE_S3_ENDPOINT` | none | Custom S3-compatible endpoint (for Tigris/MinIO/etc.) |
| `STARCITE_S3_ACCESS_KEY_ID` | none | S3 access key (falls back to `AWS_ACCESS_KEY_ID`) |
| `STARCITE_S3_SECRET_ACCESS_KEY` | none | S3 secret key (falls back to `AWS_SECRET_ACCESS_KEY`) |
| `STARCITE_S3_SESSION_TOKEN` | none | Optional S3 session token (falls back to `AWS_SESSION_TOKEN`) |
| `STARCITE_S3_PREFIX` | `starcite` | Object key prefix |
| `STARCITE_S3_PATH_STYLE` | `true` | Use path-style S3 URLs for compatibility |
| `STARCITE_S3_MAX_WRITE_RETRIES` | `4` | Retry budget for conditional-write conflicts |
| `DATABASE_URL` | none | Postgres URL (required in `postgres` mode) |
| `STARCITE_POSTGRES_URL` | none | Alternate Postgres URL (required in `postgres` mode if `DATABASE_URL` unset) |
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
| `STARCITE_AUTH_PRINCIPAL_TOKEN_SALT` | `principal-token-v1` | Phoenix.Token salt for principal token signing |
| `STARCITE_AUTH_PRINCIPAL_TOKEN_DEFAULT_TTL_SECONDS` | `600` | Default principal token TTL |
| `STARCITE_AUTH_PRINCIPAL_TOKEN_MAX_TTL_SECONDS` | `900` | Maximum principal token TTL |
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
