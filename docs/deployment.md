---
title: Deployment
sidebar_position: 3
---

# Deployment

Starcite is designed for self-hosted clustered deployment.

## Hardware guidance

- CPU: 2+ vCPUs per node
- Memory: 4+ GB RAM per node
- Disk: fast SSD/NVMe for Raft logs
- Network: low RTT between cluster nodes

Set `STARCITE_RAFT_DATA_DIR` to persistent storage so Raft state survives restarts.

## Local development (single node)

```bash
docker run -d \
  -p 4000:4000 \
  -v starcite_data:/data \
ghcr.io/starcite-ai/starcite:latest
```

## Local cluster testing (five nodes)

Use the built-in scripts:

```bash
./scripts/start-cluster.sh
./scripts/test-cluster.sh
./scripts/stop-cluster.sh
```

## Three-node production shape

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/starcite:/data \
  -e CLUSTER_NODES=starcite-1@starcite.internal,starcite-2@starcite.internal,starcite-3@starcite.internal \
  -e NODE_NAME=starcite-1 \
  ghcr.io/starcite-ai/starcite:latest
```

Repeat with `NODE_NAME=starcite-2` and `NODE_NAME=starcite-3`.

Use a load balancer in front of nodes for API traffic.

## Archive (optional Postgres)

```bash
-e STARCITE_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host/db \
-e STARCITE_ARCHIVE_FLUSH_INTERVAL_MS=5000 \
-e DB_POOL_SIZE=10
```

## Runtime metrics

Prometheus metrics are exposed on `/metrics`.

Core series:

- `starcite_events_append_total`
- `starcite_events_payload_bytes`
- `starcite_archive_pending_rows`
- `starcite_archive_pending_sessions`
- `starcite_archive_flush_duration_ms`
- `starcite_archive_attempted_total`
- `starcite_archive_inserted_total`
- `starcite_archive_lag`
- `starcite_archive_tail_size`
- `starcite_archive_trimmed_total`

## Configuration summary

| Variable | Default | Description |
| --- | --- | --- |
| `NODE_NAME` | random | Node identifier used by clustering |
| `CLUSTER_NODES` | none | Comma-separated peer Erlang node names |
| `DNS_CLUSTER_QUERY` | none | DNS name for libcluster DNS poll strategy |
| `DNS_CLUSTER_NODE_BASENAME` | `starcite` | Base name for DNS cluster nodes |
| `DNS_POLL_INTERVAL_MS` | `5000` | DNS poll interval for cluster discovery |
| `STARCITE_RAFT_DATA_DIR` | `priv/raft` | Filesystem path for Raft logs and snapshots |
| `STARCITE_ARCHIVER_ENABLED` | `false` | Enable Postgres archiver |
| `STARCITE_POSTGRES_URL` | none | Alternate database URL if `DATABASE_URL` is not set |
| `STARCITE_ARCHIVE_FLUSH_INTERVAL_MS` | `5000` | Archive flush tick interval |
| `DB_POOL_SIZE` | `10` | Postgres connection pool size |
| `DATABASE_URL` | none | Primary Postgres URL when archive is enabled |
| `PORT` | `4000` | HTTP server port |
| `PHX_SERVER` | unset | Required in release mode to start endpoint server |
| `SECRET_KEY_BASE` | none (prod) | Required secret for Phoenix endpoint in production |
| `PHX_HOST` | `example.com` (prod) | Public host used in generated endpoint URLs |

Redis PubSub options (only when `:pubsub_adapter` is configured as `:redis`):

| Variable | Default | Description |
| --- | --- | --- |
| `REDIS_HOST` | `localhost` | Redis host when PubSub adapter is `:redis` |
| `REDIS_PORT` | `6379` | Redis port when PubSub adapter is `:redis` |
