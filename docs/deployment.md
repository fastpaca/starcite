---
title: Deployment
sidebar_position: 3
---

# Deployment

FleetLM is designed for self-hosted clustered deployment.

## Hardware guidance

- CPU: 2+ vCPUs per node
- Memory: 4+ GB RAM per node
- Disk: fast SSD/NVMe for Raft logs
- Network: low RTT between cluster nodes

Set `FLEETLM_RAFT_DATA_DIR` to persistent storage so Raft state survives restarts.

## Single-node development

```bash
docker run -d \
  -p 4000:4000 \
  -v fleetlm_data:/data \
  ghcr.io/fastpaca/fleetlm:latest
```

## Three-node cluster

```bash
docker run -d \
  -p 4000:4000 \
  -v /var/lib/fleetlm:/data \
  -e CLUSTER_NODES=fleetlm-1@fleetlm.internal,fleetlm-2@fleetlm.internal,fleetlm-3@fleetlm.internal \
  -e NODE_NAME=fleetlm-1 \
  ghcr.io/fastpaca/fleetlm:latest
```

Repeat with `NODE_NAME=fleetlm-2` and `NODE_NAME=fleetlm-3`.

## Archive (optional Postgres)

```bash
-e FLEETLM_ARCHIVER_ENABLED=true \
-e DATABASE_URL=postgres://user:password@host/db \
-e FLEETLM_ARCHIVE_FLUSH_INTERVAL_MS=5000 \
-e DB_POOL_SIZE=10
```

## Runtime metrics

Prometheus metrics are exposed on `/metrics`.

Core series:

- `fleet_lm_events_append_total`
- `fleet_lm_events_payload_bytes`
- `fleet_lm_archive_pending_rows`
- `fleet_lm_archive_pending_sessions`
- `fleet_lm_archive_flush_duration_ms`
- `fleet_lm_archive_attempted_total`
- `fleet_lm_archive_inserted_total`
- `fleet_lm_archive_lag`
- `fleet_lm_archive_tail_size`
- `fleet_lm_archive_trimmed_total`

## Configuration summary

| Variable | Default | Description |
| --- | --- | --- |
| `NODE_NAME` | random | Node identifier used by clustering |
| `CLUSTER_NODES` | none | Comma-separated peer Erlang node names |
| `DNS_CLUSTER_QUERY` | none | DNS name for libcluster DNS poll strategy |
| `DNS_CLUSTER_NODE_BASENAME` | `fleet_lm` | Base name for DNS cluster nodes |
| `DNS_POLL_INTERVAL_MS` | `5000` | DNS poll interval for cluster discovery |
| `FLEETLM_RAFT_DATA_DIR` | `priv/raft` | Filesystem path for Raft logs and snapshots |
| `FLEETLM_ARCHIVER_ENABLED` | `false` | Enable Postgres archiver |
| `FLEETLM_POSTGRES_URL` | none | Alternate database URL if `DATABASE_URL` is not set |
| `FLEETLM_ARCHIVE_FLUSH_INTERVAL_MS` | `5000` | Archive flush tick interval |
| `DB_POOL_SIZE` | `10` | Postgres connection pool size |
| `REDIS_HOST` | `localhost` | Redis host when PubSub adapter is `:redis` |
| `REDIS_PORT` | `6379` | Redis port when PubSub adapter is `:redis` |
