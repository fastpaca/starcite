#!/bin/bash
set -euo pipefail

APP_BIN="/app/starcite/bin/starcite"
CMD="${1:-start}"
MIGRATE_ON_BOOT="${MIGRATE_ON_BOOT:-true}"

run_migrations() {
  # Only attempt migrations when the archiver (Postgres) is enabled
  case "${STARCITE_ARCHIVER_ENABLED:-}" in
    true|1|yes|on)
      : ;;
    *)
      echo "Skipping database migrations (archiver disabled)."
      return 0
      ;;
  esac

  DB_URL="${DATABASE_URL:-${STARCITE_POSTGRES_URL:-}}"
  if [ -z "$DB_URL" ]; then
    echo "Skipping database migrations (no DATABASE_URL provided)."
    return 0
  fi

  echo "Running database migrations..."
  "$APP_BIN" eval 'Starcite.ReleaseTasks.migrate()'
}

case "$CMD" in
  start|start_iex|daemon|daemon_iex)
    if [ "${MIGRATE_ON_BOOT}" = "true" ]; then
      run_migrations
    else
      echo "Skipping database migrations because MIGRATE_ON_BOOT=${MIGRATE_ON_BOOT}";
    fi
    ;;
  *)
    # For commands like eval, remote, etc we skip automatic migrations
    ;;
esac

exec "$APP_BIN" "$@"
