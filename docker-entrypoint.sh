#!/bin/bash
set -euo pipefail

APP_BIN="/app/starcite/bin/starcite"
CMD="${1:-start}"
MIGRATE_ON_BOOT="${MIGRATE_ON_BOOT:-true}"

require_database_url() {
  DB_URL="${DATABASE_URL:-${STARCITE_POSTGRES_URL:-}}"
  if [ -z "$DB_URL" ]; then
    echo "DATABASE_URL or STARCITE_POSTGRES_URL is required (archiver is mandatory)."
    exit 1
  fi
}

run_migrations() {
  echo "Running database migrations..."
  "$APP_BIN" eval 'Starcite.ReleaseTasks.migrate()'
}

case "$CMD" in
  start|start_iex|daemon|daemon_iex)
    require_database_url

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
