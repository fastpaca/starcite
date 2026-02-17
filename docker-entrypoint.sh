#!/bin/bash
set -euo pipefail

APP_BIN="/app/starcite/bin/starcite"
CMD="${1:-start}"
MIGRATE_ON_BOOT="${MIGRATE_ON_BOOT:-true}"
ARCHIVE_ADAPTER="${STARCITE_ARCHIVE_ADAPTER:-s3}"

is_postgres_adapter() {
  local adapter
  adapter="$(echo "$ARCHIVE_ADAPTER" | tr '[:upper:]' '[:lower:]' | xargs)"
  [ "$adapter" = "postgres" ]
}

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
    if is_postgres_adapter; then
      require_database_url

      if [ "${MIGRATE_ON_BOOT}" = "true" ]; then
        run_migrations
      else
        echo "Skipping database migrations because MIGRATE_ON_BOOT=${MIGRATE_ON_BOOT}";
      fi
    else
      echo "Skipping database requirement and migrations in archive adapter mode: ${ARCHIVE_ADAPTER}";
    fi
    ;;
  *)
    # For commands like eval, remote, etc we skip automatic migrations
    ;;
esac

exec "$APP_BIN" "$@"
