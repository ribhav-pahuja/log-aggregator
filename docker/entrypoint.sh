#!/bin/sh
set -eu

WAIT_FOR_KAFKA="${WAIT_FOR_KAFKA:-1}"
WAIT_FOR_POSTGRES="${WAIT_FOR_POSTGRES:-1}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-90}"
RUN_MIGRATIONS="${RUN_MIGRATIONS:-1}"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [entrypoint] $*"; }

wait_tcp() {
  host="$1"
  port="$2"
  name="$3"
  i=0
  while [ "$i" -lt "$WAIT_TIMEOUT_SECONDS" ]; do
    if python -c "import socket; s=socket.socket(); s.settimeout(1); s.connect(('$host', $port)); s.close()" 2>/dev/null; then
      log "$name is reachable at $host:$port"
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  log "ERROR: timed out waiting for $name at $host:$port"
  return 1
}

if [ "$WAIT_FOR_KAFKA" = "1" ] && [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
  broker="${KAFKA_BOOTSTRAP_SERVERS%%,*}"
  khost="${broker%%:*}"
  kport="${broker##*:}"
  case "$kport" in
    ''|*[!0-9]*) kport=9092 ;;
  esac
  wait_tcp "$khost" "$kport" "Kafka" || exit 1
fi

if [ "$WAIT_FOR_POSTGRES" = "1" ]; then
  if [ -z "${DATABASE_URL:-}" ]; then
    log "ERROR: DATABASE_URL is required (PostgreSQL, set it in .env)"
    exit 1
  fi
  case "$DATABASE_URL" in
    *sqlite*)
      log "ERROR: SQLite DATABASE_URL is not supported; use PostgreSQL"
      exit 1
      ;;
  esac
  rest="${DATABASE_URL#*@}"
  hostport="${rest%%/*}"
  phost="${hostport%%:*}"
  pport="${hostport##*:}"
  case "$pport" in
    ''|*[!0-9]*) pport=5432 ;;
  esac
  if [ -n "$phost" ]; then
    wait_tcp "$phost" "$pport" "Postgres" || exit 1
  fi
fi

if [ "$RUN_MIGRATIONS" = "1" ] && [ -n "${DATABASE_URL:-}" ]; then
  case "$DATABASE_URL" in
    *sqlite*)
      log "ERROR: SQLite is not supported for migrations"
      exit 1
      ;;
  esac
  if command -v alembic >/dev/null 2>&1; then
    log "Running alembic upgrade head"
    run_alembic() {
      if [ -f /app/alembic.ini ]; then
        (cd /app && "$@")
      elif [ -f alembic.ini ]; then
        "$@"
      else
        return 1
      fi
    }
    if ! run_alembic alembic upgrade head; then
      log "WARN: alembic upgrade failed; attempting stamp head for pre-existing schema"
      run_alembic alembic stamp head || log "WARN: alembic stamp failed"
    fi
  else
    log "WARN: alembic not installed; ensure schema is migrated before start"
  fi
fi

log "starting: $*"
exec "$@"
