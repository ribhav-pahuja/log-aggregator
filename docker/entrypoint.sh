#!/bin/sh
set -eu

# Optional wait-for dependencies (set WAIT_FOR_KAFKA=0 / WAIT_FOR_POSTGRES=0 to skip)
WAIT_FOR_KAFKA="${WAIT_FOR_KAFKA:-1}"
WAIT_FOR_POSTGRES="${WAIT_FOR_POSTGRES:-1}"
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-90}"

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

# Parse host:port from KAFKA_BOOTSTRAP_SERVERS (first broker only)
if [ "$WAIT_FOR_KAFKA" = "1" ] && [ -n "${KAFKA_BOOTSTRAP_SERVERS:-}" ]; then
  broker="${KAFKA_BOOTSTRAP_SERVERS%%,*}"
  khost="${broker%%:*}"
  kport="${broker##*:}"
  case "$kport" in
    ''|*[!0-9]*) kport=9092 ;;
  esac
  wait_tcp "$khost" "$kport" "Kafka" || exit 1
fi

# Parse host from DATABASE_URL (postgresql+psycopg://user:pass@host:port/db)
if [ "$WAIT_FOR_POSTGRES" = "1" ] && [ -n "${DATABASE_URL:-}" ]; then
  case "$DATABASE_URL" in
    *sqlite*)
      log "SQLite DATABASE_URL — skipping Postgres wait"
      ;;
    *)
      # crude parse: take segment after @ and before /
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
      ;;
  esac
fi

log "starting: $*"
exec "$@"
