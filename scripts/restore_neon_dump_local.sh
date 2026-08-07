#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
set -a
# shellcheck disable=SC1091
source .env
set +a

echo "DB_HOST=${DB_HOST:-localhost}"
echo "DB_PORT=${DB_PORT:-5432}"
echo "DB_USER=${DB_USER:-postgres}"
echo "DB_NAME=${DB_NAME:-postgres}"
echo "DB_PASSWORD_len=${#DB_PASSWORD}"

export PGPASSWORD="${DB_PASSWORD}"
echo "PGPASSWORD_exported_len=${#PGPASSWORD}"

# Prefer IPv4 to avoid ::1 auth quirks
HOST="${DB_HOST:-localhost}"
if [[ "$HOST" == "localhost" ]]; then HOST="127.0.0.1"; fi

psql -h "$HOST" -p "${DB_PORT:-5432}" -U "${DB_USER:-postgres}" -d "${DB_NAME:-postgres}" -c "SELECT current_database(), current_user;"

DUMP="${1:-backups/neon_20260723_082554.dump}"
LOG="backups/restore_$(date +%Y%m%d_%H%M%S).log"
echo "[$(date '+%F %T')] restoring $DUMP" | tee "$LOG"

set +e
pg_restore \
  --host="$HOST" \
  --port="${DB_PORT:-5432}" \
  --username="${DB_USER:-postgres}" \
  --dbname="${DB_NAME:-postgres}" \
  --clean \
  --if-exists \
  --no-owner \
  --no-acl \
  --jobs=4 \
  --verbose \
  "$DUMP" >>"$LOG" 2>&1
EC=$?
set -e

echo "[$(date '+%F %T')] pg_restore exit=$EC" | tee -a "$LOG"
grep -E 'error:|warning: errors|finished main|exit=' "$LOG" | tail -40 || true

psql -h "$HOST" -p "${DB_PORT:-5432}" -U "${DB_USER:-postgres}" -d "${DB_NAME:-postgres}" -c \
  "SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC NULLS LAST LIMIT 12;"
