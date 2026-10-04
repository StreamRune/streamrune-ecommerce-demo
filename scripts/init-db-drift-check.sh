#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# init-db-drift-check.sh — fail if scripts/init-db.sql has drifted from the
# StreamRune framework's schema baselines.
#
# The demo owns scripts/init-db.sql (it pre-creates the schema for docker-compose
# and the native smoke test, because the library's Flyway-on-startup would abort
# against a pre-created schema). That file is generated from the framework's
# baselines (scripts/regenerate-init-db.sh) and committed, so it can silently
# fall behind when StreamRune changes a baseline unless someone reruns that
# script and this check verifies the result.
#
# This check is SCHEMA-LEVEL, not textual: it applies init-db.sql to one database
# and the raw framework baselines to another, then — for every table the
# framework defines — asserts an EXACT match between the two databases: every
# (table, column, type, nullability, DEFAULT), every INDEX, and every PRIMARY
# KEY / UNIQUE / CHECK / FOREIGN KEY constraint must be identical. Both a
# column/index/constraint the framework defines and init-db.sql lacks, AND one
# init-db.sql adds to a framework-defined table that the framework does not
# define, are drift. It compares the databases' own canonical catalog forms
# (information_schema.columns, pg_indexes.indexdef, pg_get_constraintdef), so it
# is robust to cosmetic differences (CREATE TABLE vs CREATE TABLE IF NOT EXISTS,
# comments, whitespace, statement order).
#
# A table init-db.sql defines that the framework does NOT (a future demo-only
# table) is allowed and is not compared — only tables the framework baselines
# actually create are held to the exact-match rule.
#
# Drift it catches: a baseline change adds/alters/removes a table, column,
# default, index, or constraint that init-db.sql was never regenerated to
# include, or init-db.sql carries a stale/stray column, index, or constraint on
# a table the framework defines.
#
# Usage: bash scripts/init-db-drift-check.sh
# Requires: Docker. Override the framework location with STREAMRUNE_DIR=/path.
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
INIT_DB="$DEMO_DIR/scripts/init-db.sql"
SR="${STREAMRUNE_DIR:-$DEMO_DIR/../streamrune}"
# The event-store series does not use Flyway's default classpath:db/migration (that location is
# the CONSUMING APPLICATION's own); it lives in classpath:db/streamrune-migration, on its own
# flyway_schema_history_streamrune history table. The crypto series was already namespaced.
# Both locations now ship a single clean baseline file (V001__..._baseline.sql) rather than an
# incremental V-series.
MIG="$SR/streamrune-eventstore/streamrune-postgres/src/main/resources/db/streamrune-migration"
CRY="$SR/streamrune-crypto/streamrune-postgres-crypto/src/main/resources/db/crypto-migration"

IMAGE="postgres:18-alpine"
CONT="sr-initdb-drift-$$"
export PGPASSWORD=postgres

for path in "$INIT_DB" "$MIG" "$CRY"; do
  [[ -e "$path" ]] || { echo "ERROR: not found: $path" >&2; exit 2; }
done

cleanup() {
  docker rm -f "$CONT" >/dev/null 2>&1 || true
  rm -f /tmp/sr_*_$$_*.txt /tmp/sr_fw_tables_$$.txt /tmp/sr_initdb_tables_$$.txt
}
trap cleanup EXIT

echo "Starting throwaway $IMAGE ($CONT)…"
docker run -d --name "$CONT" -e POSTGRES_PASSWORD="$PGPASSWORD" -e POSTGRES_DB=postgres "$IMAGE" >/dev/null

# The official postgres image runs a temporary bootstrap server during initdb, which reports
# "ready to accept connections" (and satisfies pg_isready) BEFORE it restarts as the real server.
# Connecting in that window can fail the moment the server bounces (observed: CREATE DATABASE hitting
# the restart). Gate on the entrypoint's "init process complete" marker first, then wait for the
# restarted server to actually accept connections.
ready=""
for _ in $(seq 1 60); do
  if docker logs "$CONT" 2>&1 | grep -q "PostgreSQL init process complete" \
     && docker exec "$CONT" pg_isready -U postgres >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
[[ -n "$ready" ]] || { echo "ERROR: postgres did not become ready" >&2; exit 2; }

run() { docker exec -i "$CONT" psql -v ON_ERROR_STOP=1 -U postgres "$@"; }

run -d postgres -c "CREATE DATABASE initdb;" >/dev/null
run -d postgres -c "CREATE DATABASE migrations;" >/dev/null

echo "Applying init-db.sql → initdb…"
run -d initdb < "$INIT_DB" >/dev/null

echo "Applying framework baselines (eventstore, then crypto) → migrations…"
mapfile -t FILES < <(ls "$MIG"/V*.sql | sort; ls "$CRY"/V*.sql | sort)
for f in "${FILES[@]}"; do
  run -d migrations < "$f" >/dev/null
done

# The framework-defined table set (everything the baselines created in `migrations`). Only these
# tables are held to the exact-match rule; a table init-db.sql defines beyond this set is a future
# demo-only table and is allowed.
run -tA -d migrations -c "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1;" \
  | sort -u > "/tmp/sr_fw_tables_$$.txt"
FW_PATTERN="$(paste -sd'|' "/tmp/sr_fw_tables_$$.txt")"

run -tA -d initdb -c "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY 1;" \
  | sort -u > "/tmp/sr_initdb_tables_$$.txt"
EXTRA_TABLES="$(comm -13 "/tmp/sr_fw_tables_$$.txt" "/tmp/sr_initdb_tables_$$.txt" || true)"
if [[ -n "$EXTRA_TABLES" ]]; then
  echo "  Demo-only tables not defined by the framework (allowed, not compared): $(echo "$EXTRA_TABLES" | tr '\n' ' ')"
fi

# Columns incl. type, nullability AND default. COALESCE keeps no-default columns
# from collapsing to NULL (||-with-NULL is NULL); a stale/missing DEFAULT drifts.
# Format: table_name.column_name:type:nullable:default — filtered on the table_name prefix.
COLSQL="SELECT table_name||'.'||column_name||':'||data_type||':'||is_nullable||':'||COALESCE(column_default,'') \
        FROM information_schema.columns \
        WHERE table_schema='public' ORDER BY 1;"

# Indexes — pg_indexes.indexdef is the catalog's own canonical CREATE INDEX text
# (no IF NOT EXISTS, normalized whitespace/case), so logically identical indexes
# compare equal regardless of how they were written. tablename is prefixed so the
# framework-table filter below can key on it.
IDXSQL="SELECT tablename||'|'||indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY 1;"

# Constraints — pg_get_constraintdef is canonical. Restrict to primary-key /
# unique / check / foreign-key (contype p/u/c/f); NOT NULL (n) is already covered
# by is_nullable in COLSQL and would only add noise. conrelid::regclass::text (the
# table name) leads the line so the framework-table filter below can key on it.
CONSQL="SELECT conrelid::regclass::text||' '||conname||' '||pg_get_constraintdef(oid) \
        FROM pg_constraint \
        WHERE connamespace='public'::regnamespace AND contype IN ('p','u','c','f') \
        ORDER BY 1;"

DRIFT=""
check_category() {
  local label="$1" sql="$2" anchor="$3"
  run -tA -d initdb     -c "$sql" | sort -u > "/tmp/sr_${label}_$$_initdb_all.txt"
  run -tA -d migrations -c "$sql" | sort -u > "/tmp/sr_${label}_$$_mig.txt"
  # Restrict init-db.sql's rows to framework-defined tables: a demo-only table's columns,
  # indexes and constraints are not compared at all (allowed superset at the table level).
  grep -E "^(${FW_PATTERN})${anchor}" "/tmp/sr_${label}_$$_initdb_all.txt" \
    > "/tmp/sr_${label}_$$_initdb_fw.txt" 2>/dev/null || true

  local missing extra
  missing="$(comm -23 "/tmp/sr_${label}_$$_mig.txt" "/tmp/sr_${label}_$$_initdb_fw.txt" || true)"
  extra="$(comm -13 "/tmp/sr_${label}_$$_mig.txt" "/tmp/sr_${label}_$$_initdb_fw.txt" || true)"

  echo "  ${label}: framework defines $(wc -l < "/tmp/sr_${label}_$$_mig.txt" | tr -d ' '), init-db.sql matches $(wc -l < "/tmp/sr_${label}_$$_initdb_fw.txt" | tr -d ' ') on framework-defined tables (exact match required)."

  if [[ -n "$missing" ]]; then
    DRIFT+=$'\n'"Missing ${label} (defined by the framework baselines, absent from init-db.sql):"$'\n'
    DRIFT+="$(echo "$missing" | sed 's/^/  - /')"$'\n'
  fi
  if [[ -n "$extra" ]]; then
    DRIFT+=$'\n'"Extra ${label} on a framework-defined table (present in init-db.sql, not defined by the framework baselines):"$'\n'
    DRIFT+="$(echo "$extra" | sed 's/^/  + /')"$'\n'
  fi
}

echo "Comparing schema: columns+defaults, indexes, constraints (exact match on framework-defined tables)…"
check_category columns     "$COLSQL" '\.'
check_category indexes     "$IDXSQL" '\|'
check_category constraints "$CONSQL" ' '

if [[ -n "$DRIFT" ]]; then
  echo "" >&2
  echo "DRIFT DETECTED: scripts/init-db.sql has diverged from the framework's schema baselines:" >&2
  echo "$DRIFT" >&2
  echo "Regenerate scripts/init-db.sql with scripts/regenerate-init-db.sh and commit it." >&2
  exit 1
fi

echo "OK: init-db.sql matches the framework baselines exactly on every framework-defined table (column, index, constraint)."
