#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# regenerate-init-db.sh — rebuild scripts/init-db.sql from the StreamRune
# framework's schema baselines.
#
# StreamRune ships its PostgreSQL schema as ONE clean Flyway baseline per
# migration location (no more incremental V-series to concatenate):
#   streamrune-eventstore/streamrune-postgres/.../db/streamrune-migration/V001__streamrune_baseline.sql
#   streamrune-crypto/streamrune-postgres-crypto/.../db/crypto-migration/V001__crypto_baseline.sql
#
# scripts/init-db.sql is their exact concatenation — event store first, then
# crypto — minus the crypto baseline's own
#   SET LOCAL client_min_messages = warning;
# statement (and the comment that explains it): that directive only makes
# sense inside Flyway's own migration transaction, to quiet a NOTICE from its
# CREATE TABLE IF NOT EXISTS statements. Run standalone via psql — as this
# file is, by docker-compose and the native smoke test — there is no
# enclosing migration transaction for SET LOCAL to scope to, and it just
# prints a WARNING of its own instead.
#
# Usage: scripts/regenerate-init-db.sh
# Override the framework checkout with STREAMRUNE_DIR (default: ../streamrune,
# a sibling checkout of this repo).
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(dirname "$SCRIPT_DIR")"
SR="${STREAMRUNE_DIR:-$DEMO_DIR/../streamrune}"

MIG_DIR="$SR/streamrune-eventstore/streamrune-postgres/src/main/resources/db/streamrune-migration"
CRY_DIR="$SR/streamrune-crypto/streamrune-postgres-crypto/src/main/resources/db/crypto-migration"

find_one_baseline() {
  local dir="$1" label="$2"
  local files=()
  [[ -d "$dir" ]] || { echo "ERROR: no such directory: $dir" >&2; exit 2; }
  while IFS= read -r -d '' f; do
    files+=("$f")
  done < <(find "$dir" -maxdepth 1 -name 'V*.sql' -print0 | sort -z)
  if [[ ${#files[@]} -eq 0 ]]; then
    echo "ERROR: no V*.sql baseline found in $dir" >&2
    exit 2
  fi
  if [[ ${#files[@]} -gt 1 ]]; then
    echo "ERROR: expected exactly one baseline file in $dir ($label location), found ${#files[@]}:" >&2
    printf '  %s\n' "${files[@]}" >&2
    exit 2
  fi
  printf '%s' "${files[0]}"
}

MIG_FILE="$(find_one_baseline "$MIG_DIR" "event store")"
CRY_FILE="$(find_one_baseline "$CRY_DIR" "crypto")"

OUT="$DEMO_DIR/scripts/init-db.sql"

{
  cat <<HEADER
-- StreamRune schema initialization for Docker Compose.
--
-- GENERATED FILE — do not hand-edit. Regenerate with:
--   scripts/regenerate-init-db.sh   (reads STREAMRUNE_DIR, default ../streamrune)
--
-- Exact concatenation of the framework's two schema baselines — event store, then crypto — with
-- the crypto baseline's own \`SET LOCAL client_min_messages = warning;\` statement (and the comment
-- that explains it) omitted: that directive only makes sense inside Flyway's own migration
-- transaction; applied standalone via psql — as this file is, by docker-compose and the native
-- smoke test — there is no enclosing transaction for it to scope to, and it just prints a WARNING
-- of its own instead.
--
-- Source files (StreamRune framework checkout):
--   streamrune-eventstore/streamrune-postgres/src/main/resources/db/streamrune-migration/$(basename "$MIG_FILE")
--   streamrune-crypto/streamrune-postgres-crypto/src/main/resources/db/crypto-migration/$(basename "$CRY_FILE")
HEADER
  echo
  cat "$MIG_FILE"
  echo
  sed "/-- Quiet PostgreSQL's NOTICE for each table that already exists/,/^SET LOCAL client_min_messages = warning;\$/d" "$CRY_FILE"
} | cat -s > "$OUT"

echo "Wrote $OUT"
echo "  event store baseline: $MIG_FILE"
echo "  crypto baseline:      $CRY_FILE"
