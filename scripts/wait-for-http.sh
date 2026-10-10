#!/usr/bin/env bash
# Waits until each URL answers 200 with a body that contains the expected text.
#
# Usage:
#   scripts/wait-for-http.sh <url> <expected-text> [<url> <expected-text> ...]
#
# An empty expected text accepts any body. Each URL gets WAIT_SECONDS (default 180) seconds; the
# script fails with the last answer it saw as soon as one of them runs out.

set -euo pipefail

if [ "$#" -eq 0 ] || [ $(($# % 2)) -ne 0 ]; then
  echo "Usage: $0 <url> <expected-text> [<url> <expected-text> ...]" >&2
  exit 2
fi

timeout="${WAIT_SECONDS:-180}"

while [ "$#" -gt 0 ]; do
  url="$1"
  expected="$2"
  shift 2
  echo "Waiting for $url (${timeout}s) …"
  last=""
  ok=""
  for i in $(seq 1 "$timeout"); do
    if last=$(curl -sf -m 5 "$url" 2>&1) && { [ -z "$expected" ] || [[ "$last" == *"$expected"* ]]; }; then
      echo "  ✓ answered after ${i}s."
      ok=1
      break
    fi
    sleep 1
  done
  if [ -z "$ok" ]; then
    echo "  ✗ $url did not answer as expected within ${timeout}s. Last answer: ${last:-none}" >&2
    exit 1
  fi
done
