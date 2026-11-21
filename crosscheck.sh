#!/bin/sh
# Generates datasets with several seeds and checks that this pipeline's output
# equals a reference implementation's output on each.
# Usage: ./crosscheck.sh REFERENCE_BINARY [EVENTS] [SEEDS...]
set -eu
REF="$1"; shift
EVENTS="${1:-2000000}"; shift || true
SEEDS="${*:-2 3 4 5}"
GEN=../generator/target/release/generate
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT
for s in $SEEDS; do
  $GEN --seed "$s" --events "$EVENTS" --clicks $((EVENTS / 4)) --output "$WORK/data" >/dev/null
  DATA_DIR="$WORK/data" OUTPUT_DIR="$WORK/ref" "$REF" >/dev/null 2>&1
  DATA_DIR="$WORK/data" OUTPUT_DIR="$WORK/new" target/release/shopping-sessions >/dev/null 2>&1
  printf "seed %s: " "$s"; python3 scripts/compare.py "$WORK/new" "$WORK/ref" | tail -1
  rm -rf "$WORK/data" "$WORK/ref" "$WORK/new"
done
