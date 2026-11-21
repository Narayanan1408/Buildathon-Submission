#!/bin/sh
# Speed benchmark: runs the native binary N times on a dataset and prints each
# run's wall time plus the median. Usage: ./bench.sh [DATA_DIR] [RUNS]
# Example: ./bench.sh ../dev20 5
set -eu
DATA="${1:-../dev20}"
RUNS="${2:-5}"
BIN=target/release/shopping-sessions
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT
times=""
i=0
while [ "$i" -lt "$RUNS" ]; do
  rm -rf "$OUT"/*
  start=$(python3 -c 'import time; print(time.time())')
  DATA_DIR="$DATA" OUTPUT_DIR="$OUT" NCPU="${NCPU:-8}" $BIN
  end=$(python3 -c 'import time; print(time.time())')
  t=$(python3 -c "print(f'{$end - $start:.3f}')")
  echo "run $((i + 1)): ${t}s"
  times="$times $t"
  i=$((i + 1))
done
python3 -c "import statistics as s; v=[float(x) for x in '$times'.split()]; print(f'median {s.median(v):.3f}s  min {min(v):.3f}s')"
