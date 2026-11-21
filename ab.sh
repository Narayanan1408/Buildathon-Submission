#!/bin/sh
# A/B speed comparison of two binaries, alternating runs to cancel out noise.
# Usage: ./ab.sh BIN_A BIN_B [DATA_DIR] [ROUNDS]
set -eu
A="$1"; B="$2"; DATA="${3:-../dev20}"; ROUNDS="${4:-6}"
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
run() { rm -rf "$OUT"/*; /usr/bin/time -p env DATA_DIR="$DATA" OUTPUT_DIR="$OUT" NCPU="${NCPU:-8}" "$1" 2>&1 >/dev/null | awk '/real|user|sys/ {printf "%s ", $2}'; }
ra=""; rb=""
i=0
while [ "$i" -lt "$ROUNDS" ]; do
  ra="$ra $(run "$A" | awk '{print $1"/"$2"/"$3}')"
  rb="$rb $(run "$B" | awk '{print $1"/"$2"/"$3}')"
  i=$((i+1))
done
python3 - "$ra" "$rb" <<'PY'
import sys, statistics as s
for name, col in (("A", sys.argv[1]), ("B", sys.argv[2])):
    rows = [tuple(map(float, x.split("/"))) for x in col.split()]
    print(f"{name}: wall median {s.median(r[0] for r in rows):.3f}s  min {min(r[0] for r in rows):.3f}s  cpu(user+sys) median {s.median(r[1]+r[2] for r in rows):.2f}s")
PY
