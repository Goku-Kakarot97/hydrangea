#!/usr/bin/env bash
# Usage: benchmark/run-grpc.sh [seconds=15] [ioThreads=4]
# gRPC: Hydrangea vs grpc-java (netty), servers x clients, closed-loop load on one machine (see src/grpc/GrpcBench.java).
set -euo pipefail
cd "$(dirname "$0")/.."
SECS=${1:-15}; IO=${2:-4}
OUT=${BENCH_OUT:-$PWD/benchmark/out-grpc}; mkdir -p "$OUT"
mvn -q -o compile
[ -f "$OUT/cp.txt" ] || (cd interop/grpc-java && mvn -q dependency:build-classpath -Dmdep.outputFile="$OUT/cp.txt")
CP="$OUT:target/classes:$(cat "$OUT/cp.txt")"
javac --release 21 -d "$OUT" -cp "target/classes:$(cat "$OUT/cp.txt")" benchmark/src/grpc/GrpcBench.java
run() { # serverImpl clientImpl scenario inflight channels
  java -Xmx1g -cp "$CP" GrpcBench server "$1" 0 "$IO" > "$OUT/server.log" 2>&1 & SP=$!
  until grep -q READY "$OUT/server.log"; do kill -0 $SP 2>/dev/null || { cat "$OUT/server.log"; exit 1; }; sleep 0.2; done
  PORT=$(sed -n 's/READY \([0-9]*\) .*/\1/p' "$OUT/server.log")
  printf '%-9s <- ' "$1"
  java -Xmx1g -cp "$CP" GrpcBench load "$2" "$PORT" "$3" "$4" "$5" "$SECS" "$SP" 2>"$OUT/load.err" | sed 's/^RESULT //'
  kill $SP; wait $SP 2>/dev/null || true
}
for S in ${SCENARIOS:-unary-16 unary-4k unary-64k stream-100x1k}; do
  for LOAD in "1 1" "64 4" "512 8"; do
    for CLIENT in java hydrangea; do
      echo "# $S inflight/channels=$LOAD client=$CLIENT"
      for SERVER in hydrangea java; do
        [ "$S" = stream-100x1k ] && [ "${LOAD%% *}" = 512 ] && continue
        run $SERVER $CLIENT $S ${LOAD% *} ${LOAD#* }
      done
    done
  done
done
