#!/usr/bin/env bash
# Usage: benchmark/run-http2-stress.sh [seconds=60]  - stream-leak hunt for the HTTP/2 client and server (see Http2Stress.java)
set -euo pipefail
cd "$(dirname "$0")"
SECS=${1:-60}
ROOT="$(cd .. && pwd)"
(cd "$ROOT" && mvn -q -o compile)
rm -rf out-stress && mkdir out-stress
javac --release 21 -d out-stress -cp "$ROOT/target/classes" src/bench/Http2Stress.java
java -Xmx512m -cp "out-stress:$ROOT/target/classes" bench.Http2Stress "$SECS" > stress.log 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
until grep -q "^IDLE" stress.log 2>/dev/null || ! kill -0 $PID 2>/dev/null; do sleep 1; done
cat stress.log
if grep -q "^IDLE" stress.log; then
  jcmd $PID GC.class_histogram | grep -E 'Http2(Server|Client)Stage[$]|HttpClient[$]Exchange' | awk '{printf "  %8s instances  %s\n", $2, $4}'
  echo "(a zero or near-zero count for every line means nothing leaked)"
fi
wait $PID || true
tail -2 stress.log
