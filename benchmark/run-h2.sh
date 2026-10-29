#!/usr/bin/env bash
# Usage: benchmark/run-h2.sh [seconds=12] [ioThreads=4] [loadThreads=4]
# HTTP/2 (cleartext, prior knowledge): Hydrangea vs Netty (Http2FrameCodec), identical Netty-client load (src/h2/H2Bench.java).
set -euo pipefail
cd "$(dirname "$0")/.."
SECS=${1:-12}; IO=${2:-4}; LT=${3:-4}
OUT=${BENCH_OUT:-$PWD/benchmark/out-h2}; rm -rf "$OUT"; mkdir -p "$OUT"
R=~/.m2/repository/io/netty; V=4.2.10.Final
NETTY_CP=""
for m in netty-buffer netty-common netty-transport netty-codec-base netty-codec-http netty-codec-http2 netty-handler netty-resolver netty-transport-native-unix-common netty-codec-compression; do
  NETTY_CP="$NETTY_CP:$R/$m/$V/$m-$V.jar"
done
NETTY_CP=${NETTY_CP#:}
mvn -q -o compile
javac --release 21 -d "$OUT" -cp "target/classes:$NETTY_CP" benchmark/src/h2/H2Bench.java benchmark/src/servers/HydrangeaHttpServer.java
start() { # impl
  if [ "$1" = hydrangea ]; then
    java -Xmx1g -Dh2=true -cp "$OUT:target/classes" servers.HydrangeaHttpServer 18090 "$IO" > "$OUT/server.log" 2>&1 & SP=$!
    until grep -q READY "$OUT/server.log" 2>/dev/null || ! kill -0 $SP 2>/dev/null; do sleep 0.3; done
  else
    java -Xmx1g -cp "$OUT:$NETTY_CP" h2.H2Bench server netty 18090 "$IO" > "$OUT/server.log" 2>&1 & SP=$!
    until grep -q READY "$OUT/server.log" || ! kill -0 $SP 2>/dev/null; do sleep 0.3; done
  fi
  sleep 1
}
for SCN in "plain 4 16" "plain 16 64" "echo1k 4 16" "download1m 4 2"; do
  set -- $SCN
  echo "# $1, $2 connections x $3 streams"
  for IMPL in hydrangea netty; do
    start $IMPL
    printf '%-9s ' "$IMPL"
    java -Xmx1g -cp "$OUT:$NETTY_CP" h2.H2Bench load 18090 "$1" "$2" "$3" "$SECS" "$SP" "$LT" 2>"$OUT/load.err" | sed 's/^RESULT //'
    kill $SP; wait $SP 2>/dev/null || true
  done
done
