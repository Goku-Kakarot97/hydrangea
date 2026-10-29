#!/usr/bin/env bash
# Usage: benchmark/run-http.sh [rounds=3] [warmup=3] [duration=5] [ioThreads=4] [loadThreads=4] [scenarioFilter]
# Compares Hydrangea with Netty (and the JDK's built-in server) on HTTP, WebSocket and SSE workloads.
set -euo pipefail
cd "$(dirname "$0")"
ROUNDS=${1:-3}; WARMUP=${2:-3}; DURATION=${3:-5}; IO=${4:-4}; LOAD=${5:-4}; FILTER=${6:-}
ROOT="$(cd .. && pwd)"
HYD_CP="$ROOT/target/classes"
R=~/.m2/repository/io/netty; V=4.2.10.Final
NETTY_CP=""
for m in netty-buffer netty-common netty-transport netty-codec-base netty-codec-http netty-handler netty-resolver netty-transport-native-unix-common netty-codec-compression; do
  NETTY_CP="$NETTY_CP:$R/$m/$V/$m-$V.jar"
done
NETTY_CP=${NETTY_CP#:}
(cd "$ROOT" && mvn -q -o compile)
rm -rf out && mkdir out
javac -d out -cp "$HYD_CP:$NETTY_CP" src/servers/HydrangeaHttpServer.java src/servers/NettyHttpServer.java src/servers/JdkHttpServer.java src/bench/HttpLoad.java src/bench/HttpDriver.java
export HYD_CP NETTY_CP BENCH_CP="$PWD/out"
java -cp out bench.HttpDriver "$ROUNDS" "$WARMUP" "$DURATION" "$IO" "$LOAD" "${OUT_MD:-$PWD/RESULTS-HTTP.md}" "$FILTER"
