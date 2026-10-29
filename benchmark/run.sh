#!/usr/bin/env bash
# Usage: benchmark/run.sh [rounds=3] [warmup=3] [duration=5] [ioThreads=4] [loadThreads=4]
# Builds Hydrangea, compiles the benchmark against Hydrangea + the already-built XNIO classes, runs everything.
set -euo pipefail
cd "$(dirname "$0")"
ROUNDS=${1:-3}; WARMUP=${2:-3}; DURATION=${3:-5}; IO=${4:-4}; LOAD=${5:-4}

ROOT="$(cd .. && pwd)"
XNIO_ROOT="${XNIO_ROOT:-$ROOT/../xnio}"
M2=~/.m2/repository
HYD_CP="$ROOT/target/classes"
XNIO_CP="$XNIO_ROOT/api/target/classes:$XNIO_ROOT/nio-impl/target/classes"
for j in org/jboss/logging/jboss-logging/3.4.3.Final/jboss-logging-3.4.3.Final.jar \
         org/jboss/threads/jboss-threads/2.3.6.Final/jboss-threads-2.3.6.Final.jar \
         org/wildfly/common/wildfly-common/1.5.4.Final/wildfly-common-1.5.4.Final.jar \
         org/wildfly/client/wildfly-client-config/1.0.1.Final/wildfly-client-config-1.0.1.Final.jar; do
  XNIO_CP="$XNIO_CP:$M2/$j"
done

(cd "$ROOT" && mvn -q -o compile)
rm -rf out && mkdir out
javac -d out -cp "$HYD_CP:$XNIO_CP" src/servers/*.java src/bench/*.java
export HYD_CP XNIO_CP BENCH_CP="$PWD/out"
java -cp out bench.Driver "$ROUNDS" "$WARMUP" "$DURATION" "$IO" "$LOAD" "$PWD/RESULTS.md"
