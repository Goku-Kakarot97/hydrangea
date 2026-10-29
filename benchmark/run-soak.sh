#!/usr/bin/env bash
# Usage: benchmark/run-soak.sh [minutes=15]   Mixed-traffic stability test against a local Hydrangea server; writes SOAK.md
set -euo pipefail
cd "$(dirname "$0")"
MIN=${1:-15}
ROOT="$(cd .. && pwd)"
HYD_CP="$ROOT/target/classes"
R=~/.m2/repository/io/netty; V=4.2.10.Final
NETTY_CP=""
for m in netty-buffer netty-common netty-transport netty-codec-base netty-codec-http netty-handler netty-resolver netty-transport-native-unix-common netty-codec-compression; do
  NETTY_CP="$NETTY_CP:$R/$m/$V/$m-$V.jar"
done
NETTY_CP=${NETTY_CP#:}
(cd "$ROOT" && mvn -q -o compile)
rm -rf out-soak && mkdir out-soak
javac -d out-soak -cp "$HYD_CP:$NETTY_CP" src/servers/HydrangeaHttpServer.java src/servers/NettyHttpServer.java src/servers/JdkHttpServer.java src/bench/HttpLoad.java src/bench/Chaos.java src/bench/Soak.java
export HYD_CP BENCH_CP="$PWD/out-soak"
java -cp out-soak bench.Soak "$MIN" "$PWD/SOAK.md"
