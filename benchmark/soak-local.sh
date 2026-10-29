#!/usr/bin/env bash
# Usage: benchmark/soak-local.sh [minutes=1]   gRPC + HTTP/2 soak against a local SoakServer (see src/h2/SoakLoad.java)
cd "$(dirname "$0")/.."
M=${1:-1}; S=${SOAK_OUT:-$PWD/benchmark/out-soak-h2}; rm -rf "$S"; mkdir -p "$S"
mvn -q -o compile && javac --release 21 -d "$S" -cp target/classes benchmark/src/h2/SoakServer.java benchmark/src/h2/SoakLoad.java 2>&1 | grep -v "^Note"
java -Xms256m -Xmx512m -Dhealth.interval.sec=10 -cp "${S}:target/classes" h2.SoakServer 0 4 127.0.0.1 > "$S/server.log" 2>&1 & SP=$!
for i in $(seq 50); do grep -q READY "$S/server.log" && break; sleep 0.2; done
PORT=$(sed -n 's/READY \([0-9]*\)/\1/p' "$S/server.log")
java -Xmx384m -cp "${S}:target/classes" h2.SoakLoad 127.0.0.1 "$PORT" "$M" 2>&1 | grep -v "^PROGRESS"
sleep 15; grep health "$S/server.log" | tail -2; kill $SP
