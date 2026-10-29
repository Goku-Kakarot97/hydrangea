#!/usr/bin/env bash
# gRPC interop: Hydrangea's server against grpc-java and grpc-js clients, and Hydrangea's client against grpc-java and grpc-js servers.
# Needs: mvn (grpc-java deps, fetched into interop/grpc-java), node + npm (@grpc/grpc-js, @grpc/proto-loader), Java 21.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${TMPDIR:-/tmp}/hydrangea-grpc-interop; mkdir -p "$OUT"
mvn -q -o compile
(cd interop/grpc-java && mvn -q dependency:build-classpath -Dmdep.outputFile="$OUT/cp.txt")
[ -d interop/node_modules ] || (cd interop && npm install --silent @grpc/grpc-js @grpc/proto-loader)
javac -d "$OUT" -cp "target/classes:$(cat "$OUT/cp.txt")" interop/GrpcInteropClient.java interop/GrpcJavaServer.java
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT
java -cp target/classes io.hydrangea.examples.GrpcEchoExample 50071 & pids+=($!)
java -cp "$OUT:$(cat "$OUT/cp.txt")" GrpcJavaServer 50072 & pids+=($!)
node interop/grpc-js-server.js 50073 & pids+=($!)
sleep 3
for port in 50071 50072 50073; do
  echo "== Hydrangea client -> server on :$port"; java -cp "$OUT:target/classes" GrpcInteropClient "$port"
done
echo "== grpc-js client -> Hydrangea server"; node interop/grpc-js-client.js 50071
echo "== gRPC-Web (fetch) -> Hydrangea server"; node interop/grpc-web-client.js 50071
echo "== grpcurl (reflection) -> Hydrangea server"; echo "see: docker run --rm fullstorydev/grpcurl -plaintext host.docker.internal:<port> list (start GrpcEchoExample <port> interop/echo.pb)"

# Channelz, binary log and xDS are read back with grpc-java's own generated classes (see ChannelzProbe/JavaChannelzCheck,
# BinaryLogProbe/JavaBinaryLogCheck, XdsInterop; xDS needs io.grpc:grpc-xds on the classpath).
