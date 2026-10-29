#!/usr/bin/env bash
# Runs the h2spec HTTP/2 conformance suite (147 tests) against Hydrangea, over cleartext (prior knowledge) and over TLS (ALPN h2). Needs Docker.
# Expected: TLS 147/147; cleartext 146/147 - "3.5/2 Sends invalid connection preface" cannot pass on a port shared with HTTP/1.1:
# bytes that do not start like the HTTP/2 preface are an HTTP/1.1 request, so they get an HTTP/1.1 error page, not a GOAWAY.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
(cd "$ROOT" && mvn -q -o compile)
rm -rf out && mkdir -p out
javac -d out -cp "$ROOT/target/classes" src/H2SpecServer.java
[ -f out/t.p12 ] || keytool -genkeypair -alias t -keyalg RSA -keysize 2048 -validity 2 -dname CN=localhost -storetype PKCS12 -keystore out/t.p12 -storepass changeit >/dev/null 2>&1
java -cp "out:$ROOT/target/classes" H2SpecServer 28080 28443 out/t.p12 > out/server.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for i in $(seq 1 50); do grep -q READY out/server.log && break; sleep 0.2; done
echo "== cleartext"; docker run --rm summerwind/h2spec -h host.docker.internal -p 28080 -S -o 3 | tail -4 || true
echo "== TLS";       docker run --rm summerwind/h2spec -h host.docker.internal -p 28443 -t -k -S -o 3 | tail -4 || true
