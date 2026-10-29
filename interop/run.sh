#!/usr/bin/env bash
# Runs Hydrangea's HTTP client against Node.js, an independent implementation:
#  - HTTP/2 against Node's http2 server, cleartext (prior knowledge) and TLS (ALPN): GET, 3 MB POST echo, 8 MB download, redirect, 100 concurrent
#    requests, an event stream, a refused stream, a timeout on one stream while another keeps going; gzip/deflate decoding; cookies
#  - an HTTP proxy written in Node (absolute-form requests and CONNECT tunnels, Basic credentials) in front of Node's origins
# Needs node, openssl and keytool.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
(cd "$ROOT" && mvn -q -o compile)
rm -rf out && mkdir -p out
javac -d out -cp "$ROOT/target/classes" Http2InteropClient.java
openssl req -x509 -newkey rsa:2048 -nodes -keyout out/key.pem -out out/cert.pem -days 2 -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
keytool -importcert -noprompt -alias n -file out/cert.pem -keystore out/trust.p12 -storetype PKCS12 -storepass changeit >/dev/null 2>&1
node node-h2-server.js h2c 29080 > out/h2c.log 2>&1 &
P1=$!
node node-h2-server.js tls 29443 out/key.pem out/cert.pem > out/tls.log 2>&1 &
P2=$!
node node-h2-server.js h1 29081 > out/h1.log 2>&1 &
P3=$!
node node-proxy.js 29128 alice:secret > out/proxy.log 2>&1 &
P4=$!
trap 'kill $P1 $P2 $P3 $P4 2>/dev/null || true' EXIT
for i in $(seq 1 50); do grep -q READY out/h2c.log && grep -q READY out/tls.log && grep -q READY out/h1.log && grep -q READY out/proxy.log && break; sleep 0.2; done
echo "== cleartext (prior knowledge)"; java -cp "out:$ROOT/target/classes" Http2InteropClient h2c 29080
echo "== TLS (ALPN h2)";               java -cp "out:$ROOT/target/classes" Http2InteropClient tls 29443 out/trust.p12
echo "== through a Node proxy that wants alice:secret"; java -cp "out:$ROOT/target/classes" Http2InteropClient proxy 29128 29081 29443 out/trust.p12 alice:secret
