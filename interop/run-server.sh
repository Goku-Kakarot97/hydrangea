#!/usr/bin/env bash
# Checks Hydrangea's HTTP/2 server with independent clients: curl (nghttp2) for an event stream and a 60 MB streamed upload, and
# Node.js's http2 client for a WebSocket over HTTP/2 (RFC 8441 extended CONNECT). Needs curl with HTTP/2 and node.
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"
(cd "$ROOT" && mvn -q -o compile)
rm -rf out-server && mkdir -p out-server
javac -d out-server -cp "$ROOT/target/classes" Http2InteropServer.java
java -cp "out-server:$ROOT/target/classes" Http2InteropServer 29090 > out-server/server.log 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true; rm -rf out-server' EXIT
for i in $(seq 1 50); do grep -q READY out-server/server.log && break; sleep 0.2; done
echo "== curl: event stream over HTTP/2";   curl -s -N -m 5 --http2-prior-knowledge http://127.0.0.1:29090/events | tr '\n' '|'; echo
echo "== curl: negotiated version";          curl -s -o /dev/null -w "%{http_version}\n" --http2-prior-knowledge http://127.0.0.1:29090/hello
echo "== curl: 60 MB streamed upload (the server counts what it received; expect 62914560)"
head -c 62914560 /dev/zero | curl -s -m 30 --http2-prior-knowledge -T - -X POST http://127.0.0.1:29090/sink; echo
echo "== node http2 client: WebSocket over HTTP/2"; node node-ws-over-h2-client.js 29090
echo "== curl --http2 on a cleartext port (the HTTP/1.1 Upgrade: h2c mechanism)"
curl -s -v --http2 http://127.0.0.1:29090/hello 2>&1 | grep -E "Received 101|^< HTTP/2 200|^hello" | sed 's/^/   /'
