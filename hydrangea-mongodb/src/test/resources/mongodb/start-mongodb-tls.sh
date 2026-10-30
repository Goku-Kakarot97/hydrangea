#!/usr/bin/env bash
# MongoDB servers with TLS for the tests of hydrangea-mongodb, in Docker (no users: TLS is what is tested here):
#   hydrangea-mongo-tls        29021  requireTLS, certificate valid for localhost / 127.0.0.1 (ca.pem signs it)
#   hydrangea-mongo-tls-wrong  29022  requireTLS, certificate valid only for other.example
# usage: start-mongodb-tls.sh [stop]
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-mongo-tls hydrangea-mongo-tls-wrong >/dev/null 2>&1 || true; exit 0; fi
IMG=${MONGO_IMAGE:-mongo:7}
start() { # name port cert-base
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" -p "127.0.0.1:$2:27017" -v "$HERE:/certs:ro" --entrypoint bash "$IMG" -c "
    cat /certs/$3.key /certs/$3.pem > /tmp/server.pem && chmod 644 /tmp/server.pem &&
    exec mongod --bind_ip_all --tlsMode requireTLS --tlsCertificateKeyFile /tmp/server.pem --tlsCAFile /certs/ca.pem --tlsAllowConnectionsWithoutCertificates" >/dev/null
}
start hydrangea-mongo-tls 29021 server
start hydrangea-mongo-tls-wrong 29022 wrongname
for name in hydrangea-mongo-tls hydrangea-mongo-tls-wrong; do
  for i in $(seq 1 60); do
    if docker logs "$name" 2>&1 | grep -q "Waiting for connections"; then break; fi
    sleep 1
  done
done
echo "MongoDB with TLS up on 29021 (localhost certificate) and 29022 (wrong name)"
