#!/bin/sh
# Redis 7 for the tests, on 127.0.0.1 (two containers):
#   hydrangea-redis-test  26379  no password;   26380  TLS only (certificates next to this script, no client certificate)
#   hydrangea-redis-auth  26381  password "secret" on the default user, and user "alice" / "alicepw" who may only run GET, SET and PING on keys "allowed:*"
# Remove with: docker rm -f hydrangea-redis-test hydrangea-redis-auth
set -e
cd "$(dirname "$0")"
W=$(mktemp -d)
cp ca.pem server.pem server.key "$W/"
chmod 644 "$W"/*
chmod 755 "$W"
cat > "$W/plain.conf" <<'X'
port 6379
tls-port 6380
tls-cert-file /conf/server.pem
tls-key-file /conf/server.key
tls-ca-cert-file /conf/ca.pem
tls-auth-clients no
save ""
appendonly no
protected-mode no
maxmemory-policy noeviction
X
cat > "$W/auth.conf" <<'X'
port 6379
requirepass secret
user alice on >alicepw ~allowed:* -@all +get +set +ping +hello +client
save ""
appendonly no
protected-mode no
X
for n in test auth; do docker rm -f hydrangea-redis-$n >/dev/null 2>&1 || true; done
docker create --name hydrangea-redis-test -p 127.0.0.1:26379:6379 -p 127.0.0.1:26380:6380 redis:7 redis-server /conf/plain.conf >/dev/null
docker create --name hydrangea-redis-auth -p 127.0.0.1:26381:6379 redis:7 redis-server /conf/auth.conf >/dev/null
for n in test auth; do docker cp "$W" hydrangea-redis-$n:/conf; done
rm -rf "$W"
docker start hydrangea-redis-test hydrangea-redis-auth >/dev/null
echo "waiting for Redis..."
for i in $(seq 1 30); do
  if docker exec hydrangea-redis-test redis-cli ping >/dev/null 2>&1 && docker exec hydrangea-redis-auth redis-cli -a secret --no-auth-warning ping >/dev/null 2>&1; then echo ready; exit 0; fi
  sleep 1
done
echo "Redis did not become ready"; exit 1
