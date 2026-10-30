#!/usr/bin/env bash
# A three-member replica set for the tests of hydrangea-mongodb, in ONE container (so the members reach each other and the host reaches them at the same
# 127.0.0.1:port addresses): hydrangea-mongo-rs, members 127.0.0.1:29018, 29019, 29020, set name rs0, no users (a replica set with users needs a key file).
# usage: start-mongodb-rs.sh [stop]
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-mongo-rs >/dev/null 2>&1 || true; exit 0; fi
IMG=${MONGO_IMAGE:-mongo:7}
docker rm -f hydrangea-mongo-rs >/dev/null 2>&1 || true
docker run -d --name hydrangea-mongo-rs -p 127.0.0.1:29018-29020:29018-29020 --entrypoint bash "$IMG" -c '
  for p in 29018 29019 29020; do mkdir -p /data/$p; mongod --replSet rs0 --port $p --bind_ip_all --dbpath /data/$p --logpath /data/$p.log --fork >/dev/null; done
  sleep infinity' >/dev/null
for i in $(seq 1 60); do
  if docker exec hydrangea-mongo-rs mongosh --port 29018 --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec hydrangea-mongo-rs mongosh --port 29018 --quiet --eval \
  'rs.initiate({_id:"rs0", members:[{_id:0, host:"127.0.0.1:29018"},{_id:1, host:"127.0.0.1:29019"},{_id:2, host:"127.0.0.1:29020"}]})' >/dev/null
for i in $(seq 1 60); do
  if docker exec hydrangea-mongo-rs mongosh --port 29018 --quiet --eval 'rs.status().members.some(m => m.stateStr == "PRIMARY") ? "ok" : "no"' 2>/dev/null | grep -q ok; then break; fi
  sleep 1
done
echo "replica set rs0 up on 29018, 29019, 29020"
