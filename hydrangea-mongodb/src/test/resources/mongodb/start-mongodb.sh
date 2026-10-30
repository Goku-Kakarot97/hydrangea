#!/usr/bin/env bash
# A MongoDB for the tests of hydrangea-mongodb, in Docker:
#   hydrangea-mongo-test  29017  MongoDB 7, root user hydra / hydra (SCRAM-SHA-256), plus a SCRAM-SHA-1-only user "legacy" / "legacy"
# usage: start-mongodb.sh [stop]
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-mongo-test >/dev/null 2>&1 || true; exit 0; fi
IMG=${MONGO_IMAGE:-mongo:7}
docker rm -f hydrangea-mongo-test >/dev/null 2>&1 || true
docker run -d --name hydrangea-mongo-test -p 29017:27017 -e MONGO_INITDB_ROOT_USERNAME=hydra -e MONGO_INITDB_ROOT_PASSWORD=hydra "$IMG" >/dev/null
for i in $(seq 1 60); do
  if docker exec hydrangea-mongo-test mongosh -u hydra -p hydra --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec hydrangea-mongo-test mongosh -u hydra -p hydra --quiet --eval \
  'db.getSiblingDB("admin").createUser({user:"legacy", pwd:"legacy", roles:[{role:"readWrite", db:"hydra"}], mechanisms:["SCRAM-SHA-1"]})' >/dev/null 2>&1 || true
echo "MongoDB up on 29017"
