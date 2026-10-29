#!/usr/bin/env bash
# MariaDB for the compatibility tests of hydrangea-mysql:  hydrangea-mariadb  23310  (user hydra / hydra, database hydra)
# usage: start-mariadb.sh [stop]
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-mariadb >/dev/null 2>&1 || true; exit 0; fi
IMG=${MARIADB_IMAGE:-mariadb:11}
docker rm -f hydrangea-mariadb >/dev/null 2>&1 || true
docker run -d --name hydrangea-mariadb -p 23310:3306 -e MARIADB_ROOT_PASSWORD=root -e MARIADB_DATABASE=hydra -e MARIADB_USER=hydra -e MARIADB_PASSWORD=hydra "$IMG" --max-connections=200 >/dev/null
for i in $(seq 1 90); do
  if docker exec hydrangea-mariadb mariadb -uhydra -phydra hydra -e 'select 1' >/dev/null 2>&1; then break; fi
  sleep 1
done
echo "MariaDB up on 23310"
