#!/usr/bin/env bash
# MySQL servers for the tests of hydrangea-mysql, in Docker:
#   hydrangea-my-test    23306  MySQL 8.4, caching_sha2_password (the default), user hydra / hydra, database hydra
#   hydrangea-my-native  23307  MySQL 8.4 with mysql_native_password enabled for user hydra
# usage: start-mysql.sh [stop]
set -eu
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-my-test hydrangea-my-native >/dev/null 2>&1 || true; exit 0; fi
IMG=${MYSQL_IMAGE:-mysql:8.4}
start() { # name port extra-args...
  local name=$1 port=$2; shift 2
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker run -d --name "$name" -p "$port:3306" -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=hydra -e MYSQL_USER=hydra -e MYSQL_PASSWORD=hydra "$IMG" \
    --max-connections=500 "$@" >/dev/null
}
start hydrangea-my-test 23306
start hydrangea-my-native 23307 --mysql-native-password=ON
for name in hydrangea-my-test hydrangea-my-native; do
  for i in $(seq 1 60); do
    if docker exec "$name" mysqladmin ping -uroot -proot --silent >/dev/null 2>&1 && docker exec "$name" mysql -uhydra -phydra hydra -e 'select 1' >/dev/null 2>&1; then break; fi
    sleep 1
  done
done
docker exec hydrangea-my-native mysql -uroot -proot -e "ALTER USER 'hydra'@'%' IDENTIFIED WITH mysql_native_password BY 'hydra'" 2>/dev/null
for name in hydrangea-my-test hydrangea-my-native; do
  docker exec "$name" mysql -uroot -proot -e "GRANT SELECT ON performance_schema.* TO 'hydra'@'%'" 2>/dev/null
done
echo "MySQL up on 23306 (caching_sha2) and 23307 (native)"
