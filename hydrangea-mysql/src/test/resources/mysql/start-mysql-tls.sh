#!/usr/bin/env bash
# MySQL servers with TLS for the TLS tests of hydrangea-mysql (the certificates are those of the PostgreSQL tests, valid for localhost / 127.0.0.1; make-certs.sh there renews them):
#   hydrangea-my-tls         23308  certificate valid for localhost / 127.0.0.1
#   hydrangea-my-tls-wrong   23309  certificate valid only for other.example
# usage: start-mysql-tls.sh [stop]
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
if [ "${1:-}" = stop ]; then docker rm -f hydrangea-my-tls hydrangea-my-tls-wrong >/dev/null 2>&1 || true; exit 0; fi
IMG=${MYSQL_IMAGE:-mysql:8.4}
start() { # name port cert-base
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" -p "127.0.0.1:$2:3306" -v "$HERE:/certs:ro" -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=hydra -e MYSQL_USER=hydra -e MYSQL_PASSWORD=hydra \
    --entrypoint sh "$IMG" -c "
      mkdir -p /etc/mysql-tls && cp /certs/ca.pem /etc/mysql-tls/ca.pem && cp /certs/$3.pem /etc/mysql-tls/server.pem && cp /certs/$3.key /etc/mysql-tls/server.key &&
      chown -R mysql:mysql /etc/mysql-tls && chmod 600 /etc/mysql-tls/server.key &&
      exec docker-entrypoint.sh mysqld --ssl-ca=/etc/mysql-tls/ca.pem --ssl-cert=/etc/mysql-tls/server.pem --ssl-key=/etc/mysql-tls/server.key --max-connections=200" >/dev/null
}
start hydrangea-my-tls 23308 server
start hydrangea-my-tls-wrong 23309 wrongname
for name in hydrangea-my-tls hydrangea-my-tls-wrong; do
  for i in $(seq 1 90); do
    if docker exec "$name" mysql -uhydra -phydra hydra -e 'select 1' >/dev/null 2>&1; then break; fi
    sleep 1
  done
done
echo "MySQL with TLS up on 23308 (valid certificate) and 23309 (certificate for another host)"
