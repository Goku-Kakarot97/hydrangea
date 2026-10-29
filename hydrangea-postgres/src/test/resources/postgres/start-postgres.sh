#!/bin/sh
# PostgreSQL 16 for the tests, on 127.0.0.1, database "hydra", user "hydra", password "hydra" (three containers, one per login method):
#   hydrangea-pg-test        25432  scram-sha-256 (the default)
#   hydrangea-pg-md5         25433  md5
#   hydrangea-pg-password    25434  cleartext password
#   hydrangea-pg-tls         25435  scram-sha-256 and TLS (certificate valid for localhost / 127.0.0.1; run make-certs.sh once first, the files are in git)
#   hydrangea-pg-tls-wrong   25436  the same with a certificate valid only for other.example
# fsync is off: this is for tests and benchmarks, not for data. Remove with: docker rm -f hydrangea-pg-test hydrangea-pg-md5 hydrangea-pg-password hydrangea-pg-tls hydrangea-pg-tls-wrong
set -e
start() { # name port method encryption
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" -p "127.0.0.1:$2:5432" \
    -e POSTGRES_USER=hydra -e POSTGRES_PASSWORD=hydra -e POSTGRES_DB=hydra -e "POSTGRES_HOST_AUTH_METHOD=$3" \
    postgres:16 -c "password_encryption=$4" -c fsync=off -c synchronous_commit=off -c full_page_writes=off \
    -c max_connections=300 -c shared_buffers=256MB >/dev/null
}
HERE=$(pwd)
start_tls() { # name port cert-base
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" -p "127.0.0.1:$2:5432" -v "$HERE:/certs:ro" \
    -e POSTGRES_USER=hydra -e POSTGRES_PASSWORD=hydra -e POSTGRES_DB=hydra --entrypoint sh postgres:16 -c "
      cp /certs/$3.pem /var/lib/postgresql/server.crt && cp /certs/$3.key /var/lib/postgresql/server.key &&
      chown postgres:postgres /var/lib/postgresql/server.* && chmod 600 /var/lib/postgresql/server.key &&
      exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key \
        -c fsync=off -c synchronous_commit=off -c max_connections=300" >/dev/null
}
start hydrangea-pg-test 25432 scram-sha-256 scram-sha-256
start hydrangea-pg-md5 25433 md5 md5
start hydrangea-pg-password 25434 password scram-sha-256
start_tls hydrangea-pg-tls 25435 server
start_tls hydrangea-pg-tls-wrong 25436 wrongname
for p in 25432 25433 25434 25435 25436; do
  n=0
  until docker exec "$(docker ps -q -f publish=$p)" pg_isready -U hydra -d hydra >/dev/null 2>&1; do
    n=$((n+1)); [ $n -gt 60 ] && { echo "postgres on $p did not start" >&2; exit 1; }
    sleep 1
  done
  echo "postgres on $p is up"
done
