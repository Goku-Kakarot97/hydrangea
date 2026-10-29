#!/bin/sh
# PgBouncer in front of the PostgreSQL of start-postgres.sh (hydrangea-pg-test, port 25432), in transaction pooling mode (the mode that breaks session state), two containers:
#   hydrangea-pgbouncer      26432  PgBouncer 1.26 with max_prepared_statements = 200: named prepared statements work through it (it renames them per server connection); 4 server connections
#   hydrangea-pgbouncer-old  26433  max_prepared_statements = 0, as PgBouncer before 1.21: a named statement lives on the one server connection that prepared it; ONE server connection, so that
#                                   another client's use of it is easy to arrange
# Needs start-postgres.sh to have run (the database "hydra", user "hydra", password "hydra"). PgBouncer reaches it by container name on the Docker network hydra-net (the name
# host.docker.internal is not resolved from inside the PgBouncer container).
# Remove with: docker rm -f hydrangea-pgbouncer hydrangea-pgbouncer-old
set -e
docker network create hydra-net >/dev/null 2>&1 || true
docker network connect hydra-net hydrangea-pg-test >/dev/null 2>&1 || true
start() { # name port pool prepared
  docker rm -f "$1" >/dev/null 2>&1 || true
  docker run -d --name "$1" --network hydra-net -p "127.0.0.1:$2:5432" -e DB_HOST=hydrangea-pg-test -e DB_PORT=5432 -e DB_USER=hydra -e DB_PASSWORD=hydra -e DB_NAME=hydra \
    -e POOL_MODE=transaction -e AUTH_TYPE=scram-sha-256 -e DEFAULT_POOL_SIZE="$3" -e MAX_CLIENT_CONN=500 -e MAX_PREPARED_STATEMENTS="$4" -e ADMIN_USERS=hydra \
    edoburu/pgbouncer:latest >/dev/null
}
start hydrangea-pgbouncer 26432 4 200
start hydrangea-pgbouncer-old 26433 1 0
sleep 3
for p in 26432 26433; do nc -z 127.0.0.1 $p && echo "pgbouncer on $p is up" || { echo "pgbouncer on $p did not start" >&2; exit 1; }; done
