#!/usr/bin/env bash
# Hydrangea PostgreSQL client vs pgjdbc + HikariCP, with the load generator running inside the Kubernetes cluster next to the database.
# usage: PGHOST=... PGPORT=5432 PGUSER=... PGPASSWORD=... PGDB=... benchmark/run-k8s-pg.sh OUT_DIR [rounds]
# env:   NS (namespace, default: the one of the current kubectl context), SCENARIOS (default "select1 calc series": read-only, they need no table and write nothing),
#        CONFIGS (connections:callers, default "8:8 8:64 32:32": never more than 32 connections to the database), SECS (seconds measured per run, default 5), CPU (default 2),
#        KEEP=1 to leave the objects in place.
# The login is passed to the cluster as a Secret (and is not written anywhere else); everything created carries the label app.kubernetes.io/part-of=hydrangea-bench and is
# deleted at the end (also when interrupted). The scenarios insert and tx create tables in the database and write to it: they are refused unless ALLOW_WRITES=yes.
# Needs: the project built (mvn -q compile), the jars of pg-bench.sh in ~/.m2, the image eclipse-temurin:21-jre reachable from the cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=${1:?usage: run-k8s-pg.sh OUT_DIR [rounds]}; ROUNDS=${2:-3}
: "${PGHOST:?}" "${PGPORT:?}" "${PGUSER:?}" "${PGPASSWORD:?}" "${PGDB:?}"
NS=${NS:-$(kubectl config view --minify -o 'jsonpath={..namespace}')}; NS=${NS:-default}
SCENARIOS=${SCENARIOS:-select1 calc series}; CONFIGS=${CONFIGS:-8:8 8:64 32:32}; SECS=${SECS:-5}; CPU=${CPU:-2}
case " $SCENARIOS " in *" insert "*|*" tx "*|*" point "*|*" rows "*) [ "${ALLOW_WRITES:-}" = yes ] || { echo "point, rows, insert and tx need the tables of 'PgBench setup' (and insert and tx write): set ALLOW_WRITES=yes if that database may take them" >&2; exit 1; } ;; esac
M=${M:-$HOME/.m2/repository}; L=app.kubernetes.io/part-of=hydrangea-bench
T=$(mktemp -d); trap '[ "${KEEP:-}" = 1 ] || kubectl -n "$NS" delete job,cm,secret -l $L >/dev/null 2>&1 || true; rm -rf "$T"' EXIT
mkdir -p "$T/all" "$T/b"
cp -r hydrangea-core/target/classes/. "$T/all/"; cp -r hydrangea-postgres/target/classes/. "$T/all/"
jar cf "$T/hydrangea.jar" -C "$T/all" .
javac --release 21 -cp "$T/hydrangea.jar:$M/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar:$M/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar" -d "$T/b" benchmark/src/postgres/PgBench.java
jar cf "$T/bench.jar" -C "$T/b" .
cp "$M/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar" "$T/HikariCP.jar"; cp "$M/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar" "$T/slf4j.jar"
split -b 560000 "$M/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar" "$T/part-"
kubectl -n "$NS" delete job,cm,secret -l $L >/dev/null 2>&1 || true
kubectl -n "$NS" create configmap hydrangea-pg-jars --from-file="$T/hydrangea.jar" --from-file="$T/bench.jar" --from-file="$T/HikariCP.jar" --from-file="$T/slf4j.jar" >/dev/null
kubectl -n "$NS" create configmap hydrangea-pg-pgjdbc-a --from-file="$T/part-aa" >/dev/null
kubectl -n "$NS" create configmap hydrangea-pg-pgjdbc-b --from-file="$T/part-ab" >/dev/null
kubectl -n "$NS" create secret generic hydrangea-pg-db --from-literal=PGHOST="$PGHOST" --from-literal=PGPORT="$PGPORT" --from-literal=PGUSER="$PGUSER" --from-literal=PGPASSWORD="$PGPASSWORD" --from-literal=PGDB="$PGDB" >/dev/null
kubectl -n "$NS" label configmap/hydrangea-pg-jars configmap/hydrangea-pg-pgjdbc-a configmap/hydrangea-pg-pgjdbc-b secret/hydrangea-pg-db $L >/dev/null
RUNS=$(( ROUNDS * $(echo $CONFIGS | wc -w) * $(echo $SCENARIOS | wc -w) * 5 ))
JOB=hydrangea-pg-load
sed -e "s/__NAME__/$JOB/" -e "s/__NAMESPACE__/$NS/" -e "s/__DEADLINE__/$(( RUNS * (SECS + 8) + 300 ))/" -e "s/__SCENARIOS__/$SCENARIOS/" -e "s/__CONFIGS__/$CONFIGS/" \
    -e "s/__ROUNDS__/$ROUNDS/" -e "s/__SECS__/$SECS/" -e "s/__CPU__/$CPU/" k8s/pg-load.yaml.tmpl | kubectl apply -f - >/dev/null
echo "job $JOB started in namespace $NS: about $RUNS runs of $SECS s"
kubectl -n "$NS" wait --for=condition=ready pod -l app=hydrangea-pg-load --timeout=300s >/dev/null 2>&1 || true
mkdir -p "$OUT"
kubectl -n "$NS" wait --for=condition=complete "job/$JOB" --timeout=$(( RUNS * (SECS + 8) + 300 ))s >/dev/null
kubectl -n "$NS" logs "job/$JOB" > "$OUT/k8s-pg.log"
grep -c '^RESULT' "$OUT/k8s-pg.log" | sed 's/^/results: /'
