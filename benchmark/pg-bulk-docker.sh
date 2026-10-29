#!/usr/bin/env bash
# Bulk insert (PgBatchBench) with the client in a Linux container over a Docker network; see pg-bench-docker.sh.
# usage: benchmark/pg-bulk-docker.sh OUT_FILE [rounds]      env: DBHOST (default pg-durable), NET (hydra-net), CPUS (4), ROWS (200000)
set -u
cd "$(dirname "$0")/.."
OUT=$1; ROUNDS=${2:-3}
DBHOST=${DBHOST:-pg-durable}; NET=${NET:-hydra-net}; CPUS=${CPUS:-4}; ROWS=${ROWS:-200000}
ROOT=$(pwd); M2=${M:-$HOME/.m2/repository}
CP="/work/benchmark/out-pg:/work/hydrangea-core/target/classes:/work/hydrangea-postgres/target/classes:/m2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar:/m2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar"
WAYS=(hydrangea-batch hydrangea-unnest hydrangea-copy hydrangea-pipelined pgjdbc-batch pgjdbc-rewrite pgjdbc-unnest pgjdbc-copy)
: > "$OUT"
for r in $(seq 0 $((ROUNDS - 1))); do
  for k in $(seq 0 $(( ${#WAYS[@]} - 1 ))); do
    way=${WAYS[$(( (k + r * 3) % ${#WAYS[@]} ))]}
    docker run --rm --network "$NET" --cpus="$CPUS" --memory=2g -v "$ROOT:/work:ro" -v "$M2:/m2:ro" eclipse-temurin:21-jre java -Xmx1g -cp "$CP" postgres.PgBatchBench "$way" rows=$ROWS runs=6 host="$DBHOST" port=5432 2>&1 | grep -E '^RESULT' >> "$OUT" || echo "FAILED $way" >> "$OUT"
  done
done
echo done >> "$OUT"
