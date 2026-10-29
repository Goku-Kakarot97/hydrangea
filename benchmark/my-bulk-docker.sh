#!/usr/bin/env bash
# Bulk loads and reads over one connection, Hydrangea MySQL client against Connector/J, the client in a Linux container against the MySQL container (see my-bench-docker.sh).
# usage: benchmark/my-bulk-docker.sh OUT_FILE [rows] [chunk] [runs]
set -u
cd "$(dirname "$0")/.."
OUT=$1; ROWS=${2:-200000}; CHUNK=${3:-1000}; RUNS=${4:-4}
DBHOST=${DBHOST:-hydrangea-my-test}; NET=${NET:-hydra-net}; CPUS=${CPUS:-4}
ROOT=$(pwd); M2=${M:-$HOME/.m2/repository}
CP="/work/benchmark/out-my:/work/hydrangea-core/target/classes:/work/hydrangea-sql/target/classes:/work/hydrangea-mysql/target/classes:/m2/com/mysql/mysql-connector-j/9.4.0/mysql-connector-j-9.4.0.jar:/m2/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar:/m2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar"
run() { docker run --rm --network "$NET" --cpus="$CPUS" --memory=3g -v "$ROOT:/work:ro" -v "$M2:/m2:ro" eclipse-temurin:21-jre java -Xmx2g -cp "$CP" mysql.MyBulkBench "$@" host="$DBHOST" port=3306 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
: > "$OUT"
for way in hydrangea-batch mysqlj-batch hydrangea-pipelined mysqlj-rewrite hydrangea-multirow mysqlj-multirow; do
  echo "== $way" >&2
  run $way rows=$ROWS chunk=$CHUNK runs=$RUNS >> "$OUT"
done
for way in hydrangea-stream mysqlj-cursor hydrangea-whole mysqlj-whole hydrangea-stream mysqlj-cursor; do
  echo "== $way" >&2
  run $way rows=$ROWS chunk=$CHUNK runs=$RUNS >> "$OUT"
done
echo done >> "$OUT"
