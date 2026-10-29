#!/usr/bin/env bash
# Hydrangea MySQL client vs MySQL Connector/J with the client in a Linux container, over a Docker network, against a MySQL container (so Linux on both sides and a bridge
# network between them, as in pg-bench-docker.sh).
# usage: benchmark/my-bench-docker.sh OUT_DIR [rounds]
# env:   DBHOST (default hydrangea-my-test: mysql:8.4 on the network NET with its defaults, so innodb_flush_log_at_trx_commit=1), NET (default hydra-net), CPUS (default 4, for the client),
#        SCENARIOS (default "select1 point insert tx"), CONFIGS (default "8:8 8:64 32:32"), LIBS
# Needs: the project built, benchmark/src/mysql/MyBench.java compiled into benchmark/out-my (javac --release 21), the jars below in ~/.m2, the image eclipse-temurin:21-jre, the DB container
#        connected to NET (docker network connect hydra-net hydrangea-my-test) and `... mysql.MyBench setup` run once for the tables.
set -u
cd "$(dirname "$0")/.."
OUT=$1; ROUNDS=${2:-3}
DBHOST=${DBHOST:-hydrangea-my-test}; NET=${NET:-hydra-net}; CPUS=${CPUS:-4}
ROOT=$(pwd); M2=${M:-$HOME/.m2/repository}
CP="/work/benchmark/out-my:/work/hydrangea-core/target/classes:/work/hydrangea-mysql/target/classes:/m2/com/mysql/mysql-connector-j/9.4.0/mysql-connector-j-9.4.0.jar:/m2/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar:/m2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar"
mkdir -p "$OUT"
run() { docker run --rm --network "$NET" --cpus="$CPUS" --memory=2g -v "$ROOT:/work:ro" -v "$M2:/m2:ro" eclipse-temurin:21-jre java -Xmx1g -cp "$CP" mysql.MyBench "$@" host="$DBHOST" port=3306 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
read -r -a LIBS <<< "${LIBS:-hydrangea hydrangea-block mysqlj mysqlj-server mysqlj-virtual mysqlj-dedicated}"
read -r -a CONFIGS <<< "${CONFIGS:-8:8 8:64 32:32}"
read -r -a SCENARIOS <<< "${SCENARIOS:-select1 point insert tx}"
for r in $(seq 1 "$ROUNDS"); do
  f="$OUT/round$r.txt"; : > "$f"
  for cfg in "${CONFIGS[@]}"; do
    conns=${cfg%%:*}; callers=${cfg##*:}
    for sc in "${SCENARIOS[@]}"; do
      k=$(( (r - 1 + ${#sc}) % ${#LIBS[@]} ))          # rotate the order so that no library always runs first / last
      order=("${LIBS[@]:$k}" "${LIBS[@]:0:$k}")
      for lib in "${order[@]}"; do
        if [ "$lib" = mysqlj-dedicated ] && [ "$conns" != "$callers" ]; then continue; fi
        echo "== round $r $cfg $sc $lib" >&2
        run "$lib" "$sc" conns=$conns callers=$callers seconds=5 warmup=2 >> "$f"
      done
    done
  done
done
echo done >> "$OUT/round$ROUNDS.txt"
