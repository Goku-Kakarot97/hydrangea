#!/usr/bin/env bash
# Hydrangea PostgreSQL client vs pgjdbc with the client in a Linux container, over a Docker network, against a PostgreSQL container: Linux on both sides, a real (bridge) network between
# them instead of macOS loopback, and fsync as the container has it. Same scenarios and layout as pg-bench.sh.
# usage: benchmark/pg-bench-docker.sh OUT_DIR [rounds]
# env:   DBHOST (default pg-durable: a postgres:16 container on the network NET with its defaults, so fsync on), NET (default hydra-net), CPUS (default 4, for the client container),
#        SCENARIOS (default "select1 point rows insert tx"), CONFIGS (default "8:8 8:64 32:32")
# Needs: the project built, benchmark/src/postgres/PgBench.java compiled into benchmark/out-pg (javac --release 21), the jars of pg-bench.sh in ~/.m2, the image eclipse-temurin:21-jre, and
#        `docker run --rm ... postgres.PgBench setup host=$DBHOST` run once for the tables.
set -u
cd "$(dirname "$0")/.."
OUT=$1; ROUNDS=${2:-3}
DBHOST=${DBHOST:-pg-durable}; NET=${NET:-hydra-net}; CPUS=${CPUS:-4}
ROOT=$(pwd); M2=${M:-$HOME/.m2/repository}
CP="/work/benchmark/out-pg:/work/hydrangea-core/target/classes:/work/hydrangea-postgres/target/classes:/m2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar:/m2/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar:/m2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar:/m2/org/checkerframework/checker-qual/3.49.5/checker-qual-3.49.5.jar"
mkdir -p "$OUT"
run() { docker run --rm --network "$NET" --cpus="$CPUS" --memory=2g -v "$ROOT:/work:ro" -v "$M2:/m2:ro" eclipse-temurin:21-jre java -Xmx1g -cp "$CP" postgres.PgBench "$@" host="$DBHOST" port=5432 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
LIBS=(hydrangea hydrangea-block pgjdbc pgjdbc-virtual pgjdbc-dedicated)
read -r -a CONFIGS <<< "${CONFIGS:-8:8 8:64 32:32}"
read -r -a SCENARIOS <<< "${SCENARIOS:-select1 point rows insert tx}"
for r in $(seq 1 "$ROUNDS"); do
  f="$OUT/round$r.txt"; : > "$f"
  for cfg in "${CONFIGS[@]}"; do
    conns=${cfg%%:*}; callers=${cfg##*:}
    for sc in "${SCENARIOS[@]}"; do
      k=$(( (r - 1 + ${#sc}) % ${#LIBS[@]} ))          # rotate the order so that no library always runs first / last
      order=("${LIBS[@]:$k}" "${LIBS[@]:0:$k}")
      for lib in "${order[@]}"; do
        if [ "$lib" = pgjdbc-dedicated ] && [ "$conns" != "$callers" ]; then continue; fi
        echo "== round $r $cfg $sc $lib" >&2
        run "$lib" "$sc" conns=$conns callers=$callers seconds=5 warmup=2 >> "$f"
      done
    done
  done
done
echo done >> "$OUT/round$ROUNDS.txt"
