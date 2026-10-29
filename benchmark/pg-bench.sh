#!/usr/bin/env bash
# Hydrangea PostgreSQL client vs pgjdbc (+ HikariCP) against the same PostgreSQL container (hydrangea-pg-test, port 25432).
# usage: benchmark/pg-bench.sh OUT_DIR [rounds]
# Needs: the project built (hydrangea-core/target/classes, hydrangea-postgres/target/classes), benchmark/src/postgres/PgBench.java compiled into
# benchmark/out-pg with pgjdbc, HikariCP, slf4j-api and checker-qual on the class path (jars from ~/.m2, see M below), and
# `java -cp ... postgres.PgBench setup` run once.
set -u
cd "$(dirname "$0")/.."
OUT=$1; ROUNDS=${2:-3}; PORT=${PORT:-25432}; HOST=${HOST:-127.0.0.1}
# other database: HOST, PORT, SCENARIOS (default all; "select1 calc series" are read-only and need no tables), EXTRA="user=... password=... db=..."
EXTRA=${EXTRA:-}
M=${M:-$HOME/.m2/repository}
CP="benchmark/out-pg:hydrangea-core/target/classes:hydrangea-postgres/target/classes:$M/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar:$M/com/zaxxer/HikariCP/7.0.2/HikariCP-7.0.2.jar:$M/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar:$M/org/checkerframework/checker-qual/3.49.5/checker-qual-3.49.5.jar"
mkdir -p "$OUT"
run() { java -Xmx1g -cp "$CP" postgres.PgBench "$@" host=$HOST port=$PORT $EXTRA 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
LIBS=(hydrangea hydrangea-block pgjdbc pgjdbc-virtual pgjdbc-dedicated)
# conns:callers  A = no queueing, B = more callers than connections, C = more connections
CONFIGS=("8:8" "8:64" "32:32")
read -r -a SCENARIOS <<< "${SCENARIOS:-select1 point rows insert tx}"
for r in $(seq 1 "$ROUNDS"); do
  f="$OUT/round$r.txt"; : > "$f"
  for cfg in "${CONFIGS[@]}"; do
    conns=${cfg%%:*}; callers=${cfg##*:}
    for sc in "${SCENARIOS[@]}"; do
      # rotate the order so that no library always runs first / last
      k=$(( (r - 1 + ${#sc}) % ${#LIBS[@]} ))
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
