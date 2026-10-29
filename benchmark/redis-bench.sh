#!/usr/bin/env bash
# Hydrangea Redis client vs Jedis against the same Redis container (hydrangea-redis-test, port 26379).
# usage: benchmark/redis-bench.sh JEDIS_DIR OUT_DIR [rounds]   (JEDIS_DIR holds jedis, commons-pool2, slf4j-api, gson, json jars)
# Needs: javac --release 21 -d CLASSES -sourcepath hydrangea-core/src/main/java $(find hydrangea-redis/src/main/java -name '*.java'), and benchmark/out-redis compiled.
set -u
cd "$(dirname "$0")/.."
J=$1; OUT=$2; ROUNDS=${3:-3}; PORT=${PORT:-26379}
CLASSES=${CLASSES:?set CLASSES to the compiled hydrangea-redis + core classes}
CP="benchmark/out-redis:$CLASSES:$(ls $J/*.jar | tr '\n' ':')"
mkdir -p "$OUT"
run() { java -Xmx1g -cp "$CP" redis.RedisBench "$@" port=$PORT 2>&1 | grep -E '^RESULT' || echo "FAILED $*"; }
LIBS=(hydrangea hydrangea-conns jedis)
for r in $(seq 1 $ROUNDS); do
  f="$OUT/round$r.txt"; : > "$f"
  # rotate the order so that no library always runs first / last
  order=("${LIBS[@]:$((r-1)):3}" "${LIBS[@]:0:$((r-1))}")
  for lib in "${order[@]}"; do
    echo "== $lib round $r"
    run hydrangea setup >/dev/null
    { run $lib connect connections=500
      run $lib get threads=1 seconds=6 warmup=2
      run $lib get threads=32 seconds=6 warmup=2
      run $lib pipeline window=100 seconds=6 warmup=2
      run $lib pipeline window=100 op=set seconds=6 warmup=2
      run $lib pipeline window=1000 seconds=6 warmup=2
      for s in 1024 102400 1048576 8388608; do run $lib large size=$s rounds=$((  s > 1000000 ? 100 : 1000 )); done
      for w in mget hgetall lrange; do run $lib bulk-reply what=$w threads=1 seconds=5 warmup=2; done
      run $lib bulk-reply what=lrange threads=8 seconds=5 warmup=2
      run $lib pubsub seconds=6 warmup=2
      run $lib tx threads=8 seconds=6 warmup=2
    } >> "$f"
  done
done
